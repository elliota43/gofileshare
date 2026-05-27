package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/elliota43/wormbeam/internal/protocol"
)

// Send transmits the file or directory at src to peer.
//
// It walks src to build a Manifest (hashing every chunk on disk), sends the
// manifest frame, waits for MANIFEST_ACK, then streams CHUNK frames in
// manifest order, then a DONE frame.
//
// The peer is assumed to be a bidirectional frame stream. r is a buffered
// reader over the same conn so bytes aren't dropped between handshake and frames.
func Send(src string, w io.Writer, r io.Reader, progress func(sent, total uint64)) error {
	manifest, err := BuildManifest(src)
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}
	if len(manifest.Files) == 0 {
		return fmt.Errorf("nothing to send under %s", src)
	}

	mjson, err := manifest.Marshal()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := protocol.WriteFrame(w, protocol.Frame{Type: protocol.FrameManifest, Payload: mjson}); err != nil {
		return fmt.Errorf("send manifest: %w", err)
	}

	// wait for ack (or error)
	ack, err := protocol.ReadFrame(r)
	if err != nil {
		return fmt.Errorf("read manifest ack: %w", err)
	}

	switch ack.Type {
	case protocol.FrameManifestAck:
		// good
	case protocol.FrameErr:
		return fmt.Errorf("peer rejected manifest: %s", string(ack.Payload))
	default:
		return fmt.Errorf("unexpected frame waiting for ack: %s", ack.Type)
	}

	total := manifest.TotalSize()
	var sent uint64
	srcRoot := filepath.Clean(src)
	srcIsDir, err := isDir(srcRoot)
	if err != nil {
		return err
	}

	// Allocate one buffer reused across files: header + max chunk.
	buf := make([]byte, protocol.ChunkHeaderSize+ChunkSize)

	for fileIdx, entry := range manifest.Files {
		// Resolve the on-disk path for this entry. For a single-file transfer
		// the manifest path is just the basename; for a directory the manifest
		// path is relative to the root
		var diskPath string
		if srcIsDir {
			diskPath = filepath.Join(srcRoot, filepath.FromSlash(entry.Path))
		} else {
			diskPath = srcRoot
		}

		if err := sendOneFile(w, uint32(fileIdx), entry, diskPath, buf, &sent, total, progress); err != nil {
			return err
		}
	}

	if err := protocol.WriteFrame(w, protocol.Frame{Type: protocol.FrameDone}); err != nil {
		return fmt.Errorf("send done: %w", err)
	}
	return nil
}

// sendOneFile streams every chunk of one file as CHUNK frames.

func sendOneFile(w io.Writer, fileIdx uint32, entry FileEntry, diskPath string, buf []byte, sent *uint64, total uint64, progress func(sent, total uint64)) error {
	f, err := os.Open(diskPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", diskPath, err)
	}
	defer f.Close()

	nChunks := entry.ChunkCount()
	for chunkIdx := 0; chunkIdx < nChunks; chunkIdx++ {
		expected := entry.ChunkSizeAt(chunkIdx)
		// Layout: [8-byte header][chunk bytes]. Reuse the single buffer.
		protocol.EncodeChunkHeader(buf[:protocol.ChunkHeaderSize], protocol.ChunkHeader{
			FileIdx:  fileIdx,
			ChunkIdx: uint32(chunkIdx),
		})
		if expected > 0 {
			if _, err := io.ReadFull(f, buf[protocol.ChunkHeaderSize:protocol.ChunkHeaderSize+expected]); err != nil {
				return fmt.Errorf("read chunk %d of %s: %w", chunkIdx, entry.Path, err)
			}
		}
		frame := protocol.Frame{
			Type:    protocol.FrameChunk,
			Payload: buf[:protocol.ChunkHeaderSize+expected],
		}
		if err := protocol.WriteFrame(w, frame); err != nil {
			return fmt.Errorf("send chunk %d of %s: %w", chunkIdx, entry.Path, err)
		}
		*sent += uint64(expected)
		if progress != nil {
			progress(*sent, total)
		}
	}
	return nil
}

// Receive runs the joiner side. It reads frames until it sees DONE or ERR,
// writing files into outDir as chunks arrive.
//
// outDir must not already exist -- Receive creates it.
func Receive(outDir string, r io.Reader, w io.Writer, progress func(recv, total uint64)) error {
	// First frame must be the manifest (or an err).
	first, err := protocol.ReadFrame(r)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	switch first.Type {
	case protocol.FrameManifest:
		// fall through
	case protocol.FrameErr:
		return fmt.Errorf("peer sent error: %s", string(first.Payload))
	default:
		return fmt.Errorf("expected manifest frame, got %s", first.Type)
	}

	manifest, err := UnmarshalManifest(first.Payload)
	if err != nil {
		writeErr(w, "bad manifest: "+err.Error())
		return fmt.Errorf("decode manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		writeErr(w, err.Error())
		return err
	}

	// Create outDir and pre-allocate destination files. mkdirAll on outDir
	// itself is fine (caller picked a fresh name); subdirs come from the
	// manifest and must not escape outDir.
	if err := os.Mkdir(outDir, 0o755); err != nil {
		writeErr(w, "create outdir: "+err.Error())
		return fmt.Errorf("create outdir: %w", err)
	}

	openFiles := make([]*os.File, len(manifest.Files))
	defer func() {
		for _, f := range openFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	for i, e := range manifest.Files {
		dest := filepath.Join(outDir, filepath.FromSlash(e.Path))
		// Defense in depth: even with validateManifest, refuse anything
		// that would land outside outDir after symlink resolution.
		// (We resolve outDir to absolute and check prefix.)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			writeErr(w, "mkdir: "+err.Error())
			return err
		}
		f, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			writeErr(w, "open output: "+err.Error())
			return fmt.Errorf("open %s: %w", dest, err)
		}
		if e.Size > 0 {
			// Pre-allocate so out-of-order chunks could be written in place.
			// Not strictly needed today since we expect in-order chunks, but
			// it's free and makes the file's final size visible immediately.
			if err := f.Truncate(int64(e.Size)); err != nil {
				writeErr(w, "truncate: "+err.Error())
				return err
			}
		}
		openFiles[i] = f
	}

	if err := protocol.WriteFrame(w, protocol.Frame{Type: protocol.FrameManifestAck}); err != nil {
		return fmt.Errorf("send manifest ack: %w", err)
	}

	total := manifest.TotalSize()
	var recv uint64

	for {
		fr, err := protocol.ReadFrame(r)
		if err != nil {
			return fmt.Errorf("read frame: %w", err)
		}
		switch fr.Type {
		case protocol.FrameChunk:
			hdr, body, err := protocol.DecodeChunkHeader(fr.Payload)
			if err != nil {
				writeErr(w, err.Error())
				return err
			}
			if int(hdr.FileIdx) >= len(manifest.Files) {
				return fmt.Errorf("chunk for unknown file idx %d", hdr.FileIdx)
			}
			entry := manifest.Files[hdr.FileIdx]
			if int(hdr.ChunkIdx) >= entry.ChunkCount() {
				return fmt.Errorf("chunk idx %d out of range for %s", hdr.ChunkIdx, entry.Path)
			}
			expected := entry.ChunkSizeAt(int(hdr.ChunkIdx))
			if len(body) != expected {
				return fmt.Errorf("chunk %d/%d wrong size: got %d, want %d",
					hdr.FileIdx, hdr.ChunkIdx, len(body), expected)
			}
			// Verify hash before writing.
			got := sha256.Sum256(body)
			want, err := hex.DecodeString(entry.Hashes[hdr.ChunkIdx])
			if err != nil || !bytesEqual(got[:], want) {
				return fmt.Errorf("hash mismatch on %s chunk %d", entry.Path, hdr.ChunkIdx)
			}
			// Write at the right offset.
			off := int64(hdr.ChunkIdx) * int64(ChunkSize)
			if _, err := openFiles[hdr.FileIdx].WriteAt(body, off); err != nil {
				return fmt.Errorf("write %s: %w", entry.Path, err)
			}
			recv += uint64(len(body))
			if progress != nil {
				progress(recv, total)
			}

		case protocol.FrameDone:
			return nil

		case protocol.FrameErr:
			return fmt.Errorf("peer sent error: %s", string(fr.Payload))

		default:
			return fmt.Errorf("unexpected frame: %s", fr.Type)
		}
	}
}

func validateManifest(m Manifest) error {
	for _, e := range m.Files {
		if e.Path == "" {
			return fmt.Errorf("manifest contains empty path")
		}
		if strings.ContainsRune(e.Path, '\\') {
			return fmt.Errorf("manifest path %q contains backslash", e.Path)
		}
		// Reject absolute and traversal. filepath.Clean normalizes
		// .././a to a, but we want to refuse the manifest, not silently fix it.
		clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(e.Path)))
		if clean != e.Path {
			return fmt.Errorf("manifest path %q is not in canonical form (want %q)", e.Path, clean)
		}
		if filepath.IsAbs(e.Path) || strings.HasPrefix(e.Path, "/") {
			return fmt.Errorf("manifest path %q is absolute", e.Path)
		}
		if strings.HasPrefix(e.Path, "../") || e.Path == ".." || strings.Contains(e.Path, "/../") {
			return fmt.Errorf("manifest path %q escapes root", e.Path)
		}
		if uint64(len(e.Hashes)) != uint64(e.ChunkCount()) {
			return fmt.Errorf("manifest %q: hash count %d != chunk count %d",
				e.Path, len(e.Hashes), e.ChunkCount())
		}
	}
	return nil
}

func writeErr(w io.Writer, msg string) {
	_ = protocol.WriteFrame(w, protocol.Frame{Type: protocol.FrameErr, Payload: []byte(msg)})
}

func isDir(p string) (bool, error) {
	st, err := os.Stat(p)
	if err != nil {
		return false, err
	}
	return st.IsDir(), nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
