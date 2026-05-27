package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkSize is the per-chunk size in bytes. 256 KiB
const ChunkSize = 256 * 1024

type FileEntry struct {
	Path string `json:"path"`
	Size uint64 `json:size"`

	// Hashes holds the SHA256 of each chunk, in order. Last chunk
	// may be shorter than ChunkSize. len(hashes) == ceiling(Size / ChunkSize)
	// Each Hash is hex-encoded
	Hashes []string `json:"hashes"`
}

func (f FileEntry) ChunkCount() int {
	if f.Size == 0 {
		return 1
	}

	return int((f.Size + ChunkSize - 1) / ChunkSize)
}

// ChunkSizeAt returns the byte length of chunk i. (The last chunk might be short)
func (f FileEntry) ChunkSizeAt(i int) int {
	if f.Size == 0 {
		return 0
	}

	last := f.ChunkCount() - 1
	if i < last {
		return ChunkSize
	}

	rem := int(f.Size % ChunkSize)
	if rem == 0 {
		return ChunkSize
	}
	return rem
}

// Manifest is the full list of files in a transfer.
type Manifest struct {
	// RootName is the display name of the transfer - the basename of the path the host sent.
	// Joiner uses this to create a top-level dir under transfer_out/
	RootName string      `json:"root_name"`
	Files    []FileEntry `json:"files"`
}

// TotalSize returns the sum of all file sizes in the manifest.
func (m Manifest) TotalSize() uint64 {
	var n uint64
	for _, f := range m.Files {
		n += f.Size
	}
	return n
}

// Marshal serializes the manifest to JSON.
func (m Manifest) Marshal() ([]byte, error) { return json.Marshal(m) }

func UnmarshalManifest(b []byte) (Manifest, error) {
	var m Manifest
	err := json.Unmarshal(b, &m)
	return m, err
}

// BuildManifest walks src (file or directory) and returns a Manifest.
// Hashes are computed by reading every file once.
//
// Symlinks are followed only at the root. Symlinks discovered during the walk
// are ignored/skipped to avoid loops.
func BuildManifest(src string) (Manifest, error) {
	src = filepath.Clean(src)
	info, err := os.Stat(src)
	if err != nil {
		return Manifest{}, fmt.Errorf("stat %s: %w", src, err)
	}

	root := filepath.Base(src)
	m := Manifest{RootName: root}

	if !info.IsDir() {
		entry, err := buildFileEntry(src, info.Name())
		if err != nil {
			return Manifest{}, err
		}
		m.Files = []FileEntry{entry}
		return m, nil
	}

	err = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == src {
			return nil
		}

		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			// skip sockets/devices/pipes, etc
			return nil
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		// normalize to forward slashes
		relSlash := filepath.ToSlash(rel)

		entry, err := buildFileEntry(path, relSlash)
		if err != nil {
			return err
		}

		m.Files = append(m.Files, entry)
		return nil
	})

	if err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// buildFileEntry opens the file once and streams it through a SHA-256 hasher
// per chunk, recording one hex digest per chunk.
func buildFileEntry(absPath, manifestPath string) (FileEntry, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return FileEntry{}, fmt.Errorf("open %s: %w", absPath, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return FileEntry{}, err
	}
	size := uint64(st.Size())

	entry := FileEntry{Path: manifestPath, Size: size}

	if size == 0 {
		// Hash of the empty chunk, so receiver verification is uniform.
		h := sha256.Sum256(nil)
		entry.Hashes = []string{hex.EncodeToString(h[:])}
		return entry, nil
	}

	buf := make([]byte, ChunkSize)
	for {
		n, err := io.ReadFull(f, buf)
		// io.ReadFull returns ErrUnexpectedEOF for a short final read,
		// which is the normal case for the last chunk.
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return FileEntry{}, fmt.Errorf("read %s: %w", absPath, err)
		}
		h := sha256.Sum256(buf[:n])
		entry.Hashes = append(entry.Hashes, hex.EncodeToString(h[:]))
		if err == io.ErrUnexpectedEOF {
			break
		}
	}
	return entry, nil
}
