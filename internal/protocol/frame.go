package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Frame is one length-prefixed binary message on the wire:
//
// [1 byte FrameType][4 bytes BE length][payload...]
//
// Frames are the unit of communication between paired clients after
// the relay handshake completes.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// FrameType identifies what's in the payload.
type FrameType uint8

const (
	FrameManifest    FrameType = 1 // host -> joiner: JSON file list
	FrameManifestAck FrameType = 2 // joiner -> host: ready to receive
	FrameChunk       FrameType = 3 // host -> joiner: [file_idx u32][chunk_idx u32][bytes...]
	FrameDone        FrameType = 4 // host -> joiner: transfer complete
	FrameErr         FrameType = 5 // either side: utf-8 issue
)

func (t FrameType) String() string {
	switch t {
	case FrameManifest:
		return "MANIFEST"
	case FrameManifestAck:
		return "MANIFEST_ACK"
	case FrameChunk:
		return "CHUNK"
	case FrameDone:
		return "DONE"
	case FrameErr:
		return "ERR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}

// MaxFramePayload caps a single frame to prevent runaway allocations from
// a malicious / buggy peer. Default chunk size is 256KiB, but max is set at 16MiB
const MaxFramePayload = 16 << 20

// ReadFrame reads one frame from r. The returned Payload is freshly
// allocated and owned by the caller.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	t := FrameType(hdr[0])
	n := binary.BigEndian.Uint32(hdr[1:5])
	if n > MaxFramePayload {
		return Frame{}, fmt.Errorf("frame payload too large: %d bytes (max %d)", n, MaxFramePayload)
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, fmt.Errorf("read from payload: %w", err)
	}
	return Frame{Type: t, Payload: payload}, nil
}

// WriteFrame writes one frame to w. It writes the header and payload in two
// calls; the underlying conn will coalesce them into one TCP segment
func WriteFrame(w io.Writer, f Frame) error {
	if len(f.Payload) > MaxFramePayload {
		return fmt.Errorf("frame payload too large: %d bytes (max %d)", len(f.Payload), MaxFramePayload)
	}

	var hdr [5]byte
	hdr[0] = byte(f.Type)
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}

// ChunkHeader is the fixed prefix of a FrameChunk payload, before
// the chunk bytes themselves.
type ChunkHeader struct {
	FileIdx  uint32
	ChunkIdx uint32
}

const ChunkHeaderSize = 8

// EncodeChunkHeader writes a ChunkHeader into the first 8 bytes of buf.
// buf must be at least ChunkHeaderSize long.
func EncodeChunkHeader(buf []byte, h ChunkHeader) {
	binary.BigEndian.PutUint32(buf[0:4], h.FileIdx)
	binary.BigEndian.PutUint32(buf[4:8], h.ChunkIdx)
}

// DecodeChunkHeader parses a ChunkHeader from the first 8 bytes of buf and
// returns the remaining slice (the chunk bytes).
func DecodeChunkHeader(buf []byte) (ChunkHeader, []byte, error) {
	if len(buf) < ChunkHeaderSize {
		return ChunkHeader{}, nil, fmt.Errorf("chunk frame too short: %d bytes", len(buf))
	}
	return ChunkHeader{
		FileIdx:  binary.BigEndian.Uint32(buf[0:4]),
		ChunkIdx: binary.BigEndian.Uint32(buf[4:8]),
	}, buf[ChunkHeaderSize:], nil
}
