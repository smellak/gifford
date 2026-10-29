package capture

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// frameMagic heads every frame: magic, varint chunk count, then per
// chunk a varint sequence, a varint length and the bytes. No checksums.
var frameMagic = []byte{'G', 'F', 'R', '1'}

// zstdWindowSize bounds the compressor window; zstdMaxMemory bounds the
// decoder. Frames compress at level 3.
const (
	zstdWindowSize = 1 << 22
	zstdMaxMemory  = 64 << 20
	zstdLevel      = 3
)

// sha256Hex returns the lowercase hex SHA-256 of data: segment digests
// and, later, snapshot hashes.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// frameChunk is one chunk inside a frame.
type frameChunk struct {
	seq  int64
	data []byte
}

// encodeFrame renders chunks as one independent frame. Sequences
// must be non-negative.
func encodeFrame(chunks []frameChunk) ([]byte, error) {
	var out []byte
	out = append(out, frameMagic...)
	out = binary.AppendUvarint(out, uint64(len(chunks)))
	for _, c := range chunks {
		if c.seq < 0 {
			return nil, fmt.Errorf("%w: negative chunk sequence", ErrUnsafePath)
		}
		out = binary.AppendUvarint(out, uint64(c.seq))
		out = binary.AppendUvarint(out, uint64(len(c.data)))
		out = append(out, c.data...)
	}
	return out, nil
}

// decodeFrame parses one frame strictly: exact magic, exact count,
// non-negative values, enough bytes and nothing after.
func decodeFrame(frame []byte) ([]frameChunk, error) {
	fail := func() ([]frameChunk, error) {
		return nil, fmt.Errorf("%w: bad frame", ErrUnsafePath)
	}
	rest := frame
	if len(rest) < len(frameMagic) || !bytes.Equal(rest[:len(frameMagic)], frameMagic) {
		return fail()
	}
	rest = rest[len(frameMagic):]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return fail()
	}
	rest = rest[n:]
	var chunks []frameChunk
	for range count {
		seq, n := binary.Uvarint(rest)
		if n <= 0 || seq > 9223372036854775807 {
			return fail()
		}
		rest = rest[n:]
		length, n := binary.Uvarint(rest)
		if n <= 0 || length > uint64(len(rest)) {
			return fail()
		}
		rest = rest[n:]
		chunks = append(chunks, frameChunk{seq: int64(seq), data: rest[:length]})
		rest = rest[length:]
	}
	if len(rest) != 0 {
		return fail()
	}
	return chunks, nil
}

// groupFrameRuns packs chunk lengths into runs of whole chunks, each at
// most max raw bytes. A chunk never spans two frames; every length must
// fit max alone, which the limits guarantee for chunks.
func groupFrameRuns(lengths []int64, maxBytes int64) ([][]int, error) {
	var runs [][]int
	var cur []int
	var size int64
	flush := func() {
		if len(cur) > 0 {
			runs = append(runs, cur)
			cur = nil
			size = 0
		}
	}
	for i, l := range lengths {
		if l < 0 || l > maxBytes {
			return nil, fmt.Errorf("%w: chunk %d bytes", ErrUnsafePath, l)
		}
		if size+l > maxBytes {
			flush()
		}
		cur = append(cur, i)
		size += l
	}
	flush()
	return runs, nil
}

// compressFrame compresses one frame's raw bytes.
func compressFrame(raw []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)),
		zstd.WithWindowSize(zstdWindowSize))
	if err != nil {
		return nil, fmt.Errorf("zstd encoder: %w", err)
	}
	out := enc.EncodeAll(raw, nil)
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("zstd encoder: %w", err)
	}
	return out, nil
}

// decompressFrame decodes one frame with the memory cap.
func decompressFrame(comp []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(zstdMaxMemory))
	if err != nil {
		return nil, fmt.Errorf("zstd decoder: %w", err)
	}
	defer dec.Close()
	out, err := dec.DecodeAll(comp, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	return out, nil
}

// headroom returns the compression reserve for R raw bytes.
func headroom(raw int64) int64 {
	return raw + raw/64 + 1048576
}

// frameEntry maps one frame in the .zst to its raw bytes.
type frameEntry struct {
	RawOffset int64 `json:"raw_offset"`
	RawLength int64 `json:"raw_length"`
	Offset    int64 `json:"offset"`
	Length    int64 `json:"length"`
}

// framesDoc is a .frames table: schema gifford.capture.frames.v1.
type framesDoc struct {
	Schema    string       `json:"schema"`
	SegmentID string       `json:"segment_id"`
	RawBytes  int64        `json:"raw_bytes"`
	RawSHA256 string       `json:"raw_sha256"`
	Frames    []frameEntry `json:"frames"`
}

func (d *framesDoc) controlSchema() string { return "gifford.capture.frames.v1" }

func (d *framesDoc) controlFields() []string {
	return []string{"schema", "segment_id", "raw_bytes", "raw_sha256", "frames"}
}

func (d *framesDoc) controlNested() map[string][]string { return nil }

func (d *framesDoc) validate() error {
	if err := validateSegmentID(d.SegmentID); err != nil {
		return err
	}
	if d.RawBytes < 0 {
		return fmt.Errorf("%w: negative raw bytes", ErrUnsafePath)
	}
	if len(d.RawSHA256) != 64 {
		return fmt.Errorf("%w: bad raw sha", ErrUnsafePath)
	}
	var raw, at int64
	for _, f := range d.Frames {
		if f.RawOffset != at || f.RawLength < 0 || f.Offset < 0 || f.Length < 0 {
			return fmt.Errorf("%w: non contiguous frames", ErrUnsafePath)
		}
		at += f.RawLength
		raw += f.RawLength
	}
	if raw != d.RawBytes {
		return fmt.Errorf("%w: frames do not cover the raw bytes", ErrUnsafePath)
	}
	return nil
}

// verifySegment checks a closed segment: the frames decode, cover
// [0, R) contiguously, and hash to the raw digest.
func verifySegment(raw, comp []byte, doc *framesDoc) error {
	if int64(len(raw)) != doc.RawBytes || sha256Hex(raw) != doc.RawSHA256 {
		return fmt.Errorf("%w: raw bytes do not match the frames table", ErrUnsafePath)
	}
	var at int64
	assembled := make([]byte, 0, len(raw))
	for _, f := range doc.Frames {
		if f.Offset != at {
			return fmt.Errorf("%w: non contiguous compressed frames", ErrUnsafePath)
		}
		if f.Length < 0 || at+f.Length > int64(len(comp)) {
			return fmt.Errorf("%w: frame outside the container", ErrUnsafePath)
		}
		part, err := decompressFrame(comp[f.Offset : f.Offset+f.Length])
		if err != nil {
			return err
		}
		assembled = append(assembled, part...)
		at += f.Length
	}
	if at != int64(len(comp)) {
		return fmt.Errorf("%w: trailing compressed bytes", ErrUnsafePath)
	}
	if !bytes.Equal(assembled, raw) {
		return fmt.Errorf("%w: decoded bytes do not match", ErrUnsafePath)
	}
	return nil
}
