package capture

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// verifyZstdStream decodes one .zst stream and checks it against the
// raw length and SHA: existence alone never verifies.
func verifyZstdStream(r io.Reader, total int64, rawSHA string) bool {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return false
	}
	defer dec.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(dec, total+1))
	if err != nil || n != total {
		return false
	}
	return hex.EncodeToString(h.Sum(nil)) == rawSHA
}

// frameMagic heads every frame: magic, varint chunk count, then per
// chunk a varint sequence, a varint length and the bytes. No checksums.
var frameMagic = []byte{'G', 'F', 'R', '1'}

// codecWindow returns W for a frame_max_bytes: the smallest power of
// two at or above max(frame_max_bytes, 1024), capped at 8 MiB. The
// window and workspace allowance is separate from the application
// output cap and is not a process-memory guarantee.
func codecWindow(frameMaxBytes int64) int {
	want := frameMaxBytes
	if want < 1024 {
		want = 1024
	}
	w := 1
	for int64(w) < want {
		w <<= 1
	}
	if w > 8<<20 {
		w = 8 << 20
	}
	return w
}

// frameCodec binds the approved zstd settings to one session's
// frame_max_bytes: one encoder per ingest used sequentially, and one
// decoder at a time. A codec is never shared across different limits,
// and never across goroutines. The unsigned decoder settings are
// converted once here, behind the positivity check, so no call site
// converts a signed value.
type frameCodec struct {
	maxBytes int64
	enc      *zstd.Encoder
	dec      *zstd.Decoder
	buf      []byte
}

// newFrameCodec checks the frame budget, then builds the session's
// encoder and decoder with the approved options.
func newFrameCodec(frameMaxBytes int64) (*frameCodec, error) {
	if frameMaxBytes <= 0 {
		return nil, fmt.Errorf("%w: frame_max_bytes", ErrUsage)
	}
	window := codecWindow(frameMaxBytes)
	windowU := uint64(window) //nolint:gosec // G115: codecWindow returns at least 1024.
	memBytes := uint64(frameMaxBytes)
	if windowU > memBytes {
		memBytes = windowU
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithWindowSize(window),
		zstd.WithLowerEncoderMem(true))
	if err != nil {
		return nil, fmt.Errorf("zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(memBytes),
		zstd.WithDecoderMaxWindow(windowU),
		zstd.WithDecodeAllCapLimit(true),
		zstd.WithDecoderLowmem(true))
	if err != nil {
		_ = enc.Close()
		return nil, fmt.Errorf("zstd decoder: %w", err)
	}
	return &frameCodec{maxBytes: frameMaxBytes, enc: enc, dec: dec}, nil
}

// Close releases the encoder and decoder.
func (c *frameCodec) Close() error {
	if err := c.enc.Close(); err != nil {
		c.dec.Close()
		return fmt.Errorf("zstd encoder: %w", err)
	}
	c.dec.Close()
	return nil
}

// compressFrame compresses one frame's raw bytes through the session
// encoder into the reused buffer. The result is valid until the next
// call: the caller writes or copies it before compressing again.
func (c *frameCodec) compressFrame(raw []byte) ([]byte, error) {
	if int64(len(raw)) > c.maxBytes {
		return nil, fmt.Errorf("%w: frame exceeds frame_max_bytes", ErrUnsafePath)
	}
	out := c.enc.EncodeAll(raw, c.buf[:0])
	c.buf = out[:0]
	return out, nil
}

// decompressFrame decodes one frame through the session decoder into
// a destination of length zero and capacity exactly rawLength: the
// declared length is validated before anything is allocated, and the
// decoded length must match it.
func (c *frameCodec) decompressFrame(comp []byte, rawLength int64) ([]byte, error) {
	fail := func(err error) ([]byte, error) {
		return nil, fmt.Errorf("%w: frame decode: %w", ErrUnsafePath, err)
	}
	if rawLength <= 0 || rawLength > c.maxBytes {
		return fail(fmt.Errorf("bad raw length"))
	}
	out, err := c.dec.DecodeAll(comp, make([]byte, 0, rawLength))
	if err != nil {
		return fail(err)
	}
	if int64(len(out)) != rawLength {
		return fail(fmt.Errorf("length mismatch"))
	}
	return out, nil
}

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

// framesSchema is the .frames table schema.
const framesSchema = "gifford.capture.frames.v1"

// framesDoc is a .frames table: schema gifford.capture.frames.v1.
type framesDoc struct {
	Schema    string       `json:"schema"`
	SegmentID string       `json:"segment_id"`
	RawBytes  int64        `json:"raw_bytes"`
	RawSHA256 string       `json:"raw_sha256"`
	Frames    []frameEntry `json:"frames"`
}

func (d *framesDoc) controlSchema() string { return framesSchema }

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

// verifySegment checks a closed segment: the frames decode against
// their declared raw lengths, cover [0, R) contiguously, and hash to
// the raw digest. Decoding stays frame at a time; the segment is
// never decoded whole.
func verifySegment(raw, comp []byte, doc *framesDoc, codec *frameCodec) error {
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
		part, err := codec.decompressFrame(comp[f.Offset:f.Offset+f.Length], f.RawLength)
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
