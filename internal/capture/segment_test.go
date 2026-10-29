package capture

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameCodec(t *testing.T) {
	t.Parallel()
	chunks := []frameChunk{{seq: 0, data: []byte("hello")}, {seq: 7, data: bytes.Repeat([]byte{0x1b}, 4096)}, {seq: 8}}
	enc, err := encodeFrame(chunks)
	require.NoError(t, err)
	back, err := decodeFrame(enc)
	require.NoError(t, err)
	require.Len(t, back, 3)
	for i, c := range chunks {
		require.Equal(t, c.seq, back[i].seq)
		require.True(t, bytes.Equal(c.data, back[i].data))
	}
	good, err := encodeFrame(chunks)
	require.NoError(t, err)
	for _, bad := range [][]byte{
		nil,
		good[:3],
		append(append([]byte{}, good[:4]...), 0x7f),
		good[:len(good)-1],
		append(good, 0x00),
		bytes.Replace(good, []byte("hello"), []byte("hell"), 1),
	} {
		_, err := decodeFrame(bad)
		require.ErrorIs(t, err, ErrUnsafePath)
	}
}

func TestFrameGrouping(t *testing.T) {
	t.Parallel()
	runs, err := groupFrameRuns([]int64{10, 20, 30}, 100)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 1, 2}}, runs)
	runs, err = groupFrameRuns([]int64{60, 60, 60}, 1000)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0, 1, 2}}, runs)
	runs, err = groupFrameRuns([]int64{60, 60, 60}, 100)
	require.NoError(t, err)
	require.Equal(t, [][]int{{0}, {1}, {2}}, runs)
	runs, err = groupFrameRuns(nil, 100)
	require.NoError(t, err)
	require.Empty(t, runs)
	_, err = groupFrameRuns([]int64{101}, 100)
	require.ErrorIs(t, err, ErrUnsafePath)
}

func TestCodecWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		frameMax int64
		want     int
	}{
		{1, 1024},
		{1023, 1024},
		{1024, 1024},
		{1025, 2048},
		{3000000, 4194304},
		{4194304, 4194304},
		{4194305, 8388608},
		{1 << 30, 8 << 20},
	} {
		if got := codecWindow(tc.frameMax); got != tc.want {
			t.Errorf("codecWindow(%d) = %d, want %d", tc.frameMax, got, tc.want)
		}
	}
}

func TestNewFrameCodecRejects(t *testing.T) {
	t.Parallel()
	for _, maxBytes := range []int64{0, -1} {
		if _, err := newFrameCodec(maxBytes); err == nil {
			t.Errorf("newFrameCodec(%d): want error", maxBytes)
		}
	}
}

func TestZstdRoundTrip(t *testing.T) {
	t.Parallel()
	codec, err := newFrameCodec(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = codec.Close() })
	// Valid tiny frames, the 1024 boundary and above: the decoder
	// cap is max(frame_max_bytes, W), never frame_max_bytes alone.
	for _, size := range []int{1, 5, 512, 1023, 1024, 1025, 65537} {
		raw := bytes.Repeat([]byte("abcdef\x1b[0m"), (size+7)/8)[:size]
		comp, err := codec.compressFrame(raw)
		require.NoError(t, err, "size %d", size)
		back, err := codec.decompressFrame(comp, int64(size))
		require.NoError(t, err, "size %d", size)
		require.Equal(t, raw, back, "size %d", size)
	}
	// Repeated operations on one codec stay stable.
	raw := bytes.Repeat([]byte("repeatable-\x1b[31m"), 64)
	for range 10 {
		comp, err := codec.compressFrame(raw)
		require.NoError(t, err)
		back, err := codec.decompressFrame(comp, int64(len(raw)))
		require.NoError(t, err)
		require.Equal(t, raw, back)
	}
	_, err = codec.decompressFrame([]byte("not zstd"), 7)
	require.Error(t, err)
	corrupt, err := codec.compressFrame([]byte(strings.Repeat("x", 1000)))
	require.NoError(t, err)
	_, err = codec.decompressFrame(corrupt[:len(corrupt)-1], 1000)
	require.Error(t, err)
}

func TestZstdOversized(t *testing.T) {
	t.Parallel()
	// A sub-KiB frame_max_bytes still decodes through the 1024
	// window, and anything over the max is refused before decoding.
	codec, err := newFrameCodec(100)
	require.NoError(t, err)
	t.Cleanup(func() { _ = codec.Close() })
	raw := bytes.Repeat([]byte("tiny-frame"), 10)
	comp, err := codec.compressFrame(raw)
	require.NoError(t, err)
	back, err := codec.decompressFrame(comp, int64(len(raw)))
	require.NoError(t, err)
	require.Equal(t, raw, back)
	// A destination cap one byte short of the payload is rejected.
	_, err = codec.decompressFrame(comp, int64(len(raw))-1)
	require.ErrorIs(t, err, ErrUnsafePath)
	// A declared length over the max never reaches the decoder.
	_, err = codec.decompressFrame(comp, 101)
	require.ErrorIs(t, err, ErrUnsafePath)
	_, err = codec.decompressFrame(comp, 0)
	require.ErrorIs(t, err, ErrUnsafePath)
	// Compression over the max is refused before encoding.
	_, err = codec.compressFrame(bytes.Repeat([]byte("o"), 101))
	require.ErrorIs(t, err, ErrUnsafePath)
}

func TestZstdMaximumSetting(t *testing.T) {
	t.Parallel()
	// The default frame_max_bytes round-trips a multi-frame payload
	// at the maximum approved setting.
	codec, err := newFrameCodec(4194304)
	require.NoError(t, err)
	t.Cleanup(func() { _ = codec.Close() })
	raw := bytes.Repeat([]byte("maximum-setting-payload-\x1b[0m"), 1<<16)
	comp, err := codec.compressFrame(raw)
	require.NoError(t, err)
	back, err := codec.decompressFrame(comp, int64(len(raw)))
	require.NoError(t, err)
	require.Equal(t, raw, back)
}

func TestHeadroom(t *testing.T) {
	t.Parallel()
	require.Equal(t, int64(1048576), headroom(0))
	require.Equal(t, int64(1000000+15625+1048576), headroom(1000000))
}

func TestVerifySegment(t *testing.T) {
	t.Parallel()
	codec, err := newFrameCodec(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = codec.Close() })
	raw := bytes.Repeat([]byte("chunk-data-\x1b[31m"), 4096)
	partA, err := codec.compressFrame(raw[:len(raw)/2])
	require.NoError(t, err)
	partB, err := codec.compressFrame(raw[len(raw)/2:])
	require.NoError(t, err)
	comp := append(append([]byte{}, partA...), partB...)
	doc := &framesDoc{
		Schema: "gifford.capture.frames.v1", SegmentID: "seg-01AAAAAAAAAAAAAAAAAAAAAAAA",
		RawBytes: int64(len(raw)), RawSHA256: sha256Hex(raw),
		Frames: []frameEntry{
			{RawOffset: 0, RawLength: int64(len(raw) / 2), Offset: 0, Length: int64(len(partA))},
			{RawOffset: int64(len(raw) / 2), RawLength: int64(len(raw) - len(raw)/2), Offset: int64(len(partA)), Length: int64(len(partB))},
		},
	}
	require.NoError(t, doc.validate())
	require.NoError(t, verifySegment(raw, comp, doc, codec))
	broken := *doc
	broken.RawSHA256 = strings.Repeat("0", 64)
	require.ErrorIs(t, verifySegment(raw, comp, &broken, codec), ErrUnsafePath)
	require.ErrorIs(t, verifySegment(raw, comp[:len(comp)-1], doc, codec), ErrUnsafePath)
	empty := *doc
	empty.Frames = nil
	empty.RawBytes = 0
	empty.RawSHA256 = sha256Hex(nil)
	require.ErrorIs(t, verifySegment(raw, comp, &empty, codec), ErrUnsafePath)
}
