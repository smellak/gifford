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

func TestZstdRoundTrip(t *testing.T) {
	t.Parallel()
	for _, raw := range [][]byte{
		{},
		[]byte("hello"),
		bytes.Repeat([]byte("abcdef\x1b[0m"), 1<<20),
	} {
		comp, err := compressFrame(raw)
		require.NoError(t, err)
		if len(raw) > 100 {
			require.Less(t, len(comp), len(raw))
		}
		back, err := decompressFrame(comp)
		require.NoError(t, err)
		require.Equal(t, raw, back)
	}
	_, err := decompressFrame([]byte("not zstd"))
	require.Error(t, err)
	corrupt, err := compressFrame([]byte(strings.Repeat("x", 1000)))
	require.NoError(t, err)
	corrupt[10] ^= 0xff
	_, err = decompressFrame(corrupt)
	require.Error(t, err)
}

func TestHeadroom(t *testing.T) {
	t.Parallel()
	require.Equal(t, int64(1048576), headroom(0))
	require.Equal(t, int64(1000000+15625+1048576), headroom(1000000))
}

func TestVerifySegment(t *testing.T) {
	t.Parallel()
	raw := bytes.Repeat([]byte("chunk-data-\x1b[31m"), 4096)
	partA, err := compressFrame(raw[:len(raw)/2])
	require.NoError(t, err)
	partB, err := compressFrame(raw[len(raw)/2:])
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
	require.NoError(t, verifySegment(raw, comp, doc))
	broken := *doc
	broken.RawSHA256 = strings.Repeat("0", 64)
	require.ErrorIs(t, verifySegment(raw, comp, &broken), ErrUnsafePath)
	require.ErrorIs(t, verifySegment(raw, comp[:len(comp)-1], doc), ErrUnsafePath)
	empty := *doc
	empty.Frames = nil
	empty.RawBytes = 0
	empty.RawSHA256 = sha256Hex(nil)
	require.ErrorIs(t, verifySegment(raw, comp, &empty), ErrUnsafePath)
}
