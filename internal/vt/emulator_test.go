package vt

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChunkingInvariance(t *testing.T) {
	t.Parallel()
	fixtures := append([]corpusFixture(nil), corpus...)
	fixtures = append(fixtures,
		corpusFixture{name: "combining-split", cols: 8, rows: 1, input: testCombining},
		corpusFixture{name: "variation-split", cols: 8, rows: 1, input: "✔️x"},
		corpusFixture{name: "zwj-split", cols: 8, rows: 1, input: "👩‍💻x"},
		corpusFixture{name: "modifier-split", cols: 8, rows: 1, input: "👍🏽x"},
	)
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			input := []byte(fixture.input)
			want := render(t, fixture.cols, fixture.rows, input)

			byteWrites := make([][]byte, len(input))
			for i := range input {
				byteWrites[i] = input[i : i+1]
			}
			require.Equal(t, want, render(t, fixture.cols, fixture.rows, byteWrites...))

			random := rand.New(rand.NewPCG(0x676966666f7264, uint64(len(input))))
			var randomWrites [][]byte
			for pos := 0; pos < len(input); {
				size := 1 + random.IntN(7)
				end := min(pos+size, len(input))
				randomWrites = append(randomWrites, input[pos:end])
				pos = end
			}
			require.Equal(t, want, render(t, fixture.cols, fixture.rows, randomWrites...))

			for split := range len(input) + 1 {
				require.Equal(t, want, render(t, fixture.cols, fixture.rows, input[:split], input[split:]), "split %d", split)
			}
		})
	}
}

func TestSnapshotDoesNotFlush(t *testing.T) {
	t.Parallel()
	emulator, err := New(8, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	_, err = emulator.Write([]byte("e"))
	require.NoError(t, err)
	before := emulator.Snapshot()
	require.Equal(t, []string{""}, before.Lines)
	require.True(t, before.Pending)
	_, err = emulator.Write([]byte("́x"))
	require.NoError(t, err)
	require.NoError(t, emulator.Flush())
	require.Equal(t, render(t, 8, 1, []byte("éx")), emulator.Snapshot())
}

func TestHeldBack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		input         string
		lines         []string
		pending       bool
		cursorCol     int
		cursorRow     int
		cursorVisible bool
	}{
		{name: "text", input: testHello, lines: []string{"hell", ""}, pending: true, cursorCol: 4, cursorVisible: true},
		{name: "cup", input: testABC + "\x1b[2;5H", lines: []string{testABC, ""}, cursorCol: 4, cursorRow: 1, cursorVisible: true},
		{name: "cursor-hide", input: testABC + "\x1b[?25l", lines: []string{testABC, ""}, cursorCol: 3},
		{name: "osc", input: "\x1b]0;unfinished", lines: []string{"", ""}, cursorVisible: true},
		{name: "csi", input: testABC + "\x1b[", lines: []string{testABC, ""}, cursorCol: 3, cursorVisible: true},
		{name: "partial-utf8", input: "a\xcc", lines: []string{"", ""}, pending: true, cursorVisible: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			emulator, newErr := New(10, 2)
			require.NoError(t, newErr)
			t.Cleanup(func() { require.NoError(t, emulator.Close()) })
			_, writeErr := emulator.Write([]byte(test.input))
			require.NoError(t, writeErr)
			snapshot := emulator.Snapshot()
			require.Equal(t, test.lines, snapshot.Lines)
			require.Equal(t, test.pending, snapshot.Pending)
			require.Equal(t, test.cursorCol, snapshot.CursorCol)
			require.Equal(t, test.cursorRow, snapshot.CursorRow)
			require.Equal(t, test.cursorVisible, snapshot.CursorVisible)
			if test.name == "partial-utf8" {
				_, writeErr = emulator.Write([]byte("\x81x"))
				require.NoError(t, writeErr)
				require.NoError(t, emulator.Flush())
				require.Equal(t, []string{"áx", ""}, emulator.Snapshot().Lines)
			}
		})
	}
}

func TestSameStreamSameScreen(t *testing.T) {
	t.Parallel()
	stream := []byte("main\x1b[?1049h\x1b[2J\x1b[H👩‍💻 12:34:56\x1b[?25l")
	whole := render(t, 40, 4, stream)
	pieces := [][]byte{stream[:3], stream[3:12], stream[12:21], stream[21:25], stream[25:]}
	chunked := render(t, 40, 4, pieces...)
	require.Equal(t, whole, chunked)
	require.Equal(t, Hash(whole), Hash(chunked))
}

func TestQueriesNeverBlock(t *testing.T) {
	t.Parallel()
	done := make(chan error, 1)
	go func() {
		emulator, err := New(80, 24)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = emulator.Close() }()
		queries := strings.Repeat("\x1b[6n\x1b[c\x1b[>c", 1_000) + "\x1b[?2048h"
		if _, err = emulator.Write([]byte(queries)); err == nil {
			err = emulator.Resize(100, 30)
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("terminal queries or resize blocked")
	}
}

func TestUnterminatedString(t *testing.T) {
	t.Parallel()
	emulator, err := New(10, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	_, err = emulator.Write([]byte("\x1b]0;"))
	require.NoError(t, err)
	chunk := []byte(strings.Repeat("x", 4<<10))
	for range (16 << 20) / len(chunk) {
		_, err = emulator.Write(chunk)
		require.NoError(t, err)
	}
	_, err = emulator.Write([]byte("\x07ok"))
	require.NoError(t, err)
	require.NoError(t, emulator.Flush())
	require.Equal(t, []string{"ok"}, emulator.Snapshot().Lines)
}

func TestSizes(t *testing.T) {
	t.Parallel()
	invalid := [][2]int{{0, 1}, {-1, 1}, {1, 0}, {1, -1}, {MaxCols + 1, 1}, {1, MaxRows + 1}, {1_000, 251}}
	for _, size := range invalid {
		_, err := New(size[0], size[1])
		require.ErrorIs(t, err, ErrInvalidSize)
	}
	for _, size := range [][2]int{{1, 1}, {10_000, 25}, {500, 500}} {
		emulator, err := New(size[0], size[1])
		require.NoError(t, err)
		require.NoError(t, emulator.Close())
	}
	emulator, err := New(10, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	_, err = emulator.Write([]byte("one\r\ntwo"))
	require.NoError(t, err)
	require.ErrorIs(t, emulator.Resize(1_000, 251), ErrInvalidSize)
	require.Equal(t, 10, emulator.Snapshot().Cols)
	require.NoError(t, emulator.Resize(8, 2))
	require.Equal(t, []string{"one", "two"}, emulator.Snapshot().Lines)
}

func TestClose(t *testing.T) {
	t.Parallel()
	emulator, err := New(10, 1)
	require.NoError(t, err)
	_, err = emulator.Write([]byte("ok\x1b[6n"))
	require.NoError(t, err)
	require.NoError(t, emulator.Flush())
	last := emulator.Snapshot()
	require.NoError(t, emulator.Close())
	select {
	case <-emulator.replyDone:
	default:
		t.Fatal("reply drain still running after Close")
	}
	require.NoError(t, emulator.Close())
	n, err := emulator.Write([]byte("x"))
	require.Zero(t, n)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, emulator.Flush(), ErrClosed)
	require.ErrorIs(t, emulator.Resize(20, 2), ErrClosed)
	require.Equal(t, last, emulator.Snapshot())
}

func TestWriteNeverPanics(t *testing.T) {
	t.Parallel()
	emulator, err := New(10, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	inputs := [][]byte{{0xff, 0xfe}, []byte("\x1b[" + strings.Repeat("9;", 10_000) + "m"), []byte("\x1bc")}
	for _, input := range inputs {
		n, writeErr := emulator.Write(input)
		require.NoError(t, writeErr)
		require.Equal(t, len(input), n)
	}
	require.NotPanics(t, func() { _ = emulator.Snapshot() })
}

func BenchmarkSyntheticThroughput(b *testing.B) {
	unit := []byte("status 12:34:56\r\n\x1b[2K⠋ working é 👩‍💻\x1b[1A\r")
	stream := make([]byte, 8<<20)
	for pos := 0; pos < len(stream); {
		pos += copy(stream[pos:], unit)
	}
	b.SetBytes(int64(len(stream)))
	b.ResetTimer()
	for range b.N {
		emulator, err := New(80, 24)
		if err != nil {
			b.Fatal(err)
		}
		for pos := 0; pos < len(stream); pos += 4 << 10 {
			end := min(pos+(4<<10), len(stream))
			if _, err = emulator.Write(stream[pos:end]); err != nil {
				b.Fatal(err)
			}
		}
		if err = emulator.Flush(); err != nil {
			b.Fatal(err)
		}
		if err = emulator.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
