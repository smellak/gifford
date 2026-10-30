package fuzz

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The notice the oracle tests deliver, and lines the human types.
const (
	oracleNotice = "[GIFFORD] notice 0.0 abc123"
	hello        = "hello"
	never        = "never"
	world        = "world"
)

// oracleWant is what the human meant to send in the oracle tests.
var oracleWant = []Record{{RecordLine, hello}, {RecordCancel, never}, {RecordLine, world}}

// TestOracleCleanRun takes the human's lines in order with the notice
// whole between them: nothing mixed, nothing lost, the notice seen once.
func TestOracleCleanRun(t *testing.T) {
	t.Parallel()
	got := []Record{{RecordLine, hello}, {RecordLine, oracleNotice}, {RecordCancel, never}, {RecordLine, world}}
	v := checkLines(got, oracleWant, []string{oracleNotice})
	require.Equal(t, 4, v.Lines)
	require.Zero(t, v.Mixed)
	require.Zero(t, v.LostKeys)
	require.Equal(t, map[string]int{oracleNotice: 1}, v.Seen)
	require.Empty(t, v.Detail)

	v = checkLines(oracleWant, oracleWant, []string{oracleNotice})
	require.Zero(t, v.Mixed+v.LostKeys)
	require.Equal(t, map[string]int{oracleNotice: 0}, v.Seen, "a notice never delivered")
}

// TestOracleFindsAMixedLine seeds a notice typed into the human's draft:
// one mixed line, holding part of the notice, and the human's line lost.
func TestOracleFindsAMixedLine(t *testing.T) {
	t.Parallel()
	got := []Record{{RecordLine, hello[:3] + oracleNotice + hello[3:]}, {RecordCancel, never}, {RecordLine, world}}
	v := checkLines(got, oracleWant, []string{oracleNotice})
	require.Equal(t, 1, v.Mixed)
	require.Equal(t, len(hello)+1, v.LostKeys)
	require.Zero(t, v.Seen[oracleNotice])
	require.Contains(t, v.Detail[0], "holds part of a notice")
}

// TestOracleFindsALostKey seeds one key gone from a human line: the
// line that arrived is not what the human sent, and the line it sent
// counts as lost, its Enter included. A line that never arrives at all
// is lost too, and nothing else.
func TestOracleFindsALostKey(t *testing.T) {
	t.Parallel()
	got := []Record{{RecordLine, "helo"}, {RecordCancel, never}, {RecordLine, world}}
	v := checkLines(got, oracleWant, nil)
	require.Equal(t, len(hello)+1, v.LostKeys)
	require.Equal(t, 1, v.Mixed, "the line that arrived is not one the human sent")
	require.Contains(t, v.Detail[0], "which the human never sent there")

	got = []Record{{RecordLine, hello}, {RecordLine, world}}
	v = checkLines(got, oracleWant, nil)
	require.Equal(t, len(never)+1, v.LostKeys)
	require.Zero(t, v.Mixed)
}

// TestOracleKeepsTheOrder seeds two human lines swapped and a key
// pressed twice: neither passes as what the human sent.
func TestOracleKeepsTheOrder(t *testing.T) {
	t.Parallel()
	got := []Record{{RecordLine, world}, {RecordCancel, never}, {RecordLine, hello}}
	v := checkLines(got, oracleWant, nil)
	require.Positive(t, v.Mixed)
	require.Positive(t, v.LostKeys)

	got = []Record{{RecordLine, hello}, {RecordLine, ""}, {RecordCancel, never}, {RecordLine, world}}
	v = checkLines(got, oracleWant, nil)
	require.Equal(t, 1, v.Mixed, "an Enter the human pressed once arrived twice")
	require.Zero(t, v.LostKeys)
}

// TestOracleCountsNotices counts a notice delivered twice, and does not
// take a notice cut short, or one cancelled, for the notice.
func TestOracleCountsNotices(t *testing.T) {
	t.Parallel()
	got := []Record{{RecordLine, oracleNotice}, {RecordLine, hello}, {RecordLine, oracleNotice}, {RecordCancel, never}, {RecordLine, world}}
	v := checkLines(got, oracleWant, []string{oracleNotice})
	require.Equal(t, 2, v.Seen[oracleNotice])
	require.Zero(t, v.Mixed)

	got = []Record{{RecordLine, oracleNotice[:10]}, {RecordCancel, oracleNotice}, {RecordLine, hello}, {RecordCancel, never}, {RecordLine, world}}
	v = checkLines(got, oracleWant, []string{oracleNotice})
	require.Zero(t, v.Seen[oracleNotice])
	require.Equal(t, 2, v.Mixed)
}
