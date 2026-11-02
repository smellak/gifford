package detect

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// now is a fixed reference time; activities are epoch seconds around it.
var benchNow = time.Unix(1796000000, 0)

func clientLine(pane string, ago time.Duration) string {
	return pane + "\t" + strconv.FormatInt(benchNow.Add(-ago).Unix(), 10)
}

func TestNoClientsIsQuiet(t *testing.T) {
	recent, certain := HumanActivity("", "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.True(t, certain)
}

func TestOtherPaneIgnored(t *testing.T) {
	out := clientLine("%7", 10*time.Second) + "\n"
	recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.True(t, certain)
}

func TestRecentActivityBlocks(t *testing.T) {
	out := clientLine("%42", 10*time.Second) + "\n"
	recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
	require.True(t, recent)
	require.True(t, certain)
}

func TestOldActivityIsQuiet(t *testing.T) {
	out := clientLine("%42", 10*time.Minute) + "\n"
	recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.True(t, certain)
}

func TestOneRecentAmongManyBlocks(t *testing.T) {
	out := clientLine("%7", time.Second) + "\n" +
		clientLine("%42", 10*time.Minute) + "\n" +
		clientLine("%42", 5*time.Second) + "\n"
	recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
	require.True(t, recent)
	require.True(t, certain)
}

func TestEmptyActivityIsQuiet(t *testing.T) {
	recent, certain := HumanActivity("%42\t\n", "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.True(t, certain)
}

func TestZeroActivityIsQuiet(t *testing.T) {
	recent, certain := HumanActivity("%42\t0\n", "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.True(t, certain)
}

func TestGarbledLineFailsClosed(t *testing.T) {
	for _, out := range []string{"%42\tnot-a-time\n", "%42\n", "%42\ta\tb\n"} {
		recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
		require.False(t, recent, "output %q", out)
		require.False(t, certain, "output %q", out)
	}
}

func TestLiteralBackslashTFailsClosed(t *testing.T) {
	// A literal backslash-t is one field, not a tab separator: garbled
	// transport, even though it starts with the target pane id.
	recent, certain := HumanActivity(`%1\t123`+"\n", "%1", benchNow, time.Minute)
	require.False(t, recent)
	require.False(t, certain)
}

func TestMalformedOtherPaneFailsClosed(t *testing.T) {
	// Garbled rows cannot be ignored even when they name another pane.
	for _, out := range []string{
		clientLine("%7", time.Second) + "\n%BAD\t\n",
		clientLine("%7", time.Second) + "\n%42\n",
		clientLine("%7", time.Second) + "\n%7\t12x\n",
	} {
		recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
		require.False(t, recent, "output %q", out)
		require.False(t, certain, "output %q", out)
	}
}

func TestFutureTimestampFailsClosed(t *testing.T) {
	out := "%42\t" + strconv.FormatInt(benchNow.Add(time.Minute).Unix(), 10) + "\n"
	recent, certain := HumanActivity(out, "%42", benchNow, time.Minute)
	require.False(t, recent)
	require.False(t, certain)
}

func TestEmptyPaneIDFailsClosed(t *testing.T) {
	recent, certain := HumanActivity(clientLine("%42", time.Second), "", benchNow, time.Minute)
	require.False(t, recent)
	require.False(t, certain)
}
