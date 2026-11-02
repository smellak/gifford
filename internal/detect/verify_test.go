package detect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic post-write screens. notice is the allowlisted line the
// writer put in the box without submitting it.

const verifyNotice = "[GIFFORD] 1 pending message"

func TestVerifyExactShell(t *testing.T) {
	certain, exact := VerifyNotice(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ "+verifyNotice), 80, 24, verifyNotice)
	require.True(t, certain)
	require.True(t, exact)
}

func TestVerifyExactCodex(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› "+verifyNotice), 80, 24, verifyNotice)
	require.True(t, certain)
	require.True(t, exact)
}

func TestVerifyExactLabelled(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("mysession   › "+verifyNotice), 80, 24, verifyNotice)
	require.True(t, certain)
	require.True(t, exact)
}

func TestVerifyPrefixIsNotExact(t *testing.T) {
	// Partial text with no continuation: certainly not the notice.
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› [GIFFORD] 1 pend"), 80, 24, verifyNotice)
	require.True(t, certain)
	require.False(t, exact)
}

func TestVerifySuffixIsNotExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› "+verifyNotice+" hello"), 80, 24, verifyNotice)
	require.True(t, certain)
	require.False(t, exact)
}

func TestVerifyMixedIsNotExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› pre "+verifyNotice+" post"), 80, 24, verifyNotice)
	require.True(t, certain)
	require.False(t, exact)
}

func TestVerifyPlaceholderIsNotExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, verifyNotice)
	require.True(t, certain)
	require.False(t, exact)
}

func TestVerifyWrapIsUncertain(t *testing.T) {
	// The notice split across two rows may be a wrap: never Enter.
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› [GIFFORD] 1 pend\ning message"), 80, 24, verifyNotice)
	require.False(t, certain)
	require.False(t, exact)
}

func TestVerifyNoComposerIsUncertain(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, verifyNotice)
	require.False(t, certain)
	require.False(t, exact)
}

func TestVerifyEmptyNoticeRefused(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› "+verifyNotice), 80, 24, "")
	require.False(t, certain)
	require.False(t, exact)
}

func TestVerifyShellSuffixIsNotExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ "+verifyNotice+" extra"), 80, 24, verifyNotice)
	require.True(t, certain)
	require.False(t, exact)
}

// colonNotice is the shipped notice: «: » first, so a shell that hosts the
// CLI would only run «:».
const colonNotice = ": [GIFFORD] 1 pending message"

func TestVerifyExactCodexColonNotice(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› "+colonNotice), 80, 24, colonNotice)
	require.True(t, certain)
	require.True(t, exact)
}

func TestVerifyColonNoticeMissingItsColonIsNotExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		[]byte("› "+verifyNotice), 80, 24, colonNotice)
	require.True(t, certain)
	require.False(t, exact, "the composer misses the leading colon")
}
