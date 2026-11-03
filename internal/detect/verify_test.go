package detect

import (
	"strings"
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

// codexPaneCapture is a multi-row Codex pane as capture-pane prints it:
// rows end with a bare LF, and the footer sits below the composer.
func codexPaneCapture(box string) []byte {
	return []byte(strings.Join([]string{
		"",
		"• Ran the checks and summarized the results for the user",
		"",
		box,
		"",
		"  model default fast · ~/project",
	}, "\n"))
}

// A real delivery left the exact notice in a Codex box, and the verifier
// still saw no composer: the LF-only rows drew a staircase. Both faces of
// that pane must hold once the capture is normalized like Classify does.
func TestVerifyNormalizesCaptureLineEndings(t *testing.T) {
	t.Parallel()
	notice := ": " + verifyNotice
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
		codexPaneCapture("› "+notice), 80, 24, notice)
	require.True(t, certain, "the repainted box holds the notice")
	require.True(t, exact)

	certain, exact = VerifyNotice(HarnessCodex, DefaultTable(),
		codexPaneCapture("› "), 80, 24, notice)
	require.True(t, certain, "the box before the repaint is still a box")
	require.False(t, exact)
}

func TestVerifyOpenCodeMultiRowBox(t *testing.T) {
	t.Parallel()
	notice := ": " + verifyNotice
	capture := openCodeLiveFooterCapture(notice, false)

	certain, exact := VerifyNotice(HarnessOpenCode, DefaultTable(), capture, 120, 40, notice)
	require.True(t, certain)
	require.True(t, exact, "the notice is the sole content in the three-row box")

	withDraft := []byte(strings.Replace(
		string(capture),
		"  ┃\n  ┃  "+notice,
		"  ┃  public synthetic draft\n  ┃  "+notice,
		1,
	))
	certain, exact = VerifyNotice(HarnessOpenCode, DefaultTable(), withDraft, 120, 40, notice)
	require.True(t, certain)
	require.False(t, exact, "any other box content must forbid Enter")
}

func TestVerifyOpenCodeMultiRowMutant(t *testing.T) {
	t.Parallel()
	notice := ": " + verifyNotice
	table := DefaultTable()
	profile := table[HarnessOpenCode]
	profile.MultiRowBox = false
	table[HarnessOpenCode] = profile

	_, exact := VerifyNotice(
		HarnessOpenCode,
		table,
		openCodeLiveFooterCapture(notice, false),
		120,
		40,
		notice,
	)
	require.False(t, exact, "removing the multi-row projection must close the pre-Enter gate")
}
