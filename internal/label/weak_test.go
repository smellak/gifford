package label

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func weakLine(id, label, reason string) string {
	return `{"window_id":"` + id + `","label":"` + label + `","reason":"` + reason +
		`","method":"agent_review","criterion_version":"` + WeakCriterionV1 +
		`","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z"}`
}

func TestImportWeakCanonicalAndAuditable(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	in := strings.TrimSuffix(weakLine(string(idA), "A", string(WeakReasonVisibleState)), "}") + `,"source":"weak"}` + "\n" +
		weakLine(string(idB), "G", string(WeakReasonNoDeterminant)) + "\n"
	n, err := ImportWeak(dir, strings.NewReader(in))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	requirePrivate(t, dir, WeakFile)
	require.Equal(t, []string{
		`{"window_id":"01JB0000000000000000000001","label":"A","reason":"visible_state_indicator","method":"agent_review","criterion_version":"weak-review-v1","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z","source":"weak"}`,
		`{"window_id":"01JB0000000000000000000002","label":"G","reason":"no_determinant_indicator","method":"agent_review","criterion_version":"weak-review-v1","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z","source":"weak"}`,
	}, fileLines(t, filepath.Join(dir, WeakFile)))

	got, bad, err := ReadWeak(dir)
	require.NoError(t, err)
	require.Zero(t, bad)
	require.Len(t, got, 2)
	require.Equal(t, SourceWeak, got[0].Source)
	require.Equal(t, WeakReasonNoDeterminant, got[1].Reason)
}

func TestImportWeakPreservesRepeatedAuditRecords(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	a := weakLine(string(idA), "A", string(WeakReasonVisibleState))
	b := strings.Replace(weakLine(string(idA), "B", string(WeakReasonVisibleState)), "reviewer-1", "reviewer-2", 1)
	b = strings.Replace(b, "2026-10-29T12:00:00Z", "2026-10-30T12:00:00Z", 1)
	n, err := ImportWeak(dir, strings.NewReader(a+"\n"+b+"\n"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, fileLines(t, filepath.Join(dir, WeakFile)), 2, "the original audited trace is not silently deduplicated")

	got, bad, err := ReadWeak(dir)
	require.NoError(t, err)
	require.Zero(t, bad)
	require.Len(t, got, 2)
	require.Equal(t, []string{"reviewer-1", "reviewer-2"}, []string{got[0].Reviewer, got[1].Reviewer})
}

func TestImportWeakRejectsAtomicallyWithoutContentInError(t *testing.T) {
	t.Parallel()

	const canary = "CANARY-weak-private-screen"
	valid := weakLine(string(idA), "A", string(WeakReasonVisibleState))
	for _, tc := range []struct {
		name string
		line string
	}{
		{"not json", `{`},
		{"unknown field", strings.TrimSuffix(valid, "}") + `,"screen":"` + canary + `"}`},
		{"bad window", strings.Replace(valid, string(idA), "window bad", 1)},
		{"bad label", strings.Replace(valid, `"label":"A"`, `"label":"Z"`, 1)},
		{"reason outside criterion", strings.Replace(valid, string(WeakReasonVisibleState), "reviewer_says_so", 1)},
		{"reason for another label", strings.Replace(valid, string(WeakReasonVisibleState), string(WeakReasonNoDeterminant), 1)},
		{"unknown criterion", strings.Replace(valid, WeakCriterionV1, "weak-label-v2", 1)},
		{"bad method", strings.Replace(valid, "agent_review", "manual_review", 1)},
		{"bad reviewer", strings.Replace(valid, "reviewer-1", "reviewer one", 1)},
		{"uppercase reviewer", strings.Replace(valid, "reviewer-1", "Reviewer-1", 1)},
		{"zero timestamp", strings.Replace(valid, "2026-10-29T12:00:00Z", "0001-01-01T00:00:00Z", 1)},
		{"non UTC timestamp", strings.Replace(valid, "2026-10-29T12:00:00Z", "2026-10-29T14:00:00+02:00", 1)},
		{"empty source", strings.TrimSuffix(valid, "}") + `,"source":""}`},
		{"null source", strings.TrimSuffix(valid, "}") + `,"source":null}`},
		{"wrong source", strings.TrimSuffix(valid, "}") + `,"source":"manual"}`},
		{"duplicate key", strings.TrimSuffix(valid, "}") + `,"label":"A"}`},
		{"duplicate case variant", strings.TrimSuffix(valid, "}") + `,"Label":"A"}`},
		{"uppercase key", strings.Replace(valid, `"window_id"`, `"WINDOW_ID"`, 1)},
		{"missing key", strings.Replace(valid, `"reviewer":"reviewer-1",`, "", 1)},
		{"trailing object", valid + `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			n, err := ImportWeak(dir, strings.NewReader(valid+"\n"+tc.line+"\n"))
			require.Error(t, err)
			require.Zero(t, n)
			require.NotContains(t, err.Error(), canary)
			_, statErr := os.Stat(filepath.Join(dir, WeakFile))
			require.ErrorIs(t, statErr, os.ErrNotExist, "the valid prefix is not appended")
		})
	}
}

func TestReadWeakSkipsBadLines(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	_, err := ImportWeak(dir, strings.NewReader(weakLine(string(idA), "A", string(WeakReasonVisibleState))))
	require.NoError(t, err)
	require.NoError(t, appendLine(dir, WeakFile, []byte("{torn\n")))
	valid := weakLine(string(idB), "A", string(WeakReasonVisibleState))
	duplicate := strings.TrimSuffix(valid, "}") + `,"label":"A","source":"weak"}`
	require.NoError(t, appendLine(dir, WeakFile, []byte(duplicate+"\n")))
	wrongCase := strings.TrimSuffix(valid, "}") + `,"Source":"weak"}`
	require.NoError(t, appendLine(dir, WeakFile, []byte(wrongCase+"\n")))
	got, bad, err := ReadWeak(dir)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 3, bad, "canonical reads reject duplicates and key case variants as well as torn JSON")

	missing := Dir(t.TempDir())
	got, bad, err = ReadWeak(missing)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, bad)
}

type weakErrorReader struct{ err error }

func (r weakErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestImportWeakReadErrorDoesNotLeak(t *testing.T) {
	t.Parallel()

	const canary = "CANARY-reader-error"
	_, err := ImportWeak(Dir(t.TempDir()), weakErrorReader{err: errors.New(canary)})
	require.Error(t, err)
	require.NotContains(t, err.Error(), canary)
}
