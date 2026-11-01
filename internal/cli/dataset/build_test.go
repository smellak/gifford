package dataset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	clieval "github.com/smellak/gifford/internal/cli/eval"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/vt"
)

// Canaries: typed by a human, they must never reach the dataset.
const (
	canaryOpen  = "CANARY-OPEN-7q2x"
	canaryPaste = "CANARY-PASTE-9k4m"
	// canaryLong is in the first rows of a paste longer than the screen.
	canaryLong = "CANARY-LONG-6p0z"
	// canaryHidden is typed in a TUI that hides the terminal cursor.
	canaryHidden = "CANARY-HIDDEN-3v8n"
	// hiddenClosedPrompt is the prompt row of a closed draft under a
	// hidden cursor: it stays.
	hiddenClosedPrompt = "hidden-closed-prompt-row"
	// closedPrompt sits on the prompt row of a closed draft: it stays.
	closedPrompt = "submitted-prompt-row"
	maskedName   = "AcmeBank"
	harnessVer   = "0.50.0 (Codex)"
)

// draftClosed is the input guard's state for a submitted draft, and
// draftOpen for one being typed.
const (
	draftClosed = "closed"
	draftOpen   = "open"
)

// Flags of gifford dataset build.
const (
	flagOut          = "--out"
	flagScreenLines  = "--screen-lines"
	flagMask         = "--mask"
	flagNoMask       = "--no-mask"
	flagAlign        = "--align-config"
	flagSplitFile    = "--split-file"
	flagSyntheticDir = "--synthetic-dir"
	flagLiveForm     = "--live-form"
	flagLiveFormV2   = "--live-form-v2"
)

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

// chunk is one recorded output piece, or a draft state, at ms.
type chunk struct {
	ms     int64
	output string
	draft  string
}

// writeSession records one synthetic `gifford run` session in dataDir,
// as gifford run writes it, with the screen hashes a live vt gives.
func writeSession(t *testing.T, dataDir, id string, chunks []chunk) {
	t.Helper()
	writeSessionAs(t, dataDir, id, protocol.HarnessCodex, harnessVer, chunks)
}

// writeSessionAs is writeSession for a harness at a version.
func writeSessionAs(t *testing.T, dataDir, id string, harness protocol.Harness, version string, chunks []chunk) {
	t.Helper()
	var raw, events bytes.Buffer
	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	rec := session.NewRecorder(&raw)
	log := session.NewEventLog(&events, id, session.EventLogOptions{})
	seq := int64(0)
	emit := func(ms int64, typ protocol.EventType, payload any) {
		log.Emit(session.Event{
			Session: id, Type: typ, Payload: payload, Sequence: seq,
			WallTime: t0.Add(time.Duration(ms) * time.Millisecond), MonotonicNS: ms * 1e6,
		})
		seq++
	}
	emit(0, protocol.EventTypePTYOpened, session.PTYOpened{Cols: 80, Rows: 24, Harness: harness, HarnessVersion: version})
	last := int64(0)
	for _, c := range chunks {
		last = c.ms
		if c.draft != "" {
			emit(c.ms, protocol.EventTypeInputDraftUpdated, session.InputDraft{State: c.draft})
			continue
		}
		_, err := emu.Write([]byte(c.output))
		require.NoError(t, err)
		n, err := rec.Append(t0.Add(time.Duration(c.ms)*time.Millisecond), c.ms*1e6, []byte(c.output))
		require.NoError(t, err)
		emit(c.ms, protocol.EventTypeIOChunk, session.IOChunkEvent{
			ByteCount: int64(len(c.output)), ScreenHashAfter: vt.Hash(emu.Snapshot()), RecordingSequence: n,
			ChunkDigest: session.ChunkDigest([]byte(c.output)),
		})
	}
	emit(last+500, protocol.EventTypePTYClosed, session.PTYClosed{})
	require.NoError(t, log.Close())
	dir := session.RecordingsDir(dataDir)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, id+session.RecordingSuffix), raw.Bytes(), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, id+session.EventsSuffix), events.Bytes(), 0o600))
}

type env struct {
	dataDir, configDir, work, alignCfg, mask, secret string
}

// screenLines is the fixture's ml.screen_lines, and maskNone the
// ml.mask of variant B.
const (
	screenLines = "24"
	maskNone    = "mask: none"
)

// writeConfig writes config.yaml with the ml keys given, as YAML lines.
func writeConfig(t *testing.T, e env, ml ...string) {
	t.Helper()
	body := "schema_version: 1\n"
	if len(ml) > 0 {
		body += "ml:\n  " + strings.Join(ml, "\n  ") + "\n"
	}
	require.NoError(t, os.MkdirAll(e.configDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(e.configDir, "config.yaml"), []byte(body), 0o600))
}

// setup records the synthetic sessions, annotates every window but one
// and writes config.yaml with ml.screen_lines and the mask list as
// ml.mask. It sets XDG_DATA_HOME and XDG_CONFIG_HOME, so its tests do not
// run in parallel.
func setup(t *testing.T) env {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "data"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	e := env{
		dataDir: filepath.Join(root, "data", "gifford"), configDir: filepath.Join(root, "config", "gifford"),
		work: filepath.Join(root, "work"),
	}
	require.NoError(t, os.MkdirAll(e.work, 0o700))
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	e.secret = secret

	// A draft open and visible at the anchor.
	writeSession(t, e.dataDir, "sess-open", []chunk{
		{ms: 100, output: "build ok\r\n\r\n> " + canaryOpen + " "},
		{ms: 200, draft: draftOpen},
		{ms: 2100, output: " "},
	})
	// A pasted draft of several lines.
	writeSession(t, e.dataDir, "sess-paste", []chunk{
		{ms: 100, output: "agent idle\r\n\r\n> " + canaryPaste + " one\r\n  " + canaryPaste + " two\r\n  " + canaryPaste + " three "},
		{ms: 200, draft: "pasting"},
		{ms: 2100, output: " "},
	})
	// A paste of more than 10 KiB: it streams past, then the TUI redraws
	// its first rows, the canary on top, more than 8 rows above the
	// cursor.
	var paste strings.Builder
	for i := 0; paste.Len() < 10<<10; i++ {
		fmt.Fprintf(&paste, "  %s streamed paste line %04d of a long human paste\r\n", canaryLong, i)
	}
	paste.WriteString("\x1b[2J\x1b[H> " + canaryLong + " pasted row 0\r\n  " + canaryLong + " pasted row 1\r\n")
	for i := 2; i < 20; i++ {
		fmt.Fprintf(&paste, "  pasted row %d\r\n", i)
	}
	paste.WriteString("  pasted row 20")
	require.Greater(t, paste.Len(), 10<<10)
	writeSession(t, e.dataDir, "sess-longpaste", []chunk{
		{ms: 100, output: paste.String()},
		{ms: 200, draft: "pasting"},
		{ms: 2100, output: " "},
	})
	// An open draft in a TUI that hides the terminal cursor: the cursor
	// does not say where the draft is, so its one window is dropped.
	writeSession(t, e.dataDir, "sess-hidden", []chunk{
		{ms: 100, output: "\x1b[?25ltool output\r\n> " + canaryHidden + "\r\n\r\nstatus bar"},
		{ms: 200, draft: draftOpen},
		{ms: 1100, output: " "},
	})
	// A hidden cursor with the draft closed: nothing is removed.
	writeSession(t, e.dataDir, "sess-hidden-closed", []chunk{
		{ms: 100, output: "\x1b[?25ldone\r\n> " + hiddenClosedPrompt + " "},
		{ms: 200, draft: draftClosed},
		{ms: 2100, output: " "},
	})
	// A draft known closed: its prompt row stays, the screen is only
	// redacted and masked.
	writeSession(t, e.dataDir, "sess-closed", []chunk{
		{ms: 100, output: "done\r\n" + maskedName + " portal, token " + secret + "\r\n> " + closedPrompt + " "},
		{ms: 200, draft: draftClosed},
		{ms: 2100, output: " "},
	})
	for i := range 9 {
		writeSession(t, e.dataDir, fmt.Sprintf("sess-filler-%d", i), []chunk{
			{ms: 100, output: fmt.Sprintf("step %d of 9 compiling\r\n", i)},
			{ms: 200, draft: draftClosed},
			{ms: 2100, output: "ok\r\n"},
		})
	}

	// Every window annotated but the first one, which stays unlabeled.
	src, err := replay.OpenAll(e.dataDir, 0, nil)
	require.NoError(t, err)
	var anns []label.Annotation
	first := true
	for i := 0; ; i++ {
		rec, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if first {
			first = false
			continue
		}
		anns = append(anns, label.Annotation{
			WindowID: rec.WindowID, Label: []dataset.Label{dataset.LabelA, dataset.LabelB, dataset.LabelC}[i%3],
			Annotator: "tester", LabeledAt: t0,
		})
	}
	require.NoError(t, label.SaveAnnotations(label.Dir(e.dataDir), anns))

	// No weak transition and no margin: every annotated window is final.
	e.alignCfg = filepath.Join(e.work, "align.yaml")
	require.NoError(t, os.WriteFile(e.alignCfg, []byte("margin_ms: 0\nhook_harnesses: []\nweak:\n  silence_seconds: 3600\n"), 0o600))
	e.mask = filepath.Join(e.work, "mask.txt")
	require.NoError(t, os.WriteFile(e.mask, []byte("# synthetic\nacmebank\n"), 0o600))
	writeConfig(t, e, "screen_lines: "+screenLines, "mask: "+e.mask)
	return e
}

func buildCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{"build"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// treeOf maps each file under dir to its bytes.
func treeOf(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		rel, _ := filepath.Rel(dir, path)
		out[rel] = string(b)
		return err
	}))
	return out
}

func readWindows(t *testing.T, dir string) []dataset.Window {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, dataset.WindowsDir))
	require.NoError(t, err)
	out := make([]dataset.Window, 0, len(ents))
	for _, e := range ents {
		f, err := os.Open(filepath.Join(dir, dataset.WindowsDir, e.Name()))
		require.NoError(t, err)
		w, err := dataset.ParseWindow(f)
		require.NoError(t, f.Close())
		require.NoError(t, err)
		out = append(out, w)
	}
	return out
}

func readBuildManifest(t *testing.T, dir string) dataset.Manifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	return m
}

func relabelTrainWindows(t *testing.T, e env, target dataset.Label, count int) {
	t.Helper()
	src, err := replay.OpenAll(e.dataDir, 0, nil)
	require.NoError(t, err)
	anns := make([]label.Annotation, 0, count)
	for len(anns) < count {
		rec, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		split, err := (dataset.SplitV1{}).Split(rec.GroupDigest)
		require.NoError(t, err)
		if split != dataset.SplitTrain || rec.NoOutputYet || rec.Provenance.SessionID == "sess-hidden" {
			continue
		}
		anns = append(anns, label.Annotation{
			WindowID: rec.WindowID, Label: target, Annotator: "synthetic-tester", LabeledAt: t0.Add(time.Minute),
		})
	}
	require.Len(t, anns, count)
	require.NoError(t, label.SaveAnnotations(label.Dir(e.dataDir), anns))
}

func annotateSessions(t *testing.T, e env, labels map[protocol.OpaqueID]dataset.Label) {
	t.Helper()
	src, err := replay.OpenAll(e.dataDir, 0, nil)
	require.NoError(t, err)
	var anns []label.Annotation
	for {
		rec, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		target, ok := labels[rec.Provenance.SessionID]
		if !ok {
			continue
		}
		anns = append(anns, label.Annotation{
			WindowID: rec.WindowID, Label: target, Annotator: "harness-evidence-test", LabeledAt: t0.Add(2 * time.Minute),
		})
	}
	require.NotEmpty(t, anns)
	require.NoError(t, label.SaveAnnotations(label.Dir(e.dataDir), anns))
}

func syntheticWindow(template dataset.Window, n int) dataset.Window {
	id := protocol.ULID(fmt.Sprintf("01KSYN%020d", n))
	digest := func(kind string) protocol.Digest {
		return protocol.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(fmt.Sprintf("synthetic-%s-%d", kind, n)))))
	}
	template.WindowID = id
	template.Target = dataset.Target{Label: dataset.LabelD, Source: dataset.LabelSourceSynthetic}
	template.Provenance.SessionID = protocol.OpaqueID(fmt.Sprintf("synthetic-session-%d", n))
	template.Provenance.CaptureSessionID = template.Provenance.SessionID
	template.Provenance.SourceQuality = protocol.SourceQualitySynthetic
	template.Provenance.SourceRefs = []protocol.BlobRef{protocol.BlobRef(fmt.Sprintf("blob://synthetic/window-%d", n))}
	template.Provenance.SourceDigest = digest("source")
	template.Screen.ObservationForm = ""
	template.Screen.ObservationPolicyVersion = ""
	template.Screen.ObservationEpisodeDigest = ""
	template.Screen.ObservationAnchorDigest = ""
	template.Split = dataset.SplitAssignment{Split: dataset.SplitTrain, GroupDigest: digest("group")}
	return template
}

func writeWindow(t *testing.T, dir string, w dataset.Window) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	raw, err := json.Marshal(w)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, dataset.WindowFile(string(w.WindowID))), append(raw, '\n'), 0o600))
}

func TestBuildSyntheticDir(t *testing.T) {
	e := setup(t)
	relabelTrainWindows(t, e, dataset.LabelD, 7)

	baseDir := filepath.Join(e.work, "base")
	_, err := buildCmd(t, flagOut, baseDir, flagAlign, e.alignCfg)
	require.NoError(t, err)
	base := readBuildManifest(t, baseDir)
	require.Equal(t, int64(7), base.Counts.ByLabel.D)

	var template dataset.Window
	for _, w := range readWindows(t, baseDir) {
		if w.Split.Split == dataset.SplitTrain {
			template = w
			break
		}
	}
	require.NotEmpty(t, template.WindowID)

	syntheticDir := filepath.Join(e.work, "synthetic")
	for i := 1; i <= 3; i++ {
		w := syntheticWindow(template, i)
		require.NoError(t, dataset.ValidateSyntheticTrainingWindow(w))
		writeWindow(t, syntheticDir, w)
	}

	exactDir := filepath.Join(e.work, "with-synthetic")
	stdout, err := buildCmd(t, flagOut, exactDir, flagAlign, e.alignCfg, flagSyntheticDir, syntheticDir)
	require.NoError(t, err)
	exact := readBuildManifest(t, exactDir)
	require.Equal(t, base.Counts.Windows+3, exact.Counts.Windows)
	require.Equal(t, int64(10), exact.Counts.ByLabel.D)
	require.Contains(t, stdout, "by label:")
	require.Contains(t, stdout, "D 10")

	syntheticD := 0
	for _, entry := range exact.Entries {
		if entry.Label == dataset.LabelD && entry.SourceQuality == protocol.SourceQualitySynthetic && entry.LabelSource == dataset.LabelSourceSynthetic {
			syntheticD++
		}
	}
	require.Equal(t, 3, syntheticD)
	for _, fault := range exact.Readiness.Faults {
		require.False(t, fault.Rule == dataset.RuleSyntheticShare && fault.Subject == "D",
			"3 of the final 10 D windows is exactly the allowed 30%%")
	}

	var inspected bytes.Buffer
	require.NoError(t, inspect(&inspected, filepath.Join(exactDir, dataset.ManifestFile)))
	t.Log("dataset inspect output:\n" + inspected.String())
	require.Contains(t, inspected.String(), fmt.Sprintf(": %d windows", exact.Counts.Windows))
	require.NotContains(t, inspected.String(), "D: 30% synthetic", "a share at the threshold is not a readiness fault")

	fourth := syntheticWindow(template, 4)
	writeWindow(t, syntheticDir, fourth)
	aboveDir := filepath.Join(e.work, "above-synthetic-limit")
	_, err = buildCmd(t, flagOut, aboveDir, flagAlign, e.alignCfg, flagSyntheticDir, syntheticDir)
	require.NoError(t, err, "readiness faults are recorded rather than aborting a valid build")
	above := readBuildManifest(t, aboveDir)
	require.Contains(t, above.Readiness.Faults, dataset.Fault{
		Rule: dataset.RuleSyntheticShare, Subject: "D", Split: dataset.SplitTrain, Have: 36, Want: 30,
	}, "4 of the final 11 D windows exceeds 30%")

	invalidDir := filepath.Join(e.work, "synthetic-validation")
	invalid := syntheticWindow(template, 5)
	invalid.Split.Split = dataset.SplitValidation
	writeWindow(t, invalidDir, invalid)
	refusedDir := filepath.Join(e.work, "refused-synthetic")
	_, err = buildCmd(t, flagOut, refusedDir, flagAlign, e.alignCfg, flagSyntheticDir, invalidDir)
	require.ErrorIs(t, err, dataset.ErrInvalidSyntheticTrainingWindow)
	require.NoDirExists(t, refusedDir)
}

func TestBuildSyntheticDirDoesNotSupplyHarnessEvidence(t *testing.T) {
	e := setup(t)
	writeConfig(t, e,
		"screen_lines: "+screenLines,
		"mask: "+e.mask,
		"readiness:\n    harnesses_min: 3\n    harness_train_min: 1",
	)

	trainSessions := map[protocol.OpaqueID]bool{}
	labels := map[protocol.OpaqueID]dataset.Label{}
	for _, harness := range []protocol.Harness{protocol.HarnessClaudeCode, protocol.HarnessOpencode} {
		for i := range 2 {
			id := protocol.OpaqueID(fmt.Sprintf("sess-harness-%s-train-%d", harness, i))
			writeSessionAs(t, e.dataDir, string(id), harness, "1.0.0 (harness evidence)", []chunk{
				{ms: 100, output: "working\r\n"},
				{ms: 200, draft: draftClosed},
				{ms: 2100, output: "done\r\n"},
			})
			trainSessions[id] = true
			labels[id] = dataset.LabelD
		}
		id := protocol.OpaqueID(fmt.Sprintf("sess-harness-%s-test", harness))
		writeSessionAs(t, e.dataDir, string(id), harness, "1.0.0 (harness evidence)", []chunk{
			{ms: 100, output: "ready\r\n"},
			{ms: 200, draft: draftClosed},
			{ms: 2100, output: "idle\r\n"},
		})
		labels[id] = dataset.LabelA
	}
	annotateSessions(t, e, labels)

	ids, err := session.ListSessions(e.dataDir)
	require.NoError(t, err)
	var split strings.Builder
	for _, id := range ids {
		assignment := dataset.SplitTest
		if trainSessions[protocol.OpaqueID(id)] {
			assignment = dataset.SplitTrain
		}
		fmt.Fprintf(&split, `{"session":%q,"split":%q}`+"\n", id, assignment)
	}
	splitFile := filepath.Join(e.work, "harness-split.jsonl")
	require.NoError(t, os.WriteFile(splitFile, []byte(split.String()), 0o600))

	baseDir := filepath.Join(e.work, "harness-base")
	_, err = buildCmd(t, flagOut, baseDir, flagAlign, e.alignCfg, flagSplitFile, splitFile)
	require.NoError(t, err)
	base := readBuildManifest(t, baseDir)
	wantLabelHarnesses := dataset.Fault{
		Rule: dataset.RuleLabelHarnesses, Subject: "D", Split: dataset.SplitTrain, Have: 2, Want: 3,
	}
	wantHarnessCount := dataset.Fault{
		Rule: dataset.RuleHarnesses, Subject: "harnesses", Split: dataset.SplitTrain, Have: 2, Want: 3,
	}
	require.Contains(t, base.Readiness.Faults, wantLabelHarnesses)
	require.Contains(t, base.Readiness.Faults, wantHarnessCount)

	var template dataset.Window
	for _, w := range readWindows(t, baseDir) {
		if w.Split.Split == dataset.SplitTrain && w.Target.Label == dataset.LabelD {
			template = w
			break
		}
	}
	require.NotEmpty(t, template.WindowID)
	synthetic := syntheticWindow(template, 90)
	synthetic.Input.Harness = protocol.HarnessAntigravity
	synthetic.Provenance.HarnessVersion = "1.0.0 (synthetic antigravity)"
	syntheticDir := filepath.Join(e.work, "harness-synthetic")
	writeWindow(t, syntheticDir, synthetic)

	combinedDir := filepath.Join(e.work, "harness-combined")
	_, err = buildCmd(t, flagOut, combinedDir, flagAlign, e.alignCfg, flagSplitFile, splitFile, flagSyntheticDir, syntheticDir)
	require.NoError(t, err)
	combined := readBuildManifest(t, combinedDir)
	require.Equal(t, base.Counts.BySplit.Train+1, combined.Counts.BySplit.Train,
		"synthetic windows still count toward the train total")
	require.Equal(t, base.Counts.ByLabel.D+1, combined.Counts.ByLabel.D,
		"synthetic windows still count toward the label total")
	require.Contains(t, combined.Readiness.Faults, wantLabelHarnesses,
		"synthetic Antigravity is not a third observed D harness")
	require.Contains(t, combined.Readiness.Faults, wantHarnessCount,
		"synthetic Antigravity cannot satisfy the per-harness train minimum")
	require.NotContains(t, combined.Readiness.Faults, dataset.Fault{
		Rule: dataset.RuleHarnessInTest, Subject: string(protocol.HarnessAntigravity),
		Split: dataset.SplitTest, Have: 0, Want: 1,
	}, "a synthetic-only train harness creates no test-coverage obligation")
}

func TestBuildKeepsHumanInputAndSecretsOut(t *testing.T) {
	e := setup(t)
	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg)
	require.NoError(t, err, "N and the mask come from config.yaml")

	files := treeOf(t, out)
	require.Contains(t, files, dataset.ManifestFile)
	all := strings.Join(mapValues(files), "\n")
	for _, never := range []string{canaryOpen, canaryPaste, canaryLong, canaryHidden, e.secret, maskedName, "acmebank"} {
		require.NotContains(t, all, never, "no file of the dataset holds it")
	}
	require.Contains(t, all, closedPrompt, "a closed draft keeps the prompt row")
	require.Contains(t, all, hiddenClosedPrompt, "with the draft closed, a hidden cursor does not matter")
	require.Contains(t, all, screenproj.MaskMarker)

	ws := readWindows(t, out)
	require.Len(t, ws, 27, "29 windows of 15 sessions, one unlabeled, one with a hidden cursor and an open draft")
	// With the draft not closed, no row of the screen is kept.
	open := 0
	for _, w := range ws {
		switch w.Provenance.SessionID {
		case "sess-open", "sess-paste", "sess-longpaste":
			open++
			require.Empty(t, strings.Join(w.Input.ScreenLines, ""), "session %s", w.Provenance.SessionID)
		}
	}
	require.Equal(t, 6, open)
	splits := map[protocol.OpaqueID]dataset.Split{}
	for _, w := range ws {
		require.Equal(t, harnessVer, w.Provenance.HarnessVersion)
		require.Equal(t, dataset.LabelSourceManual, w.Target.Source)
		require.Equal(t, screenproj.HumanInputPolicyV1, w.Policies.HumanInputPolicyVersion)
		require.True(t, strings.HasPrefix(w.Policies.DomainMaskPolicyVersion, "domain-mask-v1:"))
		if s, ok := splits[w.Provenance.SessionID]; ok {
			require.Equal(t, s, w.Split.Split, "one session, one split")
		}
		splits[w.Provenance.SessionID] = w.Split.Split
	}

	// The manifest says how align labeled the windows: the fixture's
	// margin_ms 0, and the digest of its align config.
	raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	alignBytes, err := os.ReadFile(e.alignCfg)
	require.NoError(t, err)
	alignDigest := protocol.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(alignBytes)))
	require.Equal(t, &dataset.AlignPolicy{MarginMS: 0, ConfigDigest: &alignDigest}, m.Align)
	require.Contains(t, stdout, "align margin 0 ms, config "+string(alignDigest))

	// Numbers only on stdout.
	require.Contains(t, stdout, "27 windows")
	require.Contains(t, stdout, "discarded: margin 0, conflict 0, unlabeled 1, weak_only 0, weak_not_train 0, weak_blank 0, no_output_yet 0, no_cursor 0, cursor_hidden 1; sessions skipped 0")
	require.Contains(t, stdout, "by harness: codex 27")
	require.Contains(t, stdout, "readiness: failed")
	for _, never := range []string{canaryOpen, canaryPaste, canaryLong, canaryHidden, e.secret, maskedName, closedPrompt, "build ok"} {
		require.NotContains(t, stdout, never)
	}

	// The same sources, the same bytes, with flags that repeat
	// config.yaml.
	again := filepath.Join(e.work, "ds-again")
	_, err = buildCmd(t, flagOut, again, flagScreenLines, screenLines, flagMask, e.mask, flagAlign, e.alignCfg)
	require.NoError(t, err)
	require.Equal(t, files, treeOf(t, again))

	// Variant B, ml.mask: none: the name stays and the version says
	// none.
	writeConfig(t, e, "screen_lines: "+screenLines, maskNone)
	unmasked := filepath.Join(e.work, "ds-b")
	_, err = buildCmd(t, flagOut, unmasked, flagNoMask, flagAlign, e.alignCfg)
	require.NoError(t, err)
	b := strings.Join(mapValues(treeOf(t, unmasked)), "\n")
	require.Contains(t, b, maskedName)
	require.NotContains(t, b, canaryOpen)
	require.NotContains(t, b, canaryLong)
	require.NotContains(t, b, canaryHidden)
	require.NotContains(t, b, e.secret)
	for _, w := range readWindows(t, unmasked) {
		require.Equal(t, screenproj.DomainMaskNone, w.Policies.DomainMaskPolicyVersion)
	}
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func TestBuildRefusesBeforeWriting(t *testing.T) {
	e := setup(t)
	out := filepath.Join(e.work, "ds")
	existing := filepath.Join(e.work, "existing")
	require.NoError(t, os.Mkdir(existing, 0o700))
	other := filepath.Join(e.work, "other-mask.txt")
	require.NoError(t, os.WriteFile(other, []byte("acmebank\n"), 0o600))
	refused := func(name string, args ...string) {
		t.Helper()
		_, err := buildCmd(t, append(args, flagAlign, e.alignCfg)...)
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec, name)
		require.Equal(t, exitcode.Usage, ec.Code, name)
		require.NoDirExists(t, out, name)
	}
	for name, args := range map[string][]string{
		"no out":       {flagNoMask},
		"both masks":   {flagOut, out, flagNoMask, flagMask, e.mask},
		"existing dir": {flagOut, existing},
		// A flag that says otherwise than config.yaml.
		"other screen lines": {flagOut, out, flagScreenLines, "23"},
		"zero screen lines":  {flagOut, out, flagScreenLines, "0"},
		"other mask":         {flagOut, out, flagMask, other},
		"no mask":            {flagOut, out, flagNoMask},
		// The live form takes the draft from the guard.
		"live form and scripted input": {flagOut, out, flagLiveForm, "--scripted-input", flagSplitFile, other},
		"live v2 and scripted input":   {flagOut, out, flagLiveFormV2, "--scripted-input", flagSplitFile, other},
		"both live forms":              {flagOut, out, flagLiveForm, flagLiveFormV2},
		"live v2 wrong spacing":        {flagOut, out, flagLiveFormV2, "--every", "2s"},
		"live v2 needs split":          {flagOut, out, flagLiveFormV2},
		"live v2 wrong margin":         {flagOut, out, flagLiveFormV2, flagSplitFile, other},
	} {
		refused(name, args...)
	}
	// config.yaml without a projection, or with an invalid one.
	badMask := filepath.Join(e.work, "empty-mask.txt")
	require.NoError(t, os.WriteFile(badMask, []byte("# nothing\n"), 0o600))
	for name, ml := range map[string][]string{
		"no config keys":        nil,
		"no screen lines":       {maskNone},
		"no mask":               {"screen_lines: " + screenLines},
		"41 screen lines":       {"screen_lines: 41", maskNone},
		"a relative mask":       {"screen_lines: " + screenLines, "mask: mask.txt"},
		"a list with no name":   {"screen_lines: " + screenLines, "mask: " + badMask},
		"none but a mask given": {"screen_lines: " + screenLines, maskNone},
	} {
		writeConfig(t, e, ml...)
		if name == "none but a mask given" {
			refused(name, flagOut, out, flagMask, e.mask)
			continue
		}
		refused(name, flagOut, out)
	}
	writeConfig(t, e, "screen_lines: "+screenLines, "mask: "+e.mask)
	ents, err := os.ReadDir(existing)
	require.NoError(t, err)
	require.Empty(t, ents, "an existing directory is not written into")
	ents, err = os.ReadDir(e.work)
	require.NoError(t, err)
	for _, ent := range ents {
		require.False(t, strings.HasPrefix(ent.Name(), ".dataset-"), "no temporary directory")
	}

	// A data dir with nothing to build fails and writes nothing.
	t.Setenv("XDG_DATA_HOME", filepath.Join(e.work, "empty-data"))
	_, err = buildCmd(t, flagOut, out, flagAlign, e.alignCfg)
	require.Error(t, err)
	require.NoDirExists(t, out)
}

// TestBuildSplitFile builds under a split file: each session lands in
// the split the file names, train included, and the manifest keeps the
// file's digest. A session missing from the file, or a malformed file,
// fails before anything is written.
func TestBuildSplitFile(t *testing.T) {
	e := setup(t)
	ids, err := session.ListSessions(e.dataDir)
	require.NoError(t, err)
	want := map[protocol.OpaqueID]dataset.Split{}
	var file strings.Builder
	for i, id := range ids {
		split := []dataset.Split{dataset.SplitTrain, dataset.SplitValidation, dataset.SplitTest}[i%3]
		want[protocol.OpaqueID(id)] = split
		fmt.Fprintf(&file, `{"session":%q,"split":%q}`+"\n", id, split)
	}
	// A session the data dir does not hold is no fault.
	file.WriteString(`{"session":"sess-gone","split":"test"}` + "\n")
	lines := file.String()
	splitFile := filepath.Join(e.work, "split.jsonl")
	require.NoError(t, os.WriteFile(splitFile, []byte(lines), 0o600))

	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg, flagSplitFile, splitFile)
	require.NoError(t, err)
	ws := readWindows(t, out)
	require.Len(t, ws, 27)
	for _, w := range ws {
		require.Equal(t, want[w.Provenance.SessionID], w.Split.Split, "session %s", w.Provenance.SessionID)
	}
	raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	digest := protocol.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(lines))))
	require.NotNil(t, m.SplitFile)
	require.Equal(t, digest, *m.SplitFile)
	require.Positive(t, m.Counts.BySplit.Train)
	require.Positive(t, m.Counts.BySplit.Validation)
	require.Positive(t, m.Counts.BySplit.Test)
	require.Contains(t, stdout, "split file "+string(digest))

	// The same file, the same bytes.
	again := filepath.Join(e.work, "ds-again")
	_, err = buildCmd(t, flagOut, again, flagAlign, e.alignCfg, flagSplitFile, splitFile)
	require.NoError(t, err)
	require.Equal(t, treeOf(t, out), treeOf(t, again))

	// Without the flag, split-v1 assigns the splits and the manifest
	// names no file.
	v1 := filepath.Join(e.work, "ds-v1")
	_, err = buildCmd(t, flagOut, v1, flagAlign, e.alignCfg)
	require.NoError(t, err)
	raw, err = os.ReadFile(filepath.Join(v1, dataset.ManifestFile))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"split_file":null`)

	refused := filepath.Join(e.work, "ds-refused")
	build := func(name, body string) error {
		t.Helper()
		path := filepath.Join(e.work, "bad-split.jsonl")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		_, err := buildCmd(t, flagOut, refused, flagAlign, e.alignCfg, flagSplitFile, path)
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec, name)
		require.Equal(t, exitcode.Usage, ec.Code, name)
		require.NoDirExists(t, refused, name)
		return err
	}
	_, rest, _ := strings.Cut(lines, "\n")
	err = build("a session missing", rest)
	require.ErrorContains(t, err, "session "+ids[0]+" is not in the split file")
	first := fmt.Sprintf(`{"session":%q,"split":"test"}`, ids[0])
	for name, body := range map[string]string{
		"an unknown split": strings.Replace(lines, `"split":"validation"`, `"split":"dev"`, 1),
		"a session twice":  lines + first + "\n",
		"an unknown field": strings.Replace(lines, `"split":"test"}`, `"split":"test","label":"A"}`, 1),
		"not JSON":         lines + "sess-open validation\n",
		"two objects":      first + first + "\n" + rest,
		"no split":         strings.Replace(lines, `,"split":"test"`, "", 1),
		"a bad session id": lines + `{"session":"a b","split":"test"}` + "\n",
		"empty":            "\n",
	} {
		_ = build(name, body)
	}
	_, err = buildCmd(t, flagOut, refused, flagAlign, e.alignCfg, flagSplitFile, filepath.Join(e.work, "no-such-file"))
	require.ErrorContains(t, err, "split file no-such-file:")
	require.NotContains(t, err.Error(), e.work, "the path stays out of the error")
	require.NoDirExists(t, refused)
}

// scriptText is what a script typed on a session's prompt line.
const scriptText = "SCRIPT-TEXT-5r1w"

// TestBuildScriptedInput builds a session a script typed into, its draft
// open and its text on screen. With --scripted-input the draft counts as
// closed: the window goes through the projection as any other, redacted,
// masked and cut to N rows, its text kept. Without the flag it is blank,
// as today. The flag needs --split-file, and every session its script's
// timeline: a session without one fails the build before anything is
// written.
func TestBuildScriptedInput(t *testing.T) {
	e := setup(t)
	writeSession(t, e.dataDir, "sess-scripted", []chunk{
		{ms: 100, output: "agent ready\r\n" + maskedName + " portal, token " + e.secret + "\r\n> " + scriptText + " "},
		{ms: 200, draft: draftOpen},
		{ms: 2100, output: " "},
	})
	ids, err := session.ListSessions(e.dataDir)
	require.NoError(t, err)
	var split, timeline strings.Builder
	for i, id := range ids {
		fmt.Fprintf(&split, `{"session":%q,"split":%q}`+"\n", id, []dataset.Split{dataset.SplitValidation, dataset.SplitTest}[i%2])
		fmt.Fprintf(&timeline, `{"session":%q,"at":%q,"label":"IDLE_AT_PROMPT"}`+"\n"+`{"session":%q,"at":%q,"label":"NONE"}`+"\n",
			id, t0.Format(time.RFC3339Nano), id, t0.Add(time.Hour).Format(time.RFC3339Nano))
	}
	splitFile := filepath.Join(e.work, "split.jsonl")
	require.NoError(t, os.WriteFile(splitFile, []byte(split.String()), 0o600))
	timelineFile := filepath.Join(label.Dir(e.dataDir), label.TimelineFile)
	require.NoError(t, os.WriteFile(timelineFile, []byte(timeline.String()), 0o600))

	scriptedWindows := func(dir string) []dataset.Window {
		t.Helper()
		var out []dataset.Window
		for _, w := range readWindows(t, dir) {
			if w.Provenance.SessionID == "sess-scripted" {
				out = append(out, w)
			}
		}
		require.NotEmpty(t, out)
		return out
	}

	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg, flagSplitFile, splitFile, "--scripted-input")
	require.NoError(t, err)
	require.Contains(t, stdout, "scripted input: no human keyboard, every draft counted closed")
	require.Contains(t, stdout, "cursor_hidden 0;", "no window is left out for its cursor")
	for _, w := range scriptedWindows(out) {
		screen := strings.Join(w.Input.ScreenLines, "\n")
		require.Contains(t, screen, scriptText, "the script's text stays on screen")
		require.Contains(t, screen, screenproj.MaskMarker, "names are masked")
		require.NotContains(t, screen, maskedName)
		require.NotContains(t, screen, e.secret, "secrets are redacted")
		require.LessOrEqual(t, len(w.Input.ScreenLines), 24, "the last N rows")
		require.Equal(t, dataset.LabelSourceScript, w.Target.Source)
	}
	raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	require.True(t, m.ScriptedInput)

	// Without the flag the same window is blank, as today.
	plain := filepath.Join(e.work, "ds-plain")
	_, err = buildCmd(t, flagOut, plain, flagAlign, e.alignCfg, flagSplitFile, splitFile)
	require.NoError(t, err)
	for _, w := range scriptedWindows(plain) {
		require.Empty(t, w.Input.ScreenLines, "an open draft blanks the screen")
	}

	// The flag needs --split-file, and a timeline for every session.
	refused := filepath.Join(e.work, "ds-refused")
	var ec *deps.ExitError
	_, err = buildCmd(t, flagOut, refused, flagAlign, e.alignCfg, "--scripted-input")
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Usage, ec.Code)
	require.NoDirExists(t, refused)
	var rest strings.Builder
	for _, line := range strings.SplitAfter(timeline.String(), "\n") {
		if !strings.Contains(line, `"sess-paste"`) {
			rest.WriteString(line)
		}
	}
	require.NoError(t, os.WriteFile(timelineFile, []byte(rest.String()), 0o600))
	_, err = buildCmd(t, flagOut, refused, flagAlign, e.alignCfg, flagSplitFile, splitFile, "--scripted-input")
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Usage, ec.Code)
	require.ErrorContains(t, err, "session sess-paste has no timeline")
	require.NoDirExists(t, refused)
}

// openCodeBox draws a synthetic OpenCode screen, its measured input box
// empty with the cursor in it.
var openCodeBox = "\x1b[H\x1b[2J\x1b[16;1Hagent output" +
	"\x1b[17;3H┃\x1b[18;3H┃  \x1b[38;5;8;48;5;234mAsk anything\x1b[0m\x1b[19;3H┃" +
	"\x1b[20;3H┃  Build · modelo\x1b[21;3H╹" + strings.Repeat("▀", 75) +
	"\x1b[18;6H\x1b[0m"

// TestBuildLiveForm builds in live form: an OpenCode session keeps the
// windows whose screen the live Analyst gets, its input box masked, with
// the ids they have in any build; the one under an open draft and every
// window of the fixture's screens, which draw no measured box, are left
// out and counted by harness, label and reason. The manifest says so.
func TestBuildLiveForm(t *testing.T) {
	e := setup(t)
	writeSessionAs(t, e.dataDir, "sess-live", protocol.HarnessOpencode, "1.18.29", []chunk{
		{ms: 100, output: openCodeBox},
		{ms: 1500, draft: draftOpen},
		{ms: 2500, draft: draftClosed},
		{ms: 2600, output: openCodeBox},
	})
	timeline := fmt.Sprintf(`{"session":"sess-live","at":%q,"label":"IDLE_AT_PROMPT"}`+"\n"+
		`{"session":"sess-live","at":%q,"label":"NONE"}`+"\n",
		t0.Format(time.RFC3339Nano), t0.Add(time.Hour).Format(time.RFC3339Nano))
	require.NoError(t, os.WriteFile(filepath.Join(label.Dir(e.dataDir), label.TimelineFile), []byte(timeline), 0o600))

	plain := filepath.Join(e.work, "ds-plain")
	_, err := buildCmd(t, flagOut, plain, flagAlign, e.alignCfg)
	require.NoError(t, err)
	ids := map[protocol.ULID]bool{}
	for _, w := range readWindows(t, plain) {
		if w.Provenance.SessionID == "sess-live" {
			ids[w.WindowID] = true
		}
	}
	require.Len(t, ids, 3)

	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg, flagLiveForm)
	require.NoError(t, err)
	require.Contains(t, stdout, "screen form live-input-v1\n")
	require.Contains(t, stdout, "live form opencode A: kept 2, withheld 1 (draft_not_closed 1)\n")
	require.Regexp(t, `withheld in live form: [1-9][0-9]*\n`, stdout)
	require.Regexp(t, `live form codex [ABC]: kept 0, withheld [1-9]`, stdout)
	ws := readWindows(t, out)
	require.Len(t, ws, 2)
	for _, w := range ws {
		require.True(t, ids[w.WindowID], "a kept window keeps its id")
		require.Equal(t, protocol.HarnessOpencode, w.Input.Harness)
		screen := strings.Join(w.Input.ScreenLines, "\n")
		require.Contains(t, screen, "agent output")
		require.Contains(t, screen, "[input]")
		require.NotContains(t, screen, "Build · modelo", "the input box is masked")
	}
	raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, dataset.ScreenFormLive, m.ScreenForm)
	require.False(t, m.ScriptedInput)

	raw, err = os.ReadFile(filepath.Join(plain, dataset.ManifestFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "screen_form", "a build without the flag is as before")
}

// TestBuildLabelsFromTimeline builds with a scripted session's
// timeline: the one window no annotation labels gets the timeline's
// state, as a real label from the script; the annotated ones stay
// manual. A timeline with an unreadable line fails the build.
func TestBuildLabelsFromTimeline(t *testing.T) {
	e := setup(t)
	lines := fmt.Sprintf(`{"session":"sess-closed","at":%q,"label":"IDLE_AT_PROMPT"}`+"\n"+
		`{"session":"sess-closed","at":%q,"label":"NONE"}`+"\n",
		t0.Format(time.RFC3339Nano), t0.Add(time.Minute).Format(time.RFC3339Nano))
	timeline := filepath.Join(label.Dir(e.dataDir), label.TimelineFile)
	require.NoError(t, os.WriteFile(timeline, []byte(lines), 0o600))

	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg)
	require.NoError(t, err)
	require.Contains(t, stdout, "28 windows")
	require.Contains(t, stdout, "unlabeled 0,")
	script := 0
	for _, w := range readWindows(t, out) {
		if w.Target.Source != dataset.LabelSourceScript {
			require.Equal(t, dataset.LabelSourceManual, w.Target.Source)
			continue
		}
		script++
		require.Equal(t, protocol.OpaqueID("sess-closed"), w.Provenance.SessionID)
		require.Equal(t, dataset.LabelA, w.Target.Label)
		require.True(t, w.Target.Source.Real())
	}
	require.Equal(t, 1, script)

	require.NoError(t, os.WriteFile(timeline, []byte(lines+`{"session":"sess-closed","label":"NONE"}`+"\n"), 0o600))
	torn := filepath.Join(e.work, "ds-torn")
	_, err = buildCmd(t, flagOut, torn, flagAlign, e.alignCfg)
	require.ErrorContains(t, err, label.TimelineFile+": 1 unreadable lines")
	require.NoDirExists(t, torn)
}

// sliceSource yields recordings already in memory.
type sliceSource struct{ recs []dataset.Recording }

func (s *sliceSource) Next(context.Context) (dataset.Recording, error) {
	if len(s.recs) == 0 {
		return dataset.Recording{}, io.EOF
	}
	r := s.recs[0]
	s.recs = s.recs[1:]
	return r, nil
}

func TestFinalOnlyDropsWindowsWithoutCursor(t *testing.T) {
	t.Parallel()
	ids := []protocol.ULID{
		"01JB0000000000000000000001", "01JB0000000000000000000002", "01JB0000000000000000000003",
		"01JB0000000000000000000004", "01JB0000000000000000000005",
	}
	shown := screenproj.Cursor{Known: true, Visible: true}
	hidden := screenproj.Cursor{Row: 3, Known: true}
	d := &discards{}
	src := &finalOnly{
		src: &sliceSource{recs: []dataset.Recording{
			{WindowID: ids[0], Draft: dataset.DraftOpen},                   // no cursor: dropped
			{WindowID: ids[1], Draft: dataset.DraftClosed},                 // closed: no cursor needed
			{WindowID: ids[2], Draft: dataset.DraftUnknown, Cursor: shown}, // kept
			{WindowID: ids[3], Draft: dataset.DraftClosed},                 // no final label
			{WindowID: ids[4], Draft: dataset.DraftOpen, Cursor: hidden},   // hidden cursor: dropped
		}},
		final: map[protocol.ULID]label.AlignedLabel{
			ids[0]: {Label: dataset.LabelA, Source: label.SourceManual},
			ids[1]: {Label: dataset.LabelB, Source: label.SourceHook},
			ids[2]: {Label: dataset.LabelC, Source: label.SourceManual},
			ids[4]: {Label: dataset.LabelD, Source: label.SourceManual},
		},
		d: d,
	}
	var got []dataset.Recording
	for {
		r, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 2)
	require.Equal(t, ids[1], got[0].WindowID)
	require.Equal(t, dataset.LabelB, got[0].Label)
	require.Equal(t, dataset.LabelSourceHook, got[0].LabelSource)
	require.Equal(t, ids[2], got[1].WindowID)
	require.Equal(t, 1, d.noCursor)
	require.Equal(t, 1, d.cursorHidden, "counted apart from no cursor")
}

func groupForSplit(t *testing.T, want dataset.Split) protocol.Digest {
	t.Helper()
	for i := range 10_000 {
		sum := sha256.Sum256([]byte(fmt.Sprintf("split-fixture-%d", i)))
		d := protocol.Digest(fmt.Sprintf("sha256:%x", sum))
		got, err := (dataset.SplitV1{}).Split(d)
		require.NoError(t, err)
		if got == want {
			return d
		}
	}
	t.Fatalf("no fixture for split %s", want)
	return ""
}

func TestFinalOnlyKeepsWeakInTrainOnly(t *testing.T) {
	t.Parallel()
	ids := []protocol.ULID{
		"01JB0000000000000000000011", "01JB0000000000000000000012", "01JB0000000000000000000013",
	}
	recs := []dataset.Recording{
		{WindowID: ids[0], GroupDigest: groupForSplit(t, dataset.SplitTrain), Draft: dataset.DraftClosed},
		{WindowID: ids[1], GroupDigest: groupForSplit(t, dataset.SplitValidation), Draft: dataset.DraftClosed},
		{WindowID: ids[2], GroupDigest: groupForSplit(t, dataset.SplitTest), Draft: dataset.DraftClosed},
	}
	final := map[protocol.ULID]label.AlignedLabel{}
	for _, id := range ids {
		final[id] = label.AlignedLabel{
			WindowID: id, Label: dataset.LabelA, Source: label.SourceWeak,
			WeakOrigin: label.WeakOriginImport, Doubt: label.DoubtWeakOnly,
		}
	}
	d := &discards{}
	src := &finalOnly{src: &sliceSource{recs: recs}, final: final, keepWeak: label.WeakKeepTrain, splitter: dataset.SplitV1{}, d: d}
	got, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, ids[0], got.WindowID)
	require.Equal(t, recs[0].GroupDigest, got.GroupDigest, "weak opt-in never moves a group")
	require.Equal(t, dataset.LabelSourceWeak, got.LabelSource)
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, d.weakNotTrain, "validation and test stay free of weak labels")

	d = &discards{}
	src = &finalOnly{src: &sliceSource{recs: recs[:1]}, final: final, keepWeak: label.WeakKeepOff, splitter: dataset.SplitV1{}, d: d}
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, d.weakNotTrain, "the default keeps no weak label")

	// Under a split file, the file's train decides: the group split-v1
	// puts in train is left out where the file says test, and one the
	// file puts in train is kept.
	d = &discards{}
	table := dataset.SplitTable{}
	for i, r := range recs {
		table[r.GroupDigest] = []dataset.Split{dataset.SplitTest, dataset.SplitValidation, dataset.SplitTrain}[i]
	}
	src = &finalOnly{src: &sliceSource{recs: recs}, final: final, keepWeak: label.WeakKeepTrain, splitter: table, d: d}
	got, err = src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, ids[2], got.WindowID)
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, d.weakNotTrain)
}

type passScreenRedactor struct{}

func (passScreenRedactor) RedactScreen(s redact.Screen) (redact.ScreenResult, error) {
	return redact.ScreenResult{Rows: s.Rows, PolicyVersion: "test-v1"}, nil
}

func TestFinalOnlyRejectsWeakGOnBlankProjection(t *testing.T) {
	t.Parallel()
	ids := []protocol.ULID{
		"01JB0000000000000000000031", "01JB0000000000000000000032", "01JB0000000000000000000033",
	}
	group := groupForSplit(t, dataset.SplitTrain)
	shown := screenproj.Cursor{Known: true, Visible: true}
	recs := []dataset.Recording{
		{WindowID: ids[0], GroupDigest: group, Draft: dataset.DraftClosed, Screen: redact.Screen{Rows: []string{"   "}, Cols: 80}},
		{WindowID: ids[1], GroupDigest: group, Draft: dataset.DraftOpen, Cursor: shown, Screen: redact.Screen{Rows: []string{"human draft"}, Cols: 80}},
		{WindowID: ids[2], GroupDigest: group, Draft: dataset.DraftClosed, Screen: redact.Screen{Rows: []string{"state is unclear"}, Cols: 80}},
	}
	final := map[protocol.ULID]label.AlignedLabel{}
	for _, id := range ids {
		final[id] = label.AlignedLabel{
			WindowID: id, Label: dataset.LabelG, Source: label.SourceWeak,
			WeakOrigin: label.WeakOriginImport, Doubt: label.DoubtWeakOnly,
		}
	}
	d := &discards{}
	src := &finalOnly{
		src: &sliceSource{recs: recs}, final: final, keepWeak: label.WeakKeepTrain, splitter: dataset.SplitV1{}, d: d,
		projection: screenproj.Policies{Lines: 10, Redactor: passScreenRedactor{}, Mask: screenproj.NoMask()},
	}
	got, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, ids[2], got.WindowID)
	require.Equal(t, dataset.LabelG, got.Label)
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, d.weakBlank, "both an empty screen and human-input blanking reject weak G")
}

func TestFinalLabelsWeakDefaultAndConflict(t *testing.T) {
	t.Parallel()
	weakID := protocol.ULID("01JB0000000000000000000021")
	conflictID := protocol.ULID("01JB0000000000000000000022")
	strongID := protocol.ULID("01JB0000000000000000000023")
	activityID := protocol.ULID("01JB0000000000000000000024")
	unmarkedID := protocol.ULID("01JB0000000000000000000025")
	aligned := []label.AlignedLabel{
		{WindowID: weakID, Label: dataset.LabelG, Source: label.SourceWeak, WeakOrigin: label.WeakOriginImport, Doubt: label.DoubtWeakOnly},
		{WindowID: conflictID, Label: dataset.LabelA, Source: label.SourceHook, Doubt: label.DoubtConflict},
		{WindowID: strongID, Label: dataset.LabelB, Source: label.SourceManual},
		{WindowID: activityID, Label: dataset.LabelC, Source: label.SourceWeak, WeakOrigin: label.WeakOriginActivity, Doubt: label.DoubtWeakOnly},
		{WindowID: unmarkedID, Label: dataset.LabelA, Source: label.SourceWeak, WeakOrigin: label.WeakOriginImport},
	}
	d := &discards{}
	got := finalLabels(aligned, label.WeakKeepOff, d)
	require.Equal(t, map[protocol.ULID]label.AlignedLabel{strongID: aligned[2]}, got)
	require.Equal(t, 3, d.weakOnly)
	require.Equal(t, 1, d.conflict)

	d = &discards{}
	got = finalLabels(aligned, label.WeakKeepTrain, d)
	require.Contains(t, got, weakID, "an audited import defers to the existing split")
	require.NotContains(t, got, activityID, "activity-derived weak labels stay diagnostic")
	require.NotContains(t, got, unmarkedID, "weak eligibility also requires Align's explicit weak-only result")
	require.NotContains(t, got, conflictID, "opt-in never admits a conflict")
	require.Equal(t, 2, d.weakOnly)
	require.Equal(t, 1, d.conflict)
}

func TestPermissionDialogDTruthUsesOnlyWinningRealIntervals(t *testing.T) {
	t.Parallel()
	require.True(t, permissionDialogDTruth(label.AlignedLabel{
		Label: dataset.LabelD, Source: label.SourceHook, HookLabel: dataset.LabelD,
	}))
	require.True(t, permissionDialogDTruth(label.AlignedLabel{
		Label: dataset.LabelD, Source: label.SourceScript, ScriptLabel: dataset.LabelD,
	}))
	for name, aligned := range map[string]label.AlignedLabel{
		"manual":      {Label: dataset.LabelD, Source: label.SourceManual},
		"weak":        {Label: dataset.LabelD, Source: label.SourceWeak},
		"hook absent": {Label: dataset.LabelD, Source: label.SourceHook},
		"wrong label": {Label: dataset.LabelE, Source: label.SourceHook, HookLabel: dataset.LabelD},
	} {
		require.False(t, permissionDialogDTruth(aligned), name)
	}
}

// TestBuildThenPredictThenEval runs the whole local path: build the
// dataset, run eval predict over it with a fake Glimmer, and score it.
func TestBuildThenPredictThenEval(t *testing.T) {
	e := setup(t)
	out := filepath.Join(e.work, "ds")
	_, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Positive(t, m.Counts.BySplit.Validation+m.Counts.BySplit.Test, "the fixture has windows to score")

	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct {
			Payload json.RawMessage `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if bytes.Contains(body.Payload, []byte("CANARY-")) {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		_, _ = w.Write([]byte(`{"label":"IDLE_AT_PROMPT","probabilities":{"IDLE_AT_PROMPT":0.75,"THINKING_STREAMING":0.25},"evidence":["prompt"]}`))
	}))
	defer srv.Close()
	t.Setenv("GIFFORD_BUILD_TEST_URL", srv.URL)
	t.Setenv("GIFFORD_BUILD_TEST_TOKEN", "test-token")
	gw := filepath.Join(e.work, "gateway.yaml")
	require.NoError(t, os.WriteFile(gw, []byte(`
endpoint:
  url_env: GIFFORD_BUILD_TEST_URL
  token_env: GIFFORD_BUILD_TEST_TOKEN
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`), 0o600))
	preds := filepath.Join(e.work, "base.jsonl")
	manifest := filepath.Join(out, dataset.ManifestFile)
	evalCmd := func(args ...string) error {
		cmd := clieval.NewCmd(&deps.Deps{})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(args)
		return cmd.Execute()
	}
	require.NoError(t, evalCmd("predict", "--profile", "base", "--manifest", manifest,
		"--windows", filepath.Join(out, dataset.WindowsDir), "--gateway", gw, flagOut, preds, "--concurrency", "1"))
	b, err := os.ReadFile(preds)
	require.NoError(t, err)
	require.Equal(t, m.Counts.BySplit.Validation+m.Counts.BySplit.Test, calls.Load())
	ps, err := eval.ReadPredictions(bytes.NewReader(b), eval.ProfileBase)
	require.NoError(t, err)
	for _, p := range ps {
		require.False(t, p.Invalid, "no payload carried a canary")
		require.Equal(t, harnessVer, p.HarnessVersion)
	}
	report := filepath.Join(e.work, "RESULTS.md")
	require.NoError(t, evalCmd("--profile", "base="+preds, "--manifest", manifest, flagOut, report))
	md, err := os.ReadFile(report)
	require.NoError(t, err)
	require.Contains(t, string(md), `codex 0\.50\.0 \(Codex\)`, "the measured version, escaped")
}
