package tmuxhelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testBootID         = "12345678-1234-1234-1234-123456789abc"
	testBankID         = "example-bank"
	testNoticeFull     = ": [GIFFORD] full notice"
	testNoticeResponse = ": [GIFFORD] response notice"
	testNoticeShort    = ": [GIFFORD] short notice"
	testUID            = uint32(1001)
)

type platformCall struct {
	socket string
	argv   []string
	limit  int
}

type fakePlatform struct {
	uid           uint32
	manifest      []byte
	manifests     [][]byte
	manifestErr   error
	socketErr     error
	outputs       [][]byte
	codes         []ExitCode
	starts        map[int64]uint64
	bootID        string
	identityErr   error
	manifestLoads int
	socketChecks  int
	calls         []platformCall
}

func (p *fakePlatform) EffectiveUID() uint32 { return p.uid }

func (p *fakePlatform) LoadManifest() ([]byte, error) {
	p.manifestLoads++
	if p.manifestLoads <= len(p.manifests) {
		return slices.Clone(p.manifests[p.manifestLoads-1]), p.manifestErr
	}
	return slices.Clone(p.manifest), p.manifestErr
}

func (p *fakePlatform) ValidateSocket(uid uint32, socket string) error {
	p.socketChecks++
	if uid != testUID || socket != "/tmp/tmux-1001/agent" {
		return errors.New("wrong socket binding")
	}
	return p.socketErr
}

func (p *fakePlatform) RunTmux(
	_ context.Context,
	socket string,
	argv []string,
	limit int,
) ([]byte, ExitCode) {
	p.calls = append(p.calls, platformCall{socket: socket, argv: slices.Clone(argv), limit: limit})
	index := len(p.calls) - 1
	if index < len(p.codes) && p.codes[index] != ExitOK {
		return nil, p.codes[index]
	}
	if index < len(p.outputs) {
		return slices.Clone(p.outputs[index]), ExitOK
	}
	return nil, ExitOK
}

func (p *fakePlatform) ProcessStart(pid int64) (uint64, error) {
	if p.identityErr != nil {
		return 0, p.identityErr
	}
	return p.starts[pid], nil
}

func (p *fakePlatform) BootID() (string, error) {
	if p.identityErr != nil {
		return "", p.identityErr
	}
	return p.bootID, nil
}

func validRegistry() Registry {
	return Registry{
		Banks: map[string]Bank{
			testBankID: {
				Panes: map[string]PaneBinding{
					"%7": {BootID: testBootID, PanePID: 4242, PaneStart: 700},
					"%8": {BootID: testBootID, PanePID: 4243, PaneStart: 701},
				},
				Socket: "/tmp/tmux-1001/agent",
				UID:    testUID,
			},
		},
		NoticeFull: testNoticeFull, NoticeResponse: testNoticeResponse,
		NoticeShort: testNoticeShort, SchemaVersion: 1,
	}
}

func validManifest() []byte { return validRegistry().canonicalJSON() }

func manifestRevision(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func newFakePlatform() *fakePlatform {
	return &fakePlatform{
		uid: testUID, manifest: validManifest(),
		starts: map[int64]uint64{4242: 700, 4243: 701}, bootID: testBootID,
	}
}

func helperArgs(raw []byte, operation verb, pane ...string) []string {
	args := make([]string, 0, 3+len(pane))
	args = append(args, string(operation), manifestRevision(raw), testBankID)
	return append(args, pane...)
}

func executeFake(ctx context.Context, host *fakePlatform, args []string) (ExitCode, []byte) {
	return execute(ctx, host, host, host, args)
}

func TestEightVerbsGenerateOnlyFrozenTmuxArguments(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		verb       verb
		pane       []string
		outputs    [][]byte
		wantBody   []byte
		wantArgs   [][]string
		wantLimits []int
	}{
		{
			name: string(verbPanes), verb: verbPanes,
			outputs:    [][]byte{[]byte("%7\t4242\n%9\t9999\n")},
			wantBody:   []byte("%7\t4242\n%9\t9999\n"),
			wantArgs:   [][]string{{"list-panes", "-a", "-F", formatPanes}},
			wantLimits: []int{maxListBytes},
		},
		{
			name: string(verbClients), verb: verbClients,
			outputs:    [][]byte{[]byte("%7\t1730000000\n%9\t1730000001\n%8\t\n")},
			wantBody:   []byte("%7\t1730000000\n%9\t1730000001\n%8\t\n"),
			wantArgs:   [][]string{{"list-clients", "-F", formatClients}},
			wantLimits: []int{maxListBytes},
		},
		{
			name: string(verbObserve), verb: verbObserve, pane: []string{"%7"},
			outputs:    [][]byte{[]byte("%7\t4242\t120\t40\n")},
			wantBody:   []byte("%7\t4242\t120\t40\n"),
			wantArgs:   [][]string{{commandDisplayMessage, "-p", "-t", "%7", formatObserve}},
			wantLimits: []int{maxObserveBytes},
		},
		{
			name: string(verbCapture), verb: verbCapture, pane: []string{"%7"},
			outputs:  [][]byte{[]byte("%7\t4242\t120\t40\n"), {0, 1, 2, 0xff}},
			wantBody: []byte{0, 1, 2, 0xff},
			wantArgs: [][]string{
				{commandDisplayMessage, "-p", "-t", "%7", formatObserve},
				{"capture-pane", "-p", "-e", "-t", "%7"},
			},
			wantLimits: []int{maxObserveBytes, maxCaptureBytes},
		},
		{
			name: "notice full", verb: verbNoticeFull, pane: []string{"%7"},
			outputs: [][]byte{[]byte("%7\t4242\t120\t40\n"), nil},
			wantArgs: [][]string{
				{commandDisplayMessage, "-p", "-t", "%7", formatObserve},
				{commandSendKeys, "-l", "-t", "%7", testNoticeFull},
			},
			wantLimits: []int{maxObserveBytes, maxEffectBytes},
		},
		{
			name: "notice response", verb: verbNoticeResponse, pane: []string{"%7"},
			outputs: [][]byte{[]byte("%7\t4242\t120\t40\n"), nil},
			wantArgs: [][]string{
				{commandDisplayMessage, "-p", "-t", "%7", formatObserve},
				{commandSendKeys, "-l", "-t", "%7", testNoticeResponse},
			},
			wantLimits: []int{maxObserveBytes, maxEffectBytes},
		},
		{
			name: "notice short", verb: verbNoticeShort, pane: []string{"%7"},
			outputs: [][]byte{[]byte("%7\t4242\t120\t40\n"), nil},
			wantArgs: [][]string{
				{commandDisplayMessage, "-p", "-t", "%7", formatObserve},
				{commandSendKeys, "-l", "-t", "%7", testNoticeShort},
			},
			wantLimits: []int{maxObserveBytes, maxEffectBytes},
		},
		{
			name: string(verbEnter), verb: verbEnter, pane: []string{"%7"},
			outputs: [][]byte{[]byte("%7\t4242\t120\t40\n"), nil},
			wantArgs: [][]string{
				{commandDisplayMessage, "-p", "-t", "%7", formatObserve},
				{commandSendKeys, "-t", "%7", "Enter"},
			},
			wantLimits: []int{maxObserveBytes, maxEffectBytes},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			host := newFakePlatform()
			host.outputs = test.outputs
			code, body := executeFake(t.Context(), host, helperArgs(host.manifest, test.verb, test.pane...))
			require.Equal(t, ExitOK, code)
			require.Equal(t, test.wantBody, body)
			gotArgs := make([][]string, 0, len(host.calls))
			gotLimits := make([]int, 0, len(host.calls))
			for _, call := range host.calls {
				require.Equal(t, "/tmp/tmux-1001/agent", call.socket)
				gotArgs = append(gotArgs, call.argv)
				gotLimits = append(gotLimits, call.limit)
			}
			require.Equal(t, test.wantArgs, gotArgs)
			require.Equal(t, test.wantLimits, gotLimits)
			require.Equal(t, 1+len(host.calls), host.socketChecks)
			require.Equal(t, 1+len(host.calls), host.manifestLoads)
		})
	}
}

func TestGrammarRejectsEverythingOutsideEightExactForms(t *testing.T) {
	t.Parallel()

	revision := strings.Repeat("a", 64)
	bad := [][]string{
		nil,
		{string(verbPanes), revision},
		{string(verbPanes), revision, testBankID, "%7"},
		{string(verbObserve), revision, testBankID},
		{string(verbCapture), revision, testBankID, "%01"},
		{string(verbNoticeFull), revision, testBankID, "%7", "text"},
		{"type", revision, testBankID, "%7"},
		{"run-shell", revision, testBankID},
		{string(verbPanes), strings.Repeat("A", 64), testBankID},
		{string(verbPanes), strings.Repeat("a", 63), testBankID},
		{string(verbPanes), revision, "UPPER"},
		{string(verbPanes), revision, strings.Repeat("a", 33)},
	}
	for _, args := range bad {
		host := newFakePlatform()
		code, body := executeFake(t.Context(), host, args)
		require.Equal(t, ExitGrammar, code, args)
		require.Empty(t, body, args)
		require.Zero(t, host.manifestLoads, args)
		require.Empty(t, host.calls, args)
	}

	req, ok := parseRequest([]string{string(verbCapture), revision, testBankID, "%12345678901234567890"})
	require.True(t, ok)
	require.Equal(t, "%12345678901234567890", req.pane)
}

func TestNoticeResponseRequiresManifestField(t *testing.T) {
	t.Parallel()

	registry := validRegistry()
	registry.NoticeResponse = ""
	host := newFakePlatform()
	host.manifest = registry.canonicalJSON()
	code, body := executeFake(t.Context(), host,
		helperArgs(host.manifest, verbNoticeResponse, "%7"))
	require.Equal(t, ExitGrammar, code)
	require.Empty(t, body)
	require.Empty(t, host.calls, "the unavailable verb must refuse before any tmux call")
}

func TestFailureOrderIsClosedBeforeTmux(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*fakePlatform)
		args func(*fakePlatform) []string
		want ExitCode
	}{
		{
			name: "root euid before manifest", want: ExitEnvironment,
			edit: func(host *fakePlatform) { host.uid = 0 },
		},
		{
			name: "manifest read", want: ExitEnvironment,
			edit: func(host *fakePlatform) { host.manifestErr = errors.New("mode") },
		},
		{
			name: "revision", want: ExitRevision,
			args: func(*fakePlatform) []string {
				return []string{string(verbPanes), strings.Repeat("f", 64), testBankID}
			},
		},
		{
			name: "bank absent", want: ExitGrammar,
			args: func(host *fakePlatform) []string {
				return []string{string(verbPanes), manifestRevision(host.manifest), "missing"}
			},
		},
		{
			name: "foreign euid", want: ExitEnvironment,
			edit: func(host *fakePlatform) { host.uid = testUID + 1 },
		},
		{
			name: "pane absent", want: ExitBinding,
			args: func(host *fakePlatform) []string {
				return helperArgs(host.manifest, verbCapture, "%9")
			},
		},
		{
			name: "socket", want: ExitEnvironment,
			edit: func(host *fakePlatform) { host.socketErr = errors.New("wrong inode") },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			host := newFakePlatform()
			if test.edit != nil {
				test.edit(host)
			}
			args := helperArgs(host.manifest, verbPanes)
			if test.args != nil {
				args = test.args(host)
			}
			code, body := executeFake(t.Context(), host, args)
			require.Equal(t, test.want, code)
			require.Empty(t, body)
			require.Empty(t, host.calls)
			if test.name == "root euid before manifest" {
				require.Zero(t, host.manifestLoads)
			}
		})
	}
}

func TestIdentityMismatchUsesBindingExitAndPreventsEffect(t *testing.T) {
	t.Parallel()

	for name, edit := range map[string]func(*fakePlatform){
		"pid": func(host *fakePlatform) {
			host.outputs = [][]byte{[]byte("%7\t4243\t120\t40\n")}
		},
		"start": func(host *fakePlatform) { host.starts[4242] = 999 },
		"boot":  func(host *fakePlatform) { host.bootID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			host := newFakePlatform()
			host.outputs = [][]byte{[]byte("%7\t4242\t120\t40\n")}
			edit(host)
			code, body := executeFake(t.Context(), host, helperArgs(host.manifest, verbNoticeShort, "%7"))
			require.Equal(t, ExitBinding, code)
			require.Empty(t, body)
			require.Len(t, host.calls, 1, "the notice effect must not start")
		})
	}
}

func TestRegistryRevisionIsRecheckedImmediatelyBeforeWriting(t *testing.T) {
	t.Parallel()

	host := newFakePlatform()
	rotated := validRegistry()
	bank := rotated.Banks[testBankID]
	bank.Socket = "/tmp/tmux-1001/other"
	rotated.Banks[testBankID] = bank
	host.manifests = [][]byte{host.manifest, host.manifest, rotated.canonicalJSON()}
	host.outputs = [][]byte{[]byte("%7\t4242\t120\t40\n")}

	code, body := executeFake(t.Context(), host, helperArgs(host.manifest, verbNoticeShort, "%7"))
	require.Equal(t, ExitRevision, code)
	require.Empty(t, body)
	require.Len(t, host.calls, 1)
	require.Equal(t, 3, host.manifestLoads)
}

func TestTmuxFailuresNeverReturnPartialBody(t *testing.T) {
	t.Parallel()

	for _, code := range []ExitCode{ExitTmux, ExitTimeout, ExitOutputLimit} {
		host := newFakePlatform()
		host.outputs = [][]byte{[]byte("%7\t4242\t120\t40\n"), []byte("partial")}
		host.codes = []ExitCode{ExitOK, code}
		got, body := executeFake(t.Context(), host, helperArgs(host.manifest, verbCapture, "%7"))
		require.Equal(t, code, got)
		require.Empty(t, body)
	}
}

func TestExpiredHelperDeadlineReturnsTimeoutWithoutTmux(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	host := newFakePlatform()
	code, body := executeFake(ctx, host, helperArgs(host.manifest, verbPanes))
	require.Equal(t, ExitTimeout, code)
	require.Empty(t, body)
	require.Empty(t, host.calls)
}

func TestRegistryParserRequiresExactCanonicalJSON(t *testing.T) {
	t.Parallel()

	valid := validManifest()
	parsed, err := ParseRegistry(valid)
	require.NoError(t, err)
	require.Equal(t, testNoticeFull, parsed.NoticeFull)
	require.Equal(t, testNoticeResponse, parsed.NoticeResponse)
	require.Equal(t, testNoticeShort, parsed.NoticeShort)
	require.Equal(t, int64(4242), parsed.Banks[testBankID].Panes["%7"].PanePID)
	reordered := bytes.Replace(valid,
		[]byte(`},"socket":"/tmp/tmux-1001/agent","uid":1001}`),
		[]byte(`},"uid":1001,"socket":"/tmp/tmux-1001/agent"}`), 1)
	invalidUTF8 := slices.Clone(valid)
	invalidOffset := bytes.Index(invalidUTF8, []byte(testNoticeShort))
	require.NotEqual(t, -1, invalidOffset)
	invalidUTF8[invalidOffset] = 0xff

	invalid := map[string][]byte{
		"leading whitespace": append([]byte(" "), valid...),
		"trailing newline":   append(slices.Clone(valid), '\n'),
		"reordered keys":     reordered,
		"invalid utf8":       invalidUTF8,
		"unknown field": bytes.Replace(valid, []byte(`"schema_version":1}`),
			[]byte(`"schema_version":1,"unknown":true}`), 1),
		"duplicate field": bytes.Replace(valid, []byte(`"schema_version":1}`),
			[]byte(`"schema_version":1,"schema_version":1}`), 1),
		"alternate escape": bytes.Replace(valid, []byte(`GIFFORD`),
			[]byte(`\u0047IFFORD`), 1),
		"wrong schema": bytes.Replace(valid, []byte(`"schema_version":1`),
			[]byte(`"schema_version":2`), 1),
		"root uid": bytes.Replace(valid, []byte(`"uid":1001`), []byte(`"uid":0`), 1),
		"socket name": bytes.Replace(valid, []byte(`/tmp/tmux-1001/agent`),
			[]byte(`/tmp/tmux-1001/agent.sock`), 1),
		"pane leading zero": bytes.Replace(valid, []byte(`"%7"`), []byte(`"%07"`), 1),
		"negative start": bytes.Replace(valid, []byte(`"pane_start":700`),
			[]byte(`"pane_start":-1`), 1),
		"shell expansion notice": bytes.Replace(valid, []byte(testNoticeShort),
			[]byte(": [GIFFORD] $(id)"), 1),
		"missing prefix notice": bytes.Replace(valid, []byte(testNoticeShort),
			[]byte("[GIFFORD] short notice"), 1),
		"invalid response notice": bytes.Replace(valid, []byte(testNoticeResponse),
			[]byte(": [GIFFORD] $(id)"), 1),
		"identical notice slots": bytes.Replace(valid, []byte(testNoticeFull),
			[]byte(testNoticeShort), 1),
		"response equals full": bytes.Replace(valid, []byte(testNoticeResponse),
			[]byte(testNoticeFull), 1),
		"response equals short": bytes.Replace(valid, []byte(testNoticeResponse),
			[]byte(testNoticeShort), 1),
	}
	for name, raw := range invalid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseRegistry(raw)
			require.Error(t, err)
		})
	}

	empty := validRegistry()
	bank := empty.Banks[testBankID]
	bank.Panes = map[string]PaneBinding{}
	empty.Banks[testBankID] = bank
	_, err = ParseRegistry(empty.canonicalJSON())
	require.NoError(t, err)

	withoutResponse := validRegistry()
	withoutResponse.NoticeResponse = ""
	parsed, err = ParseRegistry(withoutResponse.canonicalJSON())
	require.NoError(t, err)
	require.Empty(t, parsed.NoticeResponse, "notice_response stays additive and optional")
}

func TestFrameIsExactAndBinarySafe(t *testing.T) {
	t.Parallel()

	host := newFakePlatform()
	host.outputs = [][]byte{[]byte("%7\t4242\t120\t40\n"), {0, '\n', 0xff}}
	var stdout bytes.Buffer
	code := runCLI(t.Context(), host, host, host,
		helperArgs(host.manifest, verbCapture, "%7"), &stdout)
	require.Equal(t, int(ExitOK), code)
	header, encoded, found := strings.Cut(stdout.String(), "\n")
	require.True(t, found)
	require.Equal(t, "GTMX/1 0 3", header)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte{0, '\n', 0xff})+"\n", encoded)

	stdout.Reset()
	code = runCLI(t.Context(), newFakePlatform(), newFakePlatform(), newFakePlatform(),
		[]string{"run-shell"}, &stdout)
	require.Equal(t, int(ExitGrammar), code)
	require.Equal(t, "GTMX/1 2 0\n\n", stdout.String())
	require.Len(t, stdout.Bytes(), 12)
}

func FuzzParseRegistry(f *testing.F) {
	f.Add(validManifest())
	f.Add([]byte(`{"banks":{}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		registry, err := ParseRegistry(raw)
		if err == nil {
			require.Equal(t, raw, registry.canonicalJSON())
		}
	})
}
