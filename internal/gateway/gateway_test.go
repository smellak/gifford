package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// Invented test values. The token and the URL must never reach an error,
// a record or a printed config.
const (
	testToken    = "sentinel-token-7f3"
	testURLEnv   = "GIFFORD_TEST_GLIMMER_URL"
	testTokenEnv = "GIFFORD_TEST_GLIMMER_TOKEN"
	testAdapter  = "abababababababababababababababababababababababababababababababab"
	secretMarker = "{{github_pat}}"
)

const testConfigYAML = `
endpoint:
  url_env: GIFFORD_TEST_GLIMMER_URL
  token_env: GIFFORD_TEST_GLIMMER_TOKEN
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: analyst-lora
    adapter_name: analyst
    adapter_revision: r1
    adapter_sha256: ` + testAdapter + `
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`

func envOf(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func testConfig(t *testing.T, url string) Config {
	t.Helper()
	cfg, err := ParseConfig(strings.NewReader(testConfigYAML), envOf(map[string]string{testURLEnv: url, testTokenEnv: testToken}))
	require.NoError(t, err)
	return cfg
}

// fakeClock advances 7 ms on every reading and fires timers only when
// the test advances it.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []fakeTimer
	asked   []time.Duration
	armedCh chan struct{}
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), armedCh: make(chan struct{}, 64)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(7 * time.Millisecond)
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), ch: ch})
	c.asked = append(c.asked, d)
	c.armedCh <- struct{}{}
	return ch
}

// advance moves the clock and fires every timer now due.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.timers[:0]
	for _, tm := range c.timers {
		if tm.at.After(c.now) {
			kept = append(kept, tm)
			continue
		}
		tm.ch <- c.now
	}
	c.timers = kept
}

func (c *fakeClock) deadlines() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.asked...)
}

type seqIDs struct{ n atomic.Int64 }

func (s *seqIDs) NewCallID() protocol.OpaqueID {
	return protocol.OpaqueID(fmt.Sprintf("call-%d", s.n.Add(1)))
}

type memRecorder struct {
	mu   sync.Mutex
	recs []protocol.GlimmerCallRecord
	err  error
}

func (m *memRecorder) RecordCall(_ context.Context, rec protocol.GlimmerCallRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.recs = append(m.recs, rec)
	return nil
}

func (m *memRecorder) all() []protocol.GlimmerCallRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]protocol.GlimmerCallRecord(nil), m.recs...)
}

type fixture struct {
	gw       *Gateway
	recorder *memRecorder
	url      string
}

func newFixture(t *testing.T, url string, tr Transport, mutate func(*Deps)) fixture {
	t.Helper()
	r, err := redact.New(redact.Options{})
	require.NoError(t, err)
	cfg := testConfig(t, url)
	if tr == nil {
		tr, err = NewHTTPTransport(cfg.Endpoint, nil)
		require.NoError(t, err)
	}
	rec := &memRecorder{}
	deps := Deps{
		Transport: tr,
		Redactor:  r,
		Recorder:  rec,
		Clock:     newFakeClock(),
		IDs:       &seqIDs{},
	}
	if mutate != nil {
		mutate(&deps)
	}
	gw, err := New(cfg, deps)
	require.NoError(t, err)
	return fixture{gw: gw, recorder: rec, url: url}
}

// cassette is one recorded exchange with the endpoint. Only recordable
// cassettes are re-recorded; the others are hand-written answers the
// real endpoint would not give on demand.
type cassette struct {
	Name       string          `json:"name"`
	Recordable bool            `json:"recordable"`
	Request    json.RawMessage `json:"request"`
	Wire       json.RawMessage `json:"wire_request"`
	Status     int             `json:"status"`
	Response   string          `json:"response"`
	Want       struct {
		Status protocol.GlimmerCallStatus `json:"status"`
		Label  string                     `json:"label"`
	} `json:"want"`
}

func loadCassettes(t *testing.T) []cassette {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "cassettes", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	out := make([]cassette, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		require.NoError(t, err)
		var c cassette
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		require.NoError(t, dec.Decode(&c), p)
		out = append(out, c)
	}
	return out
}

// inferRequest builds the cassette's request, planting the synthetic
// secret where the cassette marks it.
func (c cassette) inferRequest(t *testing.T, secret string) protocol.InferRequest {
	t.Helper()
	var req protocol.InferRequest
	require.NoError(t, json.Unmarshal(bytes.ReplaceAll(c.Request, []byte(secretMarker), []byte(secret)), &req))
	return req
}

func compact(t *testing.T, b []byte) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.Compact(&buf, b))
	return buf.String()
}

// replayServer answers as the cassette says, after checking that the
// request is exactly the recorded one and carries the credential.
func replayServer(t *testing.T, c cassette, seen *atomic.Int64) *httptest.Server {
	t.Helper()
	want := compact(t, c.Wire)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+testToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if string(body) != want {
			t.Errorf("cassette %s: request differs\n got: %s\nwant: %s", c.Name, body, want)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(c.Status)
		_, _ = io.WriteString(w, c.Response)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testSecret(t *testing.T) string {
	t.Helper()
	v, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	return v
}

// TestCassettes replays every cassette through the real HTTP transport.
// With GIFFORD_RECORD=1 (task test:record) it re-records the recordable
// ones against the endpoint named by GIFFORD_GLIMMER_URL and
// GIFFORD_GLIMMER_TOKEN instead.
func TestCassettes(t *testing.T) {
	t.Parallel()
	if os.Getenv("GIFFORD_RECORD") == "1" {
		recordCassettes(t)
		return
	}
	secret := testSecret(t)
	for _, c := range loadCassettes(t) {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			var seen atomic.Int64
			srv := replayServer(t, c, &seen)
			f := newFixture(t, srv.URL, nil, nil)
			res, err := f.gw.Infer(context.Background(), c.inferRequest(t, secret))
			require.Equal(t, int64(1), seen.Load(), "exactly one request left the node")

			recs := f.recorder.all()
			require.Len(t, recs, 1, "every call leaves one record")
			rec := recs[0]
			require.NoError(t, rec.Validate(), "the record is complete")
			require.Equal(t, c.Want.Status, rec.Status)
			require.Equal(t, rec, res.CallRecord)
			require.Equal(t, sha256Hex([]byte(compact(t, c.Wire))), rec.InputSHA256, "the input digest is of what was sent")
			assertNoLeak(t, err, rec, srv.URL, secret)

			if c.Want.Status == protocol.GlimmerCallStatusOK {
				require.NoError(t, err)
				require.NoError(t, res.Validate())
				require.Equal(t, c.Want.Label, res.Label)
				require.Equal(t, res.Probabilities, rec.Probabilities)
				require.Equal(t, sha256Hex([]byte(c.Response)), rec.OutputSHA256)
				return
			}
			require.Error(t, err)
			require.Empty(t, res.Label, "a failed call returns no label")
			require.Empty(t, res.Probabilities)
			require.Empty(t, res.Evidence)
			require.Empty(t, rec.Probabilities)
			require.Empty(t, rec.RequestID, "a failed call keeps no answer metadata")
			switch c.Want.Status {
			case protocol.GlimmerCallStatusInvalid:
				require.ErrorIs(t, err, ErrInvalidOutput)
			case protocol.GlimmerCallStatusError:
				require.ErrorIs(t, err, ErrCallFailed)
			default:
				t.Fatalf("cassette %s: unexpected want status %s", c.Name, c.Want.Status)
			}
		})
	}
}

// recordCassettes re-records against a live endpoint; never in CI.
func recordCassettes(t *testing.T) {
	t.Helper()
	url, token := os.Getenv("GIFFORD_GLIMMER_URL"), os.Getenv("GIFFORD_GLIMMER_TOKEN")
	if url == "" || token == "" {
		t.Skip("recording needs GIFFORD_GLIMMER_URL and GIFFORD_GLIMMER_TOKEN")
	}
	ep, err := resolveEndpoint("u", "t", envOf(map[string]string{"u": url, "t": token}))
	require.NoError(t, err)
	tr, err := NewHTTPTransport(ep, nil)
	require.NoError(t, err)
	for _, c := range loadCassettes(t) {
		if !c.Recordable {
			continue
		}
		answer, err := tr.Send(context.Background(), []byte(compact(t, c.Wire)))
		require.NoError(t, err, c.Name)
		c.Status, c.Response = http.StatusOK, string(answer)
		b, err := json.MarshalIndent(c, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join("testdata", "cassettes", c.Name+".json"), append(b, '\n'), 0o600))
	}
}

// assertNoLeak checks that neither the error nor the record carries the
// endpoint, the credential or the unredacted secret.
func assertNoLeak(t *testing.T, err error, rec protocol.GlimmerCallRecord, url, secret string) {
	t.Helper()
	enc, mErr := json.Marshal(rec)
	require.NoError(t, mErr)
	for _, s := range []string{url, strings.TrimPrefix(url, "http://"), testToken, secret} {
		require.NotContains(t, string(enc), s)
		if err != nil {
			require.NotContains(t, err.Error(), s)
		}
	}
}

func analyzeRequest(secret string) protocol.InferRequest {
	return protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		Payload:  protocol.Payload(`{"screen_lines":["export GITHUB_TOKEN=` + secret + `"]}`),
	}
}

func TestInferRecordsTheAnalystAdapter(t *testing.T) {
	t.Parallel()
	c := loadCassettes(t)[0]
	require.Equal(t, "analyze_ok", c.Name)
	var seen atomic.Int64
	srv := replayServer(t, c, &seen)
	f := newFixture(t, srv.URL, nil, nil)
	res, err := f.gw.Infer(context.Background(), c.inferRequest(t, testSecret(t)))
	require.NoError(t, err)
	rec := res.CallRecord
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, rec.InferenceProfile)
	require.Equal(t, "analyst", rec.AdapterName)
	require.Equal(t, "r1", rec.AdapterRevision)
	require.Equal(t, protocol.SHA256Hex(testAdapter), rec.AdapterSHA256)
	require.Equal(t, protocol.AgentID("agent-1"), rec.AgentID)
	require.Equal(t, int64(1), rec.RedactionsCount, "the planted secret was redacted")
	require.Equal(t, redact.PolicyVersion, rec.RedactionPolicyVersion)
	require.Equal(t, int64(7), rec.LatencyMS)
	require.Equal(t, protocol.OpaqueID("call-1"), rec.CallID)
	require.Equal(t, "vllm", rec.Runtime)
	require.Equal(t, "0.9.0", rec.RuntimeVersion)
	require.Equal(t, "L4", rec.GPU)
}

type failingRedactor struct{ panics bool }

func (f failingRedactor) RedactText(string) (redact.TextResult, error) {
	if f.panics {
		panic("sentinel panic")
	}
	return redact.TextResult{}, errors.New("sentinel redactor detail")
}

func (f failingRedactor) RedactScreen(redact.Screen) (redact.ScreenResult, error) {
	return redact.ScreenResult{}, errors.New("sentinel redactor detail")
}

// screenFailRedactor redacts text but fails on screens.
type screenFailRedactor struct{ Redactor }

func (screenFailRedactor) RedactScreen(redact.Screen) (redact.ScreenResult, error) {
	return redact.ScreenResult{}, errors.New("sentinel redactor detail")
}

// versionSkewRedactor names another policy version for screens.
type versionSkewRedactor struct{ Redactor }

func (v versionSkewRedactor) RedactScreen(s redact.Screen) (redact.ScreenResult, error) {
	res, err := v.Redactor.RedactScreen(s)
	res.PolicyVersion += "-other"
	return res, err
}

// countingTransport counts sends and answers with a fixed result.
type countingTransport struct {
	n      atomic.Int64
	answer []byte
	err    error
	panics bool
	block  bool // ignores its context and never returns
}

func (c *countingTransport) Send(_ context.Context, _ []byte) ([]byte, error) {
	c.n.Add(1)
	if c.panics {
		panic("sentinel panic")
	}
	if c.block {
		select {}
	}
	return c.answer, c.err
}

func TestInferRedactionFailureSendsNothing(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	base, err := redact.New(redact.Options{})
	require.NoError(t, err)
	for name, red := range map[string]Redactor{
		"redactor error":  failingRedactor{},
		"redactor panic":  failingRedactor{panics: true},
		"screen fails":    screenFailRedactor{base},
		"version differs": versionSkewRedactor{base},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &countingTransport{}
			f := newFixture(t, "http://127.0.0.1:1", tr, func(d *Deps) { d.Redactor = red })
			res, err := f.gw.Infer(context.Background(), analyzeRequest(secret))
			require.ErrorIs(t, err, ErrRedactionFailed)
			require.Zero(t, tr.n.Load(), "nothing is sent")
			require.Equal(t, protocol.GlimmerCallStatusRedactionFailed, res.CallRecord.Status)
			require.Len(t, f.recorder.all(), 1)
			require.NoError(t, res.CallRecord.Validate())
			require.Equal(t, protocol.SHA256Hex(emptySHA256), res.CallRecord.InputSHA256)
			assertNoLeak(t, err, res.CallRecord, f.url, secret)
			require.NotContains(t, err.Error(), "sentinel")
		})
	}
}

func TestInferTimeout(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	for name, tr := range map[string]Transport{
		"slow endpoint":                 nil,
		"transport ignores its context": &countingTransport{block: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, srv.URL, tr, nil)
			clk := f.gw.deps.Clock.(*fakeClock)
			type result struct {
				res protocol.InferResult
				err error
			}
			done := make(chan result, 1)
			req := analyzeRequest(testSecret(t))
			go func() {
				res, err := f.gw.Infer(context.Background(), req)
				done <- result{res, err}
			}()
			<-clk.armedCh
			clk.advance(2999 * time.Millisecond)
			select {
			case <-done:
				t.Fatal("the call ended before its deadline")
			case <-time.After(20 * time.Millisecond):
			}
			clk.advance(time.Millisecond)
			r := <-done
			require.ErrorIs(t, r.err, ErrTimeout)
			require.Equal(t, protocol.GlimmerCallStatusTimeout, r.res.CallRecord.Status)
			require.NoError(t, r.res.CallRecord.Validate())
			assertNoLeak(t, r.err, r.res.CallRecord, srv.URL, testSecret(t))
		})
	}
}

func TestContractTimeouts(t *testing.T) {
	t.Parallel()
	tr := &countingTransport{answer: []byte(`{"label":"UNKNOWN","probabilities":{"UNKNOWN":1}}`)}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	for _, req := range []protocol.InferRequest{
		analyzeRequest("x"),
		{Contract: protocol.ModelContractTriageMessage, Payload: protocol.Payload(`{"text":"hi"}`)},
		{Contract: protocol.ModelContractChooseOrchestrationAction, Payload: orchestrationPayload(t, "")},
	} {
		_, _ = f.gw.Infer(context.Background(), req)
	}
	require.Equal(t, []time.Duration{3 * time.Second, 5 * time.Second, 3 * time.Second},
		f.gw.deps.Clock.(*fakeClock).deadlines())
}

func TestInferFailures(t *testing.T) {
	t.Parallel()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/elsewhere", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte(" "), maxResponseBytes+1))
	}))
	t.Cleanup(huge.Close)

	cases := []struct {
		name   string
		url    string
		tr     Transport
		status protocol.GlimmerCallStatus
		err    error
	}{
		{"network failure", closedURL, nil, protocol.GlimmerCallStatusError, ErrCallFailed},
		{"redirect not followed", redirect.URL, nil, protocol.GlimmerCallStatusError, ErrCallFailed},
		{"answer too large", huge.URL, nil, protocol.GlimmerCallStatusError, ErrCallFailed},
		{"transport error", closedURL, &countingTransport{err: errors.New("sentinel transport detail " + closedURL)}, protocol.GlimmerCallStatusError, ErrCallFailed},
		{"transport panic", closedURL, &countingTransport{panics: true}, protocol.GlimmerCallStatusError, ErrCallFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			secret := testSecret(t)
			f := newFixture(t, tc.url, tc.tr, nil)
			res, err := f.gw.Infer(context.Background(), analyzeRequest(secret))
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.status, res.CallRecord.Status)
			require.NoError(t, res.CallRecord.Validate())
			require.Len(t, f.recorder.all(), 1)
			assertNoLeak(t, err, res.CallRecord, tc.url, secret)
			require.NotContains(t, err.Error(), "sentinel")
		})
	}
}

func TestInferCancelledIsAnError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	tr := &countingTransport{err: context.Canceled}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	cancel()
	res, err := f.gw.Infer(ctx, analyzeRequest(testSecret(t)))
	require.ErrorIs(t, err, ErrCallFailed)
	require.Equal(t, protocol.GlimmerCallStatusError, res.CallRecord.Status)
	require.Len(t, f.recorder.all(), 1, "the record is stored after the caller gave up")
}

func TestInferInvalidRequest(t *testing.T) {
	t.Parallel()
	tr := &countingTransport{}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	for name, req := range map[string]protocol.InferRequest{
		"unknown contract": {Contract: "CLASSIFY", Payload: protocol.Payload(`{}`)},
		"bad agent id":     {Contract: protocol.ModelContractTriageMessage, AgentID: "Agent One", Payload: protocol.Payload(`{}`)},
		"bad orchestration payload": {
			Contract: protocol.ModelContractChooseOrchestrationAction, Payload: protocol.Payload(`{"sentinel":1}`),
		},
	} {
		_, err := f.gw.Infer(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest, name)
	}
	require.Zero(t, tr.n.Load(), "nothing is sent")
	require.Empty(t, f.recorder.all(), "a refused request is not a call")
}

func TestInferRecordFailureWithholdsTheResult(t *testing.T) {
	t.Parallel()
	c := loadCassettes(t)[0]
	var seen atomic.Int64
	srv := replayServer(t, c, &seen)
	f := newFixture(t, srv.URL, nil, func(d *Deps) { d.Recorder = &memRecorder{err: errors.New("sentinel store detail")} })
	res, err := f.gw.Infer(context.Background(), c.inferRequest(t, testSecret(t)))
	require.ErrorIs(t, err, ErrRecordFailed)
	require.NotContains(t, err.Error(), "sentinel")
	require.Equal(t, protocol.InferResult{}, res)
}

func TestNewRefusesMissingDeps(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, "http://127.0.0.1:1")
	for name, mutate := range map[string]func(*Deps){
		"transport": func(d *Deps) { d.Transport = nil },
		"redactor":  func(d *Deps) { d.Redactor = nil },
		"recorder":  func(d *Deps) { d.Recorder = nil },
		"clock":     func(d *Deps) { d.Clock = nil },
		"ids":       func(d *Deps) { d.IDs = nil },
	} {
		deps := Deps{Transport: &countingTransport{}, Redactor: failingRedactor{}, Recorder: &memRecorder{}, Clock: newFakeClock(), IDs: &seqIDs{}}
		mutate(&deps)
		_, err := New(cfg, deps)
		require.ErrorIs(t, err, ErrInvalidConfig, name)
	}
	_, err := New(Config{}, Deps{Transport: &countingTransport{}, Redactor: failingRedactor{}, Recorder: &memRecorder{}, Clock: newFakeClock(), IDs: &seqIDs{}})
	require.ErrorIs(t, err, ErrInvalidConfig, "a zero config is refused")
}
