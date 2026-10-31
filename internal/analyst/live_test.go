package analyst

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// The live test's switch, and the deployed endpoint, named by the same
// variables the gateway cassettes are recorded with.
const (
	liveEnv      = "GIFFORD_LIVE"
	liveURLEnv   = "GIFFORD_GLIMMER_URL"
	liveTokenEnv = "GIFFORD_GLIMMER_TOKEN"
)

const (
	// liveStartWait bounds the wait for the endpoint's first answer: the
	// longest startup timeout ml/serve accepts.
	liveStartWait    = 30 * time.Minute
	liveProbeTimeout = 5 * time.Minute
	liveProbeEvery   = 2 * time.Second
	// liveColdAfter marks a cold start: a first answer slower than the
	// gateway's ANALYZE_AGENT_STATE deadline means a container started.
	liveColdAfter = 3 * time.Second
	// liveRepeats is how many calls each contract gets.
	liveRepeats = 3
)

// liveAnalyze is a synthetic ANALYZE_AGENT_STATE payload with a plain
// answer: an approval menu on screen.
const liveAnalyze = `{"harness":"codex","seconds_since_last_output":8,` +
	`"output_bytes":{"last_500ms":0,"last_2s":0,"last_10s":0},"new_child_processes":[],` +
	`"previous_state":"RUNNING_TOOL","previous_state_age_seconds":20,` +
	`"screen_lines":["Run this command?","  rm -rf ./build","> 1. Yes","  2. No",` +
	`"Press enter to confirm or esc to cancel"]}`

// liveTriage is a synthetic TRIAGE_MESSAGE payload.
const liveTriage = `{"text":"Subject: parser review\n\nPlease review the parser change before Friday.\nThe release waits on it."}`

// TestLiveGlimmer calls a deployed Glimmer endpoint through the real
// gateway and prompts, on the base model, for the three contracts with
// synthetic payloads. It runs only with GIFFORD_LIVE=1 (task test:live),
// never in CI. It first waits for the endpoint to answer at all, and logs
// how long that took, cold when a container started. Every call then
// runs warm, and it logs each one's contract, status, label and latency,
// with the letter mass at the first token that is not special. It never
// logs the endpoint, the token or a payload.
func TestLiveGlimmer(t *testing.T) {
	if os.Getenv(liveEnv) != "1" {
		t.Skip("needs a deployed endpoint: task test:live, with " + liveURLEnv + " and " + liveTokenEnv)
	}
	url, token := os.Getenv(liveURLEnv), os.Getenv(liveTokenEnv)
	require.True(t, url != "" && token != "", "set %s and %s", liveURLEnv, liveTokenEnv)
	first, start := firstAnswer(t, url), "warm"
	if first > liveColdAfter {
		start = "cold"
	}
	t.Logf("endpoint: first answer after %.1f s, %s", first.Seconds(), start)

	cfg, err := gateway.ParseConfig(strings.NewReader(gatewayYAML), func(k string) string {
		return map[string]string{"U": url, "T": token}[k]
	})
	require.NoError(t, err)
	tr, err := gateway.NewHTTPTransport(cfg.Endpoint, nil)
	require.NoError(t, err)
	tap := &tappedTransport{next: tr}
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	deps := gateway.Deps{
		Transport: tap, Redactor: red, Recorder: &memRecorder{}, Clock: gateway.SystemClock{},
		IDs: gateway.RandomCallIDs{}, Prompts: Prompts{},
	}

	for _, c := range []struct {
		contract protocol.ModelContract
		payload  string
	}{
		{protocol.ModelContractAnalyzeAgentState, liveAnalyze},
		{protocol.ModelContractTriageMessage, liveTriage},
		{protocol.ModelContractChooseOrchestrationAction, fullChoose},
	} {
		// A gateway per contract, so the failures of one do not open the
		// breaker on the next.
		gw, err := gateway.New(cfg, deps)
		require.NoError(t, err)
		for i := 1; i <= liveRepeats; i++ {
			res, err := gw.Infer(context.Background(), protocol.InferRequest{
				Contract: c.contract, Payload: protocol.Payload(c.payload),
			})
			rec := res.CallRecord
			t.Logf("%s call %d: status %s, label %q, %d ms, warm (letter mass %.3f, gpu %q)",
				c.contract, i, rec.Status, res.Label, rec.LatencyMS, tap.letterMass(), rec.GPU)
			if !assert.NoError(t, err) {
				continue
			}
			assert.NoError(t, res.Validate())
			assert.Equal(t, protocol.GlimmerCallStatusOK, rec.Status)
			if c.contract == protocol.ModelContractAnalyzeAgentState {
				// Below a letter mass of 0.5 the endpoint answers UNKNOWN:
				// the model wrote something else first, such as reasoning.
				assert.NotEqual(t, string(protocol.AgentPhaseUnknown), res.Label, "the label is the first token")
			}
		}
	}
}

// firstAnswer posts an empty body with no credential until the endpoint
// answers 401, and returns how long that took. It sends neither the token
// nor any data, and never logs a transport error, which names the
// endpoint.
func firstAnswer(t *testing.T, url string) time.Duration {
	t.Helper()
	client := &http.Client{
		Timeout:       liveProbeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	start := time.Now()
	for time.Since(start) < liveStartWait {
		status := 0
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader("{}"))
		require.NoError(t, err, "the endpoint URL does not parse")
		if resp, err := client.Do(req); err == nil {
			status = resp.StatusCode
			_ = resp.Body.Close()
		}
		switch {
		case status == http.StatusUnauthorized:
			return time.Since(start)
		case status >= 400 && status < 500 && status != http.StatusRequestTimeout && status != http.StatusTooManyRequests:
			t.Fatalf("the endpoint answers %d without a credential, not 401: check that the URL ends in /infer", status)
		}
		time.Sleep(liveProbeEvery)
	}
	t.Fatal("the endpoint did not answer in time")
	return 0
}

// tappedTransport passes each call to the endpoint and keeps the last
// answer, so the test reads its letter mass, which no result carries.
type tappedTransport struct {
	next gateway.Transport
	mu   sync.Mutex
	last []byte
}

func (t *tappedTransport) Send(ctx context.Context, body []byte) ([]byte, error) {
	out, err := t.next.Send(ctx, body)
	t.mu.Lock()
	t.last = out
	t.mu.Unlock()
	return out, err
}

// letterMass is the last answer's letter mass, or -1 when it has none.
func (t *tappedTransport) letterMass() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var a struct {
		LetterMass *float64 `json:"letter_mass"`
	}
	if json.Unmarshal(t.last, &a) != nil || a.LetterMass == nil {
		return -1
	}
	return *a.LetterMass
}
