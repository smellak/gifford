package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	automaticE2ERecipient      protocol.AgentID = "automatic-recipient"
	automaticE2ESender         protocol.AgentID = "automatic-sender"
	automaticE2EURLVar                          = "GIFFORD_E2E_MODEL_URL"
	automaticE2ETokenVar                        = "GIFFORD_E2E_MODEL_TOKEN"
	automaticE2EToken                           = "loopback-test-token"
	automaticE2EProfileSHA                      = "3333333333333333333333333333333333333333333333333333333333333333"
	automaticE2ECalibrationSHA                  = "4444444444444444444444444444444444444444444444444444444444444444"
	automaticE2EStateWait                       = 10 * time.Second
)

type automaticWireRequest struct {
	Contract         protocol.ModelContract    `json:"contract"`
	InferenceProfile protocol.InferenceProfile `json:"inference_profile"`
	PromptVersion    string                    `json:"prompt_version"`
	Messages         []json.RawMessage         `json:"messages"`
	Payload          json.RawMessage           `json:"payload"`
}

type automaticFakeEndpoint struct {
	server *httptest.Server
	url    string

	mu       sync.Mutex
	requests []automaticWireRequest
	times    []time.Time
	problems []string
}

func newAutomaticFakeEndpoint(t *testing.T) *automaticFakeEndpoint {
	t.Helper()
	endpoint := &automaticFakeEndpoint{}
	endpoint.server = httptest.NewServer(endpoint)
	endpoint.url = endpoint.server.URL
	t.Cleanup(endpoint.server.Close)
	return endpoint
}

func (e *automaticFakeEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		e.problem("method is not POST")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+automaticE2EToken {
		e.problem("bearer token was not presented")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		e.problem("request body was unreadable")
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var request automaticWireRequest
	if json.Unmarshal(body, &request) != nil || !request.Contract.Valid() ||
		request.InferenceProfile != protocol.InferenceProfileBase || request.PromptVersion == "" ||
		len(request.Messages) == 0 || len(request.Payload) == 0 {
		e.problem("request did not carry the gateway wire contract")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sequence := e.record(request)
	if request.Contract != protocol.ModelContractTriageMessage {
		http.Error(w, "unexpected contract", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if json.NewEncoder(w).Encode(map[string]any{
		"request_id":      fmt.Sprintf("loopback-request-%d", sequence),
		"runtime_version": "test-runtime-v1",
		"gpu":             "test-gpu",
		"label":           protocol.TriageLabelNormal,
		"probabilities": map[string]float64{
			string(protocol.TriageLabelNormal):      0.9,
			string(protocol.TriageLabelLowPriority): 0.1,
		},
		"evidence":    []string{"ordinary message"},
		"letter_mass": 0.9,
	}) != nil {
		e.problem("response could not be encoded")
	}
}

func (e *automaticFakeEndpoint) record(request automaticWireRequest) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests = append(e.requests, request)
	e.times = append(e.times, time.Now())
	return len(e.requests)
}

func (e *automaticFakeEndpoint) problem(problem string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.problems = append(e.problems, problem)
}

func (e *automaticFakeEndpoint) count(contract protocol.ModelContract) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	count := 0
	for _, request := range e.requests {
		if request.Contract == contract {
			count++
		}
	}
	return count
}

func (e *automaticFakeEndpoint) firstAt(contract protocol.ModelContract) (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for index, request := range e.requests {
		if request.Contract == contract {
			return e.times[index], true
		}
	}
	return time.Time{}, false
}

func (e *automaticFakeEndpoint) errors() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.problems...)
}

type automaticNegativeNode struct {
	ctx         context.Context
	environment e2eEnvironment
	endpoint    *automaticFakeEndpoint
	hub         *e2eProcess
	hubStderr   *lockedBuffer
	recipient   *e2eRecipient
	sender      *e2eAgent
	db          *sql.DB
	stopOnce    sync.Once
}

func startAutomaticNegativeNode(ctx context.Context, t *testing.T) *automaticNegativeNode {
	t.Helper()
	environment := newE2EEnvironment(t)
	endpoint := newAutomaticFakeEndpoint(t)
	configureAutomaticE2E(t, &environment, endpoint.url)
	hub, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	recipient := startE2ERecipient(ctx, t, environment, automaticE2ERecipient, "quiet")
	sender := startE2EAgentBinary(
		ctx, t, environment.env, automaticE2ESender, e2eMeasuredGiffordBinary,
	)
	node := &automaticNegativeNode{
		ctx: ctx, environment: environment, endpoint: endpoint, hub: hub,
		hubStderr: hubStderr, recipient: recipient, sender: sender,
		db: openE2EReadDB(t, environment.database),
	}
	t.Cleanup(func() { node.stop(nil) })

	var session, draft, activity string
	require.Eventually(t, func() bool {
		if node.db.QueryRowContext(ctx, `SELECT session_id FROM sessions
			WHERE agent_id = ? AND state = 'CONNECTED' AND capture_mode = 'pty_proxy'
			AND deliverable = 1 AND ended_at IS NULL`, automaticE2ERecipient).Scan(&session) != nil {
			return false
		}
		return node.db.QueryRowContext(ctx, `SELECT draft_state, activity_state FROM input_guards
			WHERE session_id = ? AND agent_id = ?`, session, automaticE2ERecipient).
			Scan(&draft, &activity) == nil && draft == string(protocol.DraftStateClosed) &&
			activity == string(protocol.ActivityStateIdle)
	}, 15*time.Second, 20*time.Millisecond, "real Proxy never reported a quiet closed prompt")
	var observationGeneration int64
	require.Eventually(t, func() bool {
		return node.db.QueryRowContext(ctx, `SELECT coalesce(max(observation_generation), 0)
			FROM state_snapshots WHERE agent_id = ? AND session_id = ?
			AND observation_id LIKE 'o-%'`, automaticE2ERecipient, session).
			Scan(&observationGeneration) == nil && observationGeneration >= 3
	}, 15*time.Second, 20*time.Millisecond,
		"real Proxy never reached a stable periodic observation")

	var channel protocol.AgentChannel
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &channel,
		"ctl", "agent", string(automaticE2ERecipient), "channel", "status")
	require.True(t, channel.Effective.AutoDelivery)
	return node
}

func (n *automaticNegativeNode) stop(t *testing.T) {
	n.stopOnce.Do(func() {
		if n.sender != nil {
			select {
			case <-n.sender.process.done:
			default:
				_ = n.sender.process.terminateAndWait()
			}
			_ = n.sender.outer.Close()
		}
		if n.recipient != nil {
			_ = n.recipient.process.terminateAndWait()
			_ = n.recipient.outer.Close()
		}
		if n.hub != nil {
			err := n.hub.terminateAndWait()
			if t != nil {
				require.NoError(t, err, "hub stderr: %s", n.hubStderr.String())
			}
		}
	})
}

func (n *automaticNegativeNode) send(t *testing.T) protocol.ULID {
	t.Helper()
	var admitted protocol.AdmissionResult
	n.sender.callTool(t, "send_message", map[string]any{
		e2eToField: automaticE2ERecipient, e2eSubjectField: "automatic decision",
		e2eBodyField: "defer behind a withheld screen", e2eClientIDField: "automatic-withheld",
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	require.True(t, admitted.MessageID.Valid())
	return admitted.MessageID
}

func (n *automaticNegativeNode) pendingApproval(message protocol.ULID) (protocol.ApprovalRequest, bool) {
	var pending protocol.ApprovalList
	if readE2ECLIJSON(n.ctx, n.environment.env, e2eMeasuredGiffordBinary,
		&pending, "ctl", "approvals", "--pending") != nil {
		return protocol.ApprovalRequest{}, false
	}
	for _, approval := range pending.Approvals {
		if approval.ObjectID == string(message) && approval.AgentID == automaticE2ERecipient {
			return approval, true
		}
	}
	return protocol.ApprovalRequest{}, false
}

func configureAutomaticE2E(t *testing.T, environment *e2eEnvironment, endpoint string) {
	t.Helper()
	root := automaticE2EEnvValue(environment.env, "XDG_CONFIG_HOME")
	require.NotEmpty(t, root)
	gatewayPath := filepath.Join(root, "gifford", "gateway.yaml")
	require.NoError(t, os.WriteFile(gatewayPath, []byte(`endpoint:
  url_env: `+automaticE2EURLVar+`
  token_env: `+automaticE2ETokenVar+`
base_model:
  repo: example.invalid/glimmer
  revision: revision-v1
runtime: test-runtime
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`), 0o600))
	config := fmt.Sprintf(`schema_version: 1
node_id: node-a
ml:
  screen_lines: 20
  mask: none
state_analysis:
  gateway_config: %q
  profile_revision: profile-v1
  profile_sha256: %q
  calibration_revision: calibration-v1
  calibration_sha256: %q
  temperature: 1
  idle_threshold: 0.9
`, gatewayPath, automaticE2EProfileSHA, automaticE2ECalibrationSHA)
	require.NoError(t, os.WriteFile(filepath.Join(root, "gifford", "config.yaml"), []byte(config), 0o600))
	environment.env = append(environment.env,
		automaticE2EURLVar+"="+endpoint,
		automaticE2ETokenVar+"="+automaticE2EToken,
	)
}

func automaticE2EEnvValue(env []string, name string) string {
	prefix := name + "="
	for i := len(env) - 1; i >= 0; i-- {
		if strings.HasPrefix(env[i], prefix) {
			return strings.TrimPrefix(env[i], prefix)
		}
	}
	return ""
}

func TestAutomaticDecisionRealProxyWithheld(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	node := startAutomaticNegativeNode(ctx, t)
	sentAt := time.Now()
	message := node.send(t)
	var approval protocol.ApprovalRequest
	require.Eventually(t, func() bool {
		var ok bool
		approval, ok = node.pendingApproval(message)
		return ok
	}, 25*time.Second, 50*time.Millisecond,
		"real Proxy did not defer after the state wait: %s", node.hubStderr.String())
	require.Contains(t, approval.RequestNote, "automatic delivery deferred:")

	var queueState string
	var approvals, attempts, analyzeCalls int
	require.NoError(t, node.db.QueryRowContext(ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`, message, automaticE2ERecipient).Scan(&queueState))
	require.Equal(t, "QUEUED", queueState)
	require.NoError(t, node.db.QueryRowContext(ctx, `SELECT count(*) FROM approval_requests
		WHERE object_id = ? AND agent_id = ?`, message, automaticE2ERecipient).Scan(&approvals))
	require.Equal(t, 1, approvals)
	require.NoError(t, node.db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ?`, message, automaticE2ERecipient).Scan(&attempts))
	require.Zero(t, attempts)
	require.NoError(t, node.db.QueryRowContext(ctx, `SELECT count(*) FROM glimmer_calls
		WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&analyzeCalls))
	require.Zero(t, analyzeCalls, "WITHHELD screen must stop before the external ANALYZE contract")

	require.Zero(t, node.endpoint.count(protocol.ModelContractAnalyzeAgentState))
	require.Equal(t, 1, node.endpoint.count(protocol.ModelContractTriageMessage))
	require.Zero(t, node.endpoint.count(protocol.ModelContractChooseOrchestrationAction))
	triageAt, found := node.endpoint.firstAt(protocol.ModelContractTriageMessage)
	require.True(t, found)
	require.GreaterOrEqual(t, triageAt.Sub(sentAt), automaticE2EStateWait-500*time.Millisecond)
	require.Empty(t, node.recipient.deliveredLines(t))
	require.NotContains(t, node.recipient.output.String(), "[GIFFORD]",
		"the real Proxy negative must inject zero notice bytes")
	require.Empty(t, node.endpoint.errors())
	node.stop(t)
}
