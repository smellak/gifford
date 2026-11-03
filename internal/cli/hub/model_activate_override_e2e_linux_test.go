//go:build linux

package hub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/ctl"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	modelOverrideAdapterName     = "synthetic-analyst"
	modelOverrideAdapterRevision = "lora-e2e-v1"
	modelOverrideAdapterSHA      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	modelOverrideTrainingRun     = "training-run-e2e-1"
)

type modelOverrideWireRequest struct {
	Contract         protocol.ModelContract    `json:"contract"`
	InferenceProfile protocol.InferenceProfile `json:"inference_profile"`
	AdapterName      string                    `json:"adapter_name"`
	AdapterRevision  string                    `json:"adapter_revision"`
}

type modelOverrideGlimmer struct {
	mu           sync.Mutex
	quantization string
	requests     []modelOverrideWireRequest
	errors       []error
}

func (g *modelOverrideGlimmer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var request modelOverrideWireRequest
	err := json.NewDecoder(r.Body).Decode(&request)

	g.mu.Lock()
	if err != nil {
		g.errors = append(g.errors, err)
	} else {
		g.requests = append(g.requests, request)
	}
	quantization := g.quantization
	requestID := fmt.Sprintf("glimmer-request-%d", len(g.requests))
	g.mu.Unlock()

	if err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Label          string             `json:"label"`
		Probabilities  map[string]float64 `json:"probabilities"`
		Evidence       []string           `json:"evidence"`
		RequestID      protocol.OpaqueID  `json:"request_id"`
		RuntimeVersion string             `json:"runtime_version"`
		GPU            string             `json:"gpu"`
		Quantization   string             `json:"quantization"`
	}{
		Label:          string(protocol.AgentPhaseIdleAtPrompt),
		Probabilities:  map[string]float64{string(protocol.AgentPhaseIdleAtPrompt): 1},
		Evidence:       []string{"synthetic glimmer endpoint"},
		RequestID:      protocol.OpaqueID(requestID),
		RuntimeVersion: "synthetic-runtime-v1",
		GPU:            "synthetic-cpu",
		Quantization:   quantization,
	})
}

func (g *modelOverrideGlimmer) setQuantization(value string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.quantization = value
}

func (g *modelOverrideGlimmer) snapshot() ([]modelOverrideWireRequest, []error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]modelOverrideWireRequest(nil), g.requests...), append([]error(nil), g.errors...)
}

type modelOverrideNode struct {
	store   *eventstore.Store
	readDB  *sql.DB
	wiring  *automaticDecisionWiring
	cancel  context.CancelFunc
	serveCh chan error
}

func startModelOverrideNode(
	t *testing.T,
	databasePath string,
	runtimeDir string,
	cfg config.Config,
) *modelOverrideNode {
	t.Helper()
	store, err := eventstore.Open(t.Context(), databasePath)
	require.NoError(t, err)
	clock := agentlink.SystemClock{}
	engine, err := channel.New(channel.Options{Clock: clock})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock, NodeID: cfg.NodeID,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	wiring, err := newAutomaticDecisionWiring(
		cfg, store, controller, engine, clock,
		orchestrator.NewPersistenceGate(), io.Discard,
	)
	require.NoError(t, err)
	readDB, err := openReadDB(t.Context(), databasePath)
	require.NoError(t, err)
	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: runtimeDir, DB: store.DB(), ReadDB: readDB,
		Controller: controller, Model: modelService(wiring.modelControl),
	})
	require.NoError(t, err)
	serveCtx, cancel := context.WithCancel(t.Context())
	serveCh := make(chan error, 1)
	go func() { serveCh <- server.Serve(serveCtx) }()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(server.Path())
		return statErr == nil
	}, 5*time.Second, 5*time.Millisecond)
	return &modelOverrideNode{
		store: store, readDB: readDB, wiring: wiring, cancel: cancel, serveCh: serveCh,
	}
}

func (n *modelOverrideNode) stop(t *testing.T) {
	t.Helper()
	if n.cancel == nil {
		return
	}
	n.cancel()
	require.NoError(t, <-n.serveCh)
	require.NoError(t, n.readDB.Close())
	require.NoError(t, n.store.Close())
	n.cancel = nil
}

func modelOverrideCTL(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	cmd := ctl.NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{"model"}, args...))
	err := cmd.ExecuteContext(t.Context())
	return append([]byte(nil), output.Bytes()...), err
}

func modelOverrideStatus(t *testing.T) protocol.ModelStatusResult {
	t.Helper()
	raw, err := modelOverrideCTL(t, "status")
	require.NoError(t, err)
	var result protocol.ModelStatusResult
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

func writeModelOverrideEvidence(
	t *testing.T,
	dir string,
	base protocol.ModelSelection,
) string {
	t.Helper()
	adapter := protocol.ModelAdapterMetadata{
		AdapterName: modelOverrideAdapterName, AdapterRevision: modelOverrideAdapterRevision,
		AdapterSHA256: modelOverrideAdapterSHA, TrainingRunID: modelOverrideTrainingRun,
		Contract: base.Profile.Contract, BaseModelRepo: base.BaseModelRepo,
		BaseModelRevision: base.BaseModelRevision, Runtime: base.Runtime,
		Quantization: base.Quantization, PromptVersion: base.PromptVersion,
		ProjectionPolicyVersion: base.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  base.ProjectionPolicySHA256,
		ProfileRevision:         base.ProfileRevision, ProfileSHA256: base.ProfileSHA256,
		CalibrationRevision:    base.CalibrationRevision,
		CalibrationSHA256:      base.CalibrationSHA256,
		CalibrationTemperature: base.CalibrationTemperature,
		IdleThreshold:          base.IdleThreshold,
		AnalysisPolicyVersion:  base.AnalysisPolicyVersion,
	}
	ended := time.Now().UTC()
	training := protocol.ModelTrainingRunMetadata{
		TrainingRunID:     modelOverrideTrainingRun,
		DatasetVersion:    "synthetic-dataset-v1",
		DatasetSHA256:     protocol.SHA256Hex(strings.Repeat("d", 64)),
		MLContractVersion: base.MLContractVersion, Contract: base.Profile.Contract,
		BaseModelRepo: base.BaseModelRepo, BaseModelRevision: base.BaseModelRevision,
		Method: "lora", Hyperparameters: protocol.Payload(`{"rank":8}`), Seed: 7,
		TrainingCodeRevision: "synthetic-training-v1", ReadinessStatus: "passed",
		StartedAt: ended.Add(-time.Minute), EndedAt: &ended, Status: "completed",
	}
	baseRun := eval.RunProvenance{
		PromptVersion: base.PromptVersion, BaseModelRepo: base.BaseModelRepo,
		BaseModelRevision: base.BaseModelRevision, Runtime: base.Runtime,
		RuntimeVersion: "synthetic-runtime-v1", Quantization: base.Quantization,
		GPU: "synthetic-cpu", PredictConcurrency: 1,
	}
	loraRun := baseRun
	loraRun.AdapterName = adapter.AdapterName
	loraRun.AdapterRevision = adapter.AdapterRevision
	loraRun.AdapterSHA256 = adapter.AdapterSHA256
	loraRun.TrainingRunID = adapter.TrainingRunID
	report := eval.Report{
		Inputs: []eval.Input{
			{Profile: eval.ProfileBase, Run: baseRun, RunRecorded: true},
			{Profile: eval.ProfileLoRA, Run: loraRun, RunRecorded: true},
		},
		Profiles: []eval.ProfileReport{
			{Profile: eval.ProfileBase, Role: string(eval.ProfileBase)},
			{Profile: eval.ProfileLoRA, Role: string(eval.ProfileLoRA)},
		},
	}

	adapterRef := writeModelOverrideJSON(t, dir, "adapter.json", adapter)
	trainingRef := writeModelOverrideJSON(t, dir, "training.json", training)
	reportRef := writeModelOverrideJSON(t, dir, "report.json", report)
	request := protocol.ActivateAdapterOverrideRequest{
		AdapterMetadata: protocol.ModelEvidenceReference{
			Path: filepath.Base(adapterRef.Path), SHA256: adapterRef.SHA256,
		},
		TrainingRunMetadata: protocol.ModelEvidenceReference{
			Path: filepath.Base(trainingRef.Path), SHA256: trainingRef.SHA256,
		},
		EvaluationReport: protocol.ModelEvidenceReference{
			Path: filepath.Base(reportRef.Path), SHA256: reportRef.SHA256,
		},
		Reason: "synthetic e2e verification",
	}
	require.NoError(t, request.Validate())
	manifest := filepath.Join(dir, "evidence.json")
	raw, err := json.Marshal(request)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifest, raw, 0o600))
	return manifest
}

func writeModelOverrideJSON(t *testing.T, dir, name string, value any) protocol.ModelEvidenceReference {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	digest := sha256.Sum256(raw)
	return protocol.ModelEvidenceReference{
		Path: path, SHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
}

func modelOverrideAnalyze(
	t *testing.T,
	node *modelOverrideNode,
	agent protocol.AgentID,
) protocol.InferResult {
	t.Helper()
	result, err := node.wiring.model.Infer(t.Context(), protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  agent,
		Payload:  protocol.Payload(`{"harness":"codex","seconds_since_last_output":0,"output_bytes":{"last_500ms":0,"last_2s":0,"last_10s":0},"new_child_processes":[],"previous_state":"","previous_state_age_seconds":0,"screen_lines":[]}`),
	})
	require.NoError(t, err)
	return result
}

// TestNodeModelActivateOverrideEndToEnd proves that one node accepts explicit
// synthetic override evidence only after its real Gateway confirms the served
// tuple, persists that selection over a restart, and keeps a failed probe from
// changing the durable selection.
func TestNodeModelActivateOverrideEndToEnd(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	databasePath := filepath.Join(t.TempDir(), "node.db")
	runtimeDir := filepath.Join(runtimeHome, "gifford")
	glimmer := &modelOverrideGlimmer{}
	endpoint := httptest.NewServer(glimmer)
	t.Cleanup(endpoint.Close)
	cfg := configuredAutomaticTestConfig(t, endpoint.URL)

	node := startModelOverrideNode(t, databasePath, runtimeDir, cfg)
	t.Cleanup(func() { node.stop(t) })
	baseStatus := modelOverrideStatus(t)
	require.NotNil(t, baseStatus.Selected)
	require.Equal(t, protocol.InferenceProfileBase, baseStatus.Selected.Profile.InferenceProfile)
	evidencePath := writeModelOverrideEvidence(t, t.TempDir(), *baseStatus.Selected)

	raw, err := modelOverrideCTL(t, "activate-override", evidencePath)
	require.NoError(t, err)
	var activated protocol.ModelActivationResult
	require.NoError(t, json.Unmarshal(raw, &activated))
	require.True(t, activated.Applied)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, activated.Selection.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusOverride, activated.Selection.GateStatus)
	require.Equal(t, modelOverrideAdapterName, activated.Selection.Profile.AdapterName)
	require.Equal(t, modelOverrideAdapterRevision, activated.Selection.Profile.AdapterRevision)

	analyzed := modelOverrideAnalyze(t, node, "agent-override-e2e")
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, analyzed.InferenceProfile)
	require.Equal(t, protocol.SHA256Hex(modelOverrideAdapterSHA), analyzed.CallRecord.AdapterSHA256)
	var storedProfile, storedAdapterSHA string
	require.NoError(t, node.store.DB().QueryRowContext(t.Context(), `SELECT
		inference_profile, COALESCE(adapter_sha256, '') FROM glimmer_calls
		WHERE agent_id = ? ORDER BY created_at DESC LIMIT 1`, "agent-override-e2e").Scan(
		&storedProfile, &storedAdapterSHA,
	))
	require.Equal(t, string(protocol.InferenceProfileAnalystLoRA), storedProfile)
	require.Equal(t, modelOverrideAdapterSHA, storedAdapterSHA)

	selectedBeforeRestart := activated.Selection
	node.stop(t)
	node = startModelOverrideNode(t, databasePath, runtimeDir, cfg)
	restarted := modelOverrideStatus(t)
	require.NotNil(t, restarted.Selected)
	require.Equal(t, selectedBeforeRestart, *restarted.Selected)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, restarted.Selected.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusOverride, restarted.Selected.GateStatus)

	raw, err = modelOverrideCTL(t, "rollback")
	require.NoError(t, err)
	var rolledBack protocol.ModelActivationResult
	require.NoError(t, json.Unmarshal(raw, &rolledBack))
	require.True(t, rolledBack.Applied)
	require.Equal(t, protocol.InferenceProfileBase, rolledBack.Selection.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusBase, rolledBack.Selection.GateStatus)
	baseCall := modelOverrideAnalyze(t, node, "agent-base-e2e")
	require.Equal(t, protocol.InferenceProfileBase, baseCall.InferenceProfile)
	require.Empty(t, baseCall.CallRecord.AdapterSHA256)

	beforeRejected := modelOverrideStatus(t)
	require.NotNil(t, beforeRejected.Selected)
	glimmer.setQuantization("q4")
	_, err = modelOverrideCTL(t, "activate-override", evidencePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Model profile unavailable")
	afterRejected := modelOverrideStatus(t)
	require.NotNil(t, afterRejected.Selected)
	require.Equal(t, *beforeRejected.Selected, *afterRejected.Selected,
		"a served quantization mismatch must not mutate the selection")

	requests, endpointErrors := glimmer.snapshot()
	require.Empty(t, endpointErrors)
	require.Len(t, requests, 4)
	for _, index := range []int{0, 1, 3} {
		require.Equal(t, protocol.ModelContractAnalyzeAgentState, requests[index].Contract)
		require.Equal(t, protocol.InferenceProfileAnalystLoRA, requests[index].InferenceProfile)
		require.Equal(t, modelOverrideAdapterName, requests[index].AdapterName)
		require.Equal(t, modelOverrideAdapterRevision, requests[index].AdapterRevision)
	}
	require.Equal(t, protocol.InferenceProfileBase, requests[2].InferenceProfile)
	require.Empty(t, requests[2].AdapterName)
	require.Empty(t, requests[2].AdapterRevision)
}
