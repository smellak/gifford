package hub

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testGatewayURLEnv                   = "GIFFORD_TEST_GATEWAY_URL"
	testGatewayTokenEnv                 = "GIFFORD_TEST_GATEWAY_TOKEN"
	testProfileSHA                      = "1111111111111111111111111111111111111111111111111111111111111111"
	testCalibrationSHA                  = "2222222222222222222222222222222222222222222222222222222222222222"
	testProfileRevision                 = "profile-v1"
	testCalibrationRev                  = "calibration-v1"
	testVersionSuffix                   = ".next"
	automaticTestNodeID protocol.NodeID = "node-a"
)

type automaticTestNode struct {
	store      *eventstore.Store
	engine     *channel.Engine
	controller *channel.Controller
	clock      agentlink.SystemClock
}

func newAutomaticTestNode(t *testing.T) automaticTestNode {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), dbName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := agentlink.SystemClock{}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: automaticTestNodeID,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	return automaticTestNode{store: store, engine: engine, controller: controller, clock: clk}
}

func (n automaticTestNode) build(t *testing.T, cfg config.Config) (*automaticDecisionWiring, error) {
	t.Helper()
	return newAutomaticDecisionWiring(
		cfg, n.store, n.controller, n.engine, n.clock,
		orchestrator.NewPersistenceGate(), io.Discard,
	)
}

func TestAutomaticDecisionWiringAbsentStaysFailClosed(t *testing.T) {
	node := newAutomaticTestNode(t)
	wiring, err := node.build(t, config.DefaultConfig())
	require.NoError(t, err)
	require.NotNil(t, wiring.producer, "the node always has its unique state writer")
	require.Nil(t, wiring.model)
	require.Nil(t, wiring.triage)
	require.Nil(t, wiring.states)
	require.Nil(t, wiring.stateProfile)
	require.Empty(t, wiring.policyVersion, "NewExecutor applies deterministic-5.3c")
}

func TestAutomaticDecisionWiringConfiguredSharesOneGateway(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(endpoint.Close)
	cfg := configuredAutomaticTestConfig(t, endpoint.URL)
	node := newAutomaticTestNode(t)

	wiring, err := node.build(t, cfg)
	require.NoError(t, err)
	require.NotNil(t, wiring.producer)
	require.IsType(t, &gateway.Gateway{}, wiring.model)
	require.NotNil(t, wiring.triage)
	require.Same(t, wiring.producer, wiring.states)
	require.Equal(t, &orchestrator.StateAnalysisProfile{
		Revision: testProfileRevision, SHA256: testProfileSHA,
		CalibrationRevision: testCalibrationRev, CalibrationSHA256: testCalibrationSHA,
		Temperature: 1, IdleThreshold: 0.9,
	}, wiring.stateProfile)
	require.Regexp(t, `^automatic-5\.8:[0-9a-f]{24}$`, wiring.policyVersion)
	require.Len(t, wiring.policyVersion, 38)

	again, err := node.build(t, cfg)
	require.NoError(t, err)
	require.Equal(t, wiring.policyVersion, again.policyVersion)
}

func TestNodeModelSelectionCompatibilityBindsConfiguredTuple(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(endpoint.Close)
	node := newAutomaticTestNode(t)
	wiring, err := node.build(t, configuredAutomaticTestConfig(t, endpoint.URL))
	require.NoError(t, err)
	base := wiring.selector.CurrentModelSelection()
	require.NoError(t, validateNodeModelSelection(base, base))

	incompatible := map[string]func(*protocol.ModelSelection){
		"base model repo":     func(v *protocol.ModelSelection) { v.BaseModelRepo += testVersionSuffix },
		"base model revision": func(v *protocol.ModelSelection) { v.BaseModelRevision += testVersionSuffix },
		"runtime":             func(v *protocol.ModelSelection) { v.Runtime += testVersionSuffix },
		"quantization":        func(v *protocol.ModelSelection) { v.Quantization = "q4" },
		"ML contract":         func(v *protocol.ModelSelection) { v.MLContractVersion += testVersionSuffix },
		"prompt":              func(v *protocol.ModelSelection) { v.PromptVersion = "unknown-prompt" },
		"projection revision": func(v *protocol.ModelSelection) { v.ProjectionPolicyVersion += testVersionSuffix },
		"projection digest":   func(v *protocol.ModelSelection) { v.ProjectionPolicySHA256 = testProfileSHA },
	}
	for name, mutate := range incompatible {
		t.Run(name, func(t *testing.T) {
			selected := base
			mutate(&selected)
			require.Error(t, validateNodeModelSelection(base, selected))
		})
	}

	candidate := base
	candidate.ProfileRevision += testVersionSuffix
	candidate.ProfileSHA256 = testCalibrationSHA
	candidate.CalibrationRevision += testVersionSuffix
	candidate.CalibrationSHA256 = testProfileSHA
	candidate.CalibrationTemperature = 2
	candidate.IdleThreshold = 0.8
	candidate.AnalysisPolicyVersion += testVersionSuffix
	require.NoError(t, validateNodeModelSelection(base, candidate),
		"an evaluated candidate owns its profile, calibration, and analysis policy tuple")
}

func TestAutomaticPolicyVersionBindsEveryTupleField(t *testing.T) {
	profile := orchestrator.StateAnalysisProfile{
		Revision: testProfileRevision, SHA256: testProfileSHA,
		CalibrationRevision: testCalibrationRev, CalibrationSHA256: testCalibrationSHA,
		Temperature: 1, IdleThreshold: 0.9,
	}
	base := newAutomaticPolicyTuple(profile)
	want, err := hashAutomaticPolicyTuple(base)
	require.NoError(t, err)

	tests := map[string]func(*automaticPolicyTuple){
		"policy":               func(v *automaticPolicyTuple) { v.Policy += testVersionSuffix },
		"analyze prompt":       func(v *automaticPolicyTuple) { v.AnalyzePrompt += testVersionSuffix },
		"triage prompt":        func(v *automaticPolicyTuple) { v.TriagePrompt += testVersionSuffix },
		"choose prompt":        func(v *automaticPolicyTuple) { v.ChoosePrompt += testVersionSuffix },
		"profile revision":     func(v *automaticPolicyTuple) { v.ProfileRevision += testVersionSuffix },
		"profile digest":       func(v *automaticPolicyTuple) { v.ProfileSHA256 = testCalibrationSHA },
		"calibration revision": func(v *automaticPolicyTuple) { v.CalibrationRevision += testVersionSuffix },
		"calibration digest":   func(v *automaticPolicyTuple) { v.CalibrationSHA256 = testProfileSHA },
		"temperature":          func(v *automaticPolicyTuple) { v.Temperature = 2 },
		"idle threshold":       func(v *automaticPolicyTuple) { v.IdleThreshold = 0.8 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, hashErr := hashAutomaticPolicyTuple(changed)
			require.NoError(t, hashErr)
			require.NotEqual(t, want, got)
		})
	}
}

func TestAutomaticDecisionWiringRejectsInvalidGatewayConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte("not_a_gateway: true\n"), 0o600))
	cfg := config.DefaultConfig()
	cfg.StateAnalysis = &config.StateAnalysis{
		GatewayConfig: path, ProfileRevision: testProfileRevision, ProfileSHA256: testProfileSHA,
		CalibrationRevision: testCalibrationRev, CalibrationSHA256: testCalibrationSHA,
		Temperature: 1, IdleThreshold: 0.9,
	}
	node := newAutomaticTestNode(t)
	_, err := node.build(t, cfg)
	require.ErrorIs(t, err, gateway.ErrInvalidConfig)
}

func configuredAutomaticTestConfig(t *testing.T, endpoint string) config.Config {
	t.Helper()
	t.Setenv(testGatewayURLEnv, endpoint)
	t.Setenv(testGatewayTokenEnv, "test-token")
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`endpoint:
  url_env: `+testGatewayURLEnv+`
  token_env: `+testGatewayTokenEnv+`
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
	cfg := config.DefaultConfig()
	cfg.NodeID = automaticTestNodeID
	cfg.ML = config.ML{ScreenLines: 20, Mask: "none"}
	cfg.StateAnalysis = &config.StateAnalysis{
		GatewayConfig: path, ProfileRevision: testProfileRevision, ProfileSHA256: testProfileSHA,
		CalibrationRevision: testCalibrationRev, CalibrationSHA256: testCalibrationSHA,
		Temperature: 1, IdleThreshold: 0.9,
	}
	return cfg
}
