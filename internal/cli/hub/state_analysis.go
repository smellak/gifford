package hub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/triage"
)

const automaticPolicy = "automatic-5.8"

// automaticDecisionWiring is the all-or-nothing model wiring for the state
// producer and executor. producer always exists; the other fields stay nil
// when state_analysis is absent so the executor keeps its deterministic
// fail-closed policy.
type automaticDecisionWiring struct {
	producer      *orchestrator.StateProducer
	model         orchestrator.Inferrer
	triage        orchestrator.Triager
	states        orchestrator.AutomaticStates
	stateProfile  *orchestrator.StateAnalysisProfile
	policyVersion string
}

// newAutomaticDecisionWiring activates the node's unique state_snapshots
// writer and, only with state_analysis configured, shares one Gateway between
// ANALYZE, TRIAGE and CHOOSE. No partially configured executor is returned.
func newAutomaticDecisionWiring(
	cfg config.Config,
	db gatewayDB,
	controller *channel.Controller,
	engine *channel.Engine,
	clk agentlink.Clock,
	persistence *orchestrator.PersistenceGate,
	errW io.Writer,
) (*automaticDecisionWiring, error) {
	stateAnalyst, model, profile, err := buildStateAnalyst(cfg, db, clk)
	if err != nil {
		return nil, err
	}
	producerProfile := orchestrator.StateAnalysisProfile{}
	if profile != nil {
		producerProfile = *profile
	}
	producer, err := orchestrator.NewStateProducer(orchestrator.StateProducerOptions{
		DB: db.DB(), Clock: clk, Channel: controller, Analyzer: stateAnalyst,
		Profile: producerProfile, Persistence: persistence,
		ErrorSink: func(err error) { sayf(errW, "State producer: %v", err) },
	})
	if err != nil {
		return nil, fmt.Errorf("new state producer: %w", err)
	}
	wiring := &automaticDecisionWiring{producer: producer}
	if model == nil {
		return wiring, nil
	}
	triageChannel, err := triage.NewChannel(engine)
	if err != nil {
		return nil, fmt.Errorf("new triage channel: %w", err)
	}
	messageTriage, err := triage.New(db.DB(), model, triageChannel, clk)
	if err != nil {
		return nil, fmt.Errorf("new message triage: %w", err)
	}
	policyVersion, err := automaticPolicyVersion(cfg.StateAnalysis.AnalyzePrompt, *profile)
	if err != nil {
		return nil, err
	}
	wiring.model = model
	wiring.triage = messageTriage
	wiring.states = producer
	wiring.stateProfile = profile
	wiring.policyVersion = policyVersion
	return wiring, nil
}

func buildStateAnalyst(
	cfg config.Config,
	db gatewayDB,
	clk agentlink.Clock,
) (orchestrator.StateAnalyzer, *gateway.Gateway, *orchestrator.StateAnalysisProfile, error) {
	configured := cfg.StateAnalysis
	if configured == nil {
		return nil, nil, nil, nil
	}
	file, err := os.Open(configured.GatewayConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open gateway config: %w", err)
	}
	gatewayConfig, parseErr := gateway.ParseConfig(file, os.Getenv)
	closeErr := file.Close()
	if parseErr != nil {
		return nil, nil, nil, fmt.Errorf("parse gateway config: %w", parseErr)
	}
	if closeErr != nil {
		return nil, nil, nil, fmt.Errorf("close gateway config: %w", closeErr)
	}
	transport, err := gateway.NewHTTPTransport(gatewayConfig.Endpoint, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new gateway transport: %w", err)
	}
	redactor, err := redact.New(redact.Options{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new gateway redactor: %w", err)
	}
	recorder, err := gateway.NewSQLRecorder(db.DB())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new gateway recorder: %w", err)
	}
	modelGateway, err := gateway.New(gatewayConfig, gateway.Deps{
		Transport: transport, Redactor: redactor, Recorder: recorder,
		Clock: clk, IDs: gateway.RandomCallIDs{},
		Prompts: analyst.Prompts{Analyze: configured.AnalyzePrompt},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new model gateway: %w", err)
	}
	policies, err := cfg.ML.ProjectionPolicies()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load analyst projection: %w", err)
	}
	stateAnalyst, err := analyst.NewWithPromptVersion(modelGateway, policies, configured.AnalyzePrompt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new state analyst: %w", err)
	}
	profile := &orchestrator.StateAnalysisProfile{
		PromptVersion: configured.AnalyzePrompt,
		Revision:      configured.ProfileRevision, SHA256: configured.ProfileSHA256,
		CalibrationRevision: configured.CalibrationRevision,
		CalibrationSHA256:   configured.CalibrationSHA256,
		Temperature:         configured.Temperature, IdleThreshold: configured.IdleThreshold,
	}
	return stateAnalyst, modelGateway, profile, nil
}

// automaticPolicyTuple binds every orchestration and prompt revision plus the
// full state profile/calibration tuple fixed at config load. Its declared JSON
// field order is the canonical order hashed for the policy version.
type automaticPolicyTuple struct {
	Policy              string  `json:"policy"`
	AnalyzePrompt       string  `json:"analyze_prompt"`
	TriagePrompt        string  `json:"triage_prompt"`
	ChoosePrompt        string  `json:"choose_prompt"`
	ProfileRevision     string  `json:"profile_revision"`
	ProfileSHA256       string  `json:"profile_sha256"`
	CalibrationRevision string  `json:"calibration_revision"`
	CalibrationSHA256   string  `json:"calibration_sha256"`
	Temperature         float64 `json:"temperature"`
	IdleThreshold       float64 `json:"idle_threshold"`
}

func newAutomaticPolicyTuple(analyzePrompt string, profile orchestrator.StateAnalysisProfile) automaticPolicyTuple {
	return automaticPolicyTuple{
		Policy: automaticPolicy, AnalyzePrompt: analyzePrompt,
		TriagePrompt: analyst.TriagePromptVersion, ChoosePrompt: analyst.ChoosePromptVersion,
		ProfileRevision: profile.Revision, ProfileSHA256: string(profile.SHA256),
		CalibrationRevision: profile.CalibrationRevision,
		CalibrationSHA256:   string(profile.CalibrationSHA256),
		Temperature:         profile.Temperature, IdleThreshold: profile.IdleThreshold,
	}
}

func automaticPolicyVersion(analyzePrompt string, profile orchestrator.StateAnalysisProfile) (string, error) {
	return hashAutomaticPolicyTuple(newAutomaticPolicyTuple(analyzePrompt, profile))
}

func hashAutomaticPolicyTuple(tuple automaticPolicyTuple) (string, error) {
	canonical, err := json.Marshal(tuple)
	if err != nil {
		return "", fmt.Errorf("encode automatic policy tuple: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return automaticPolicy + ":" + hex.EncodeToString(digest[:])[:24], nil
}

type gatewayDB interface {
	DB() *sql.DB
}

type stateProducerLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startStateProducerLoop(parent context.Context, producer *orchestrator.StateProducer) *stateProducerLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &stateProducerLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		_ = producer.Run(ctx)
	}()
	return loop
}

func (l *stateProducerLoop) stop() {
	l.cancel()
	<-l.done
}
