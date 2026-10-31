package hub

import (
	"context"
	"database/sql"
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
)

// newStateProducer always activates the node's unique state_snapshots writer.
// Without state_analysis configuration it records synthetic observations as
// honest UNKNOWN state and makes no model call. With configuration, exactly
// one Gateway instance serves ANALYZE_AGENT_STATE.
func newStateProducer(
	cfg config.Config,
	db gatewayDB,
	controller *channel.Controller,
	clk agentlink.Clock,
	persistence *orchestrator.PersistenceGate,
	errW io.Writer,
) (*orchestrator.StateProducer, error) {
	stateAnalyst, profile, err := buildStateAnalyst(cfg, db, clk)
	if err != nil {
		return nil, err
	}
	producer, err := orchestrator.NewStateProducer(orchestrator.StateProducerOptions{
		DB: db.DB(), Clock: clk, Channel: controller, Analyzer: stateAnalyst,
		Profile: profile, Persistence: persistence,
		ErrorSink: func(err error) { sayf(errW, "State producer: %v", err) },
	})
	if err != nil {
		return nil, fmt.Errorf("new state producer: %w", err)
	}
	return producer, nil
}

func buildStateAnalyst(
	cfg config.Config,
	db gatewayDB,
	clk agentlink.Clock,
) (orchestrator.StateAnalyzer, orchestrator.StateAnalysisProfile, error) {
	configured := cfg.StateAnalysis
	if configured == nil {
		return nil, orchestrator.StateAnalysisProfile{}, nil
	}
	file, err := os.Open(configured.GatewayConfig)
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("open gateway config: %w", err)
	}
	gatewayConfig, parseErr := gateway.ParseConfig(file, os.Getenv)
	closeErr := file.Close()
	if parseErr != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("parse gateway config: %w", parseErr)
	}
	if closeErr != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("close gateway config: %w", closeErr)
	}
	transport, err := gateway.NewHTTPTransport(gatewayConfig.Endpoint, nil)
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("new gateway transport: %w", err)
	}
	redactor, err := redact.New(redact.Options{})
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("new gateway redactor: %w", err)
	}
	recorder, err := gateway.NewSQLRecorder(db.DB())
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("new gateway recorder: %w", err)
	}
	modelGateway, err := gateway.New(gatewayConfig, gateway.Deps{
		Transport: transport, Redactor: redactor, Recorder: recorder,
		Clock: clk, IDs: gateway.RandomCallIDs{}, Prompts: analyst.Prompts{},
	})
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("new model gateway: %w", err)
	}
	policies, err := cfg.ML.ProjectionPolicies()
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("load analyst projection: %w", err)
	}
	stateAnalyst, err := analyst.New(modelGateway, policies)
	if err != nil {
		return nil, orchestrator.StateAnalysisProfile{}, fmt.Errorf("new state analyst: %w", err)
	}
	profile := orchestrator.StateAnalysisProfile{
		Revision: configured.ProfileRevision, SHA256: configured.ProfileSHA256,
		CalibrationRevision: configured.CalibrationRevision,
		CalibrationSHA256:   configured.CalibrationSHA256,
		Temperature:         configured.Temperature, IdleThreshold: configured.IdleThreshold,
	}
	return stateAnalyst, profile, nil
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
