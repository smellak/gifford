package modelprofile

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type testPublisher struct {
	mu      sync.Mutex
	current protocol.ModelSelection
}

func (p *testPublisher) CurrentModelSelection() protocol.ModelSelection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *testPublisher) Publish(next protocol.ModelSelection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if next.Generation <= p.current.Generation {
		return errors.New("generation did not advance")
	}
	p.current = next
	return nil
}

type testVerifier struct {
	err  error
	seen []protocol.ModelSelection
}

func (v *testVerifier) VerifyModelSelection(_ context.Context, selection protocol.ModelSelection) error {
	v.seen = append(v.seen, selection)
	return v.err
}

type failingEvents struct{ err error }

func (f failingEvents) NextEventSequenceTx(context.Context, *sql.Tx, protocol.OpaqueID) (int64, error) {
	return 2, nil
}

func (f failingEvents) AppendEventsTx(context.Context, *sql.Tx, []protocol.EventEnvelope) error {
	return f.err
}

type serviceFixture struct {
	store     *eventstore.Store
	clock     testClock
	base      protocol.ModelSelection
	publisher *testPublisher
	verifier  *testVerifier
	service   *Service
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	fixture := &serviceFixture{
		store:    store,
		clock:    testClock{now: time.Date(2026, time.November, 1, 17, 0, 0, 0, time.UTC)},
		base:     validBase(),
		verifier: &testVerifier{},
	}
	opts := fixture.options(store)
	selected, err := Initialize(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, int64(1), selected.Generation)
	fixture.publisher = &testPublisher{current: selected}
	opts.Publisher = fixture.publisher
	opts.Verifier = fixture.verifier
	fixture.service, err = New(opts)
	require.NoError(t, err)
	return fixture
}

func (f *serviceFixture) options(events EventStore) Options {
	return Options{
		DB: f.store.DB(), Events: events, NodeID: "node-a", Clock: f.clock,
		Base: f.base,
		Validate: func(selection protocol.ModelSelection) error {
			if selection.PromptVersion != f.base.PromptVersion ||
				selection.ProjectionPolicyVersion != f.base.ProjectionPolicyVersion ||
				selection.ProjectionPolicySHA256 != f.base.ProjectionPolicySHA256 ||
				selection.ProfileRevision != f.base.ProfileRevision ||
				selection.ProfileSHA256 != f.base.ProfileSHA256 ||
				selection.CalibrationRevision != f.base.CalibrationRevision ||
				selection.CalibrationSHA256 != f.base.CalibrationSHA256 ||
				selection.CalibrationTemperature != f.base.CalibrationTemperature ||
				selection.IdleThreshold != f.base.IdleThreshold ||
				selection.AnalysisPolicyVersion != f.base.AnalysisPolicyVersion {
				return errors.New("incompatible tuple")
			}
			return nil
		},
	}
}

func validBase() protocol.ModelSelection {
	return protocol.ModelSelection{
		Profile: protocol.GatewayProfile{
			Contract: protocol.ModelContractAnalyzeAgentState, InferenceProfile: protocol.InferenceProfileBase,
		},
		BaseModelRepo: "example.invalid/model", BaseModelRevision: "base-1", Runtime: "vllm",
		MLContractVersion: "ml-1", PromptVersion: "analyze_agent_state.v2",
		ProjectionPolicyVersion: "projection-1", ProjectionPolicySHA256: digest("1"),
		ProfileRevision: "profile-1", ProfileSHA256: digest("2"),
		CalibrationRevision: "calibration-1", CalibrationSHA256: digest("3"),
		CalibrationTemperature: 1.25, IdleThreshold: 0.6, AnalysisPolicyVersion: "analysis-1",
	}
}

func validRegistration() protocol.RegisterModelMetadataRequest {
	report := protocol.Payload(`{"metrics":{"macro_f1":0.9}}`)
	sum := sha256.Sum256(report)
	gate := make([]protocol.ModelGateCriterion, 6)
	for i := range gate {
		gate[i] = protocol.ModelGateCriterion{ID: i, Name: "criterion", Verdict: "PASS"}
	}
	base := validBase()
	return protocol.RegisterModelMetadataRequest{
		AdapterName: "analyst", AdapterRevision: "rev-1", AdapterSHA256: digest("4"),
		Contract:      protocol.ModelContractAnalyzeAgentState,
		BaseModelRepo: base.BaseModelRepo, BaseModelRevision: base.BaseModelRevision,
		Runtime: base.Runtime, Quantization: "bf16", MLContractVersion: base.MLContractVersion,
		DatasetVersion: "dataset-1", DatasetSHA256: digest("5"),
		DatasetReadinessStatus: "passed", DatasetReadinessSHA256: digest("6"),
		TrainingRunID: "training-1", TrainingCodeRevision: "code-1",
		TrainingReadinessStatus: "passed", TrainingStatus: "completed",
		EvaluationRunID: "evaluation-1", EvaluationSHA256: protocol.SHA256Hex(hex.EncodeToString(sum[:])),
		EvaluationReport: report, Gate: gate,
		PromptVersion:           base.PromptVersion,
		ProjectionPolicyVersion: base.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  base.ProjectionPolicySHA256,
		ProfileRevision:         base.ProfileRevision, ProfileSHA256: base.ProfileSHA256,
		CalibrationRevision: base.CalibrationRevision, CalibrationSHA256: base.CalibrationSHA256,
		CalibrationTemperature: base.CalibrationTemperature, IdleThreshold: base.IdleThreshold,
		AnalysisPolicyVersion: base.AnalysisPolicyVersion,
	}
}

func digest(char string) protocol.SHA256Hex { return protocol.SHA256Hex(strings.Repeat(char, 64)) }

func operator() channel.Actor { return channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"} }

func TestRegisterActivateRollbackAndRestart(t *testing.T) {
	fixture := newServiceFixture(t)
	req := validRegistration()
	registered, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	require.True(t, registered.Imported)
	require.Equal(t, req.AdapterName, registered.AdapterName)

	again, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	require.False(t, again.Imported)
	require.Equal(t, registered.RegisteredAt, again.RegisteredAt)

	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.True(t, activated.Applied)
	require.Equal(t, int64(2), activated.Selection.Generation)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, activated.Selection.Profile.InferenceProfile)
	require.Equal(t, req.AdapterSHA256, activated.Selection.AdapterSHA256)
	require.Equal(t, req.DatasetSHA256, activated.Selection.DatasetSHA256)
	require.Equal(t, req.EvaluationSHA256, activated.Selection.EvaluationSHA256)
	require.Len(t, fixture.verifier.seen, 1)
	require.Equal(t, activated.Selection, fixture.publisher.CurrentModelSelection())

	var active int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Equal(t, int64(1), active)
	var events int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events
		WHERE event_type = ?`, protocol.EventTypeControlModelProfileChanged).Scan(&events))
	require.Equal(t, int64(2), events, "startup and activation are both durable changes")

	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, activated.Selection, restarted)

	noOp, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.False(t, noOp.Applied)
	require.Equal(t, int64(2), noOp.Selection.Generation)

	rolledBack, err := fixture.service.Rollback(t.Context(), protocol.RollbackToBaseRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
	}, operator())
	require.NoError(t, err)
	require.True(t, rolledBack.Applied)
	require.Equal(t, int64(3), rolledBack.Selection.Generation)
	require.Equal(t, protocol.InferenceProfileBase, rolledBack.Selection.Profile.InferenceProfile)
	require.Empty(t, rolledBack.Selection.AdapterSHA256)
	require.Empty(t, rolledBack.Selection.DatasetVersion)
	require.Empty(t, rolledBack.Selection.TrainingRunID)
	require.Empty(t, rolledBack.Selection.EvaluationRunID)
	require.Equal(t, fixture.base.BaseModelRevision, rolledBack.Selection.BaseModelRevision)
	require.Equal(t, fixture.base.PromptVersion, rolledBack.Selection.PromptVersion)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Zero(t, active)
}

func TestRegisteredMetadataIsImmutableInStorage(t *testing.T) {
	fixture := newServiceFixture(t)
	req := validRegistration()
	_, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_profile_registry
		SET prompt_version = 'changed' WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision)
	require.ErrorContains(t, err, "model_profile_registry is immutable")
	_, err = fixture.store.DB().ExecContext(t.Context(), `DELETE FROM model_profile_registry
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.ErrorContains(t, err, "model_profile_registry is immutable")

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_adapters SET gate_status = 'failed'
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.NoError(t, err)
	_, err = fixture.service.Register(t.Context(), req, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid,
		"an idempotent import still verifies the mutable serving binding")
}

func TestRegisterRejectsIncompleteOrUntrustedEvidence(t *testing.T) {
	tests := map[string]func(*protocol.RegisterModelMetadataRequest){
		"gate failure": func(req *protocol.RegisterModelMetadataRequest) { req.Gate[3].Verdict = "FAIL" },
		"missing criterion": func(req *protocol.RegisterModelMetadataRequest) {
			req.Gate[5].ID = 4
		},
		"evaluation digest":   func(req *protocol.RegisterModelMetadataRequest) { req.EvaluationSHA256 = digest("9") },
		"dataset readiness":   func(req *protocol.RegisterModelMetadataRequest) { req.DatasetReadinessStatus = "failed" },
		"underpowered":        func(req *protocol.RegisterModelMetadataRequest) { req.TrainingReadinessStatus = "underpowered" },
		"training incomplete": func(req *protocol.RegisterModelMetadataRequest) { req.TrainingStatus = "running" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newServiceFixture(t)
			req := validRegistration()
			mutate(&req)
			_, err := fixture.service.Register(t.Context(), req, operator())
			require.ErrorIs(t, err, ErrRegistryInvalid)
			var count int64
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT COUNT(*) FROM model_profile_registry`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestActivateRejectsMissingIncompatibleAndUnservedCandidates(t *testing.T) {
	fixture := newServiceFixture(t)
	initial := fixture.publisher.CurrentModelSelection()
	_, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: "missing", AdapterRevision: "rev-1",
	}, operator())
	require.ErrorIs(t, err, ErrAdapterNotRegistered)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())

	req := validRegistration()
	req.PromptVersion = "unknown-prompt"
	_, err = fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.Empty(t, fixture.verifier.seen)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())

	compatible := validRegistration()
	compatible.AdapterRevision = "rev-2"
	compatible.AdapterSHA256 = digest("7")
	_, err = fixture.service.Register(t.Context(), compatible, operator())
	require.NoError(t, err)
	fixture.verifier.err = errors.New("not loaded")
	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: compatible.AdapterName, AdapterRevision: compatible.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrProfileNotServed)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())
	var generation int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT generation
		FROM model_profile_selections WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&generation))
	require.Equal(t, int64(1), generation)
}

func TestPersistenceFailureLeavesMemoryAndAdapterFlagsUnchanged(t *testing.T) {
	fixture := newServiceFixture(t)
	req := validRegistration()
	_, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	broken := fixture.options(failingEvents{err: errors.New("event write failed")})
	broken.Publisher = fixture.publisher
	broken.Verifier = fixture.verifier
	service, err := New(broken)
	require.NoError(t, err)
	initial := fixture.publisher.CurrentModelSelection()
	_, err = service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.ErrorContains(t, err, "append model selection event")
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())
	var active, generation int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Zero(t, active)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT generation
		FROM model_profile_selections WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&generation))
	require.Equal(t, int64(1), generation)
}

func TestAutomaticFallbackOnlyRollsBackItsCapturedGeneration(t *testing.T) {
	fixture := newServiceFixture(t)
	first := validRegistration()
	_, err := fixture.service.Register(t.Context(), first, operator())
	require.NoError(t, err)
	firstActivation, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: first.AdapterName, AdapterRevision: first.AdapterRevision,
	}, operator())
	require.NoError(t, err)

	second := validRegistration()
	second.AdapterRevision = "rev-2"
	second.AdapterSHA256 = digest("7")
	_, err = fixture.service.Register(t.Context(), second, operator())
	require.NoError(t, err)
	secondActivation, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: second.AdapterName, AdapterRevision: second.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(3), secondActivation.Selection.Generation)

	require.NoError(t, fixture.service.FallbackToBase(
		t.Context(), firstActivation.Selection.Generation, "stale adapter failure",
	))
	require.Equal(t, secondActivation.Selection, fixture.publisher.CurrentModelSelection(),
		"an old in-flight failure cannot roll back a newer activation")

	require.NoError(t, fixture.service.FallbackToBase(
		t.Context(), secondActivation.Selection.Generation, "current adapter failure",
	))
	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, int64(4), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
}

func TestInitializeDistrustsCorruptAdapterSelectionAndFallsBackDegraded(t *testing.T) {
	fixture := newServiceFixture(t)
	req := validRegistration()
	_, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(2), activated.Selection.Generation)
	corrupt := activated.Selection
	corrupt.AdapterSHA256 = digest("9")
	raw, err := json.Marshal(corrupt)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_profile_selections
		SET selection_json = ? WHERE contract = ?`, raw, protocol.ModelContractAnalyzeAgentState)
	require.NoError(t, err)

	selected, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, int64(3), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
	require.Contains(t, selected.Reason, "startup selection validation failed")
}

func TestInitializeDistrustsInconsistentAdapterFlags(t *testing.T) {
	fixture := newServiceFixture(t)
	req := validRegistration()
	_, err := fixture.service.Register(t.Context(), req, operator())
	require.NoError(t, err)
	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(2), activated.Selection.Generation)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_adapters SET active = 0
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.NoError(t, err)

	selected, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, int64(3), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
}
