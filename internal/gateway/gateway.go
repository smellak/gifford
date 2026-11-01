package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// Sentinel errors, one per way a call can fail. Infer wraps exactly one.
var (
	// ErrInvalidRequest means the request failed its validation, so no
	// call was made and no record was written.
	ErrInvalidRequest = errors.New("invalid infer request")
	// ErrRedactionFailed means the payload could not be redacted, so
	// nothing was sent.
	ErrRedactionFailed = errors.New("payload redaction failed")
	// ErrTimeout means the endpoint did not answer before the deadline.
	ErrTimeout = errors.New("glimmer call timed out")
	// ErrInvalidOutput means the endpoint's answer broke the contract.
	ErrInvalidOutput = errors.New("glimmer answer is invalid")
	// ErrCallFailed means the call failed for any other reason.
	ErrCallFailed = errors.New("glimmer call failed")
	// ErrRecordFailed means the call record could not be stored, so the
	// result is withheld.
	ErrRecordFailed = errors.New("call record not stored")
	// ErrCircuitOpen means the breaker is open after repeated endpoint
	// failures, so the endpoint was not called.
	ErrCircuitOpen = errors.New("glimmer circuit open")
	// ErrAgentBusy means the agent already has an Analyst call in flight.
	ErrAgentBusy = errors.New("analyst call already in flight for the agent")
)

// CallRecorder stores one call record, with the version of the prompt the
// call rendered, empty when it failed before rendering one. The version
// stays beside the record: the record's contract is frozen.
type CallRecorder interface {
	RecordCall(ctx context.Context, rec protocol.GlimmerCallRecord, promptVersion string) error
}

// Prompt is a rendered prompt template, as chat messages, and the
// version that names all of them together.
type Prompt struct {
	Version  string
	Messages []Message
}

// Message is one chat message of a prompt.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Chat roles a prompt may use.
const (
	RoleSystem = "system"
	RoleUser   = "user"
)

// maxPromptMessages bounds the messages of a prompt.
const maxPromptMessages = 8

// PromptRenderer renders a contract's versioned prompt template over its
// sanitised payload. A zero Prompt means the contract sends none.
type PromptRenderer interface {
	RenderPrompt(c protocol.ModelContract, sanitised []byte) (Prompt, error)
}

// VersionedPromptRenderer renders a selected ANALYZE prompt without mutating a
// shared renderer. RenderPrompt remains the compatibility path for callers
// without a control-plane selection.
type VersionedPromptRenderer interface {
	RenderPromptVersion(c protocol.ModelContract, version string, sanitised []byte) (Prompt, error)
}

// maxPromptBytes bounds a rendered prompt.
const maxPromptBytes = 256 << 10

// maxPromptVersion bounds the version that names a prompt.
const maxPromptVersion = 64

// validPromptVersion reports whether v can name a prompt.
func validPromptVersion(v string) bool {
	return len(v) <= maxPromptVersion && protocol.OpaqueID(v).Valid()
}

// CallIDs issues one fresh call id per call.
type CallIDs interface {
	NewCallID() protocol.OpaqueID
}

// Deps are the gateway's collaborators. Every one is required but Logger.
type Deps struct {
	Transport Transport
	Redactor  Redactor
	Recorder  CallRecorder
	Clock     Clock
	IDs       CallIDs
	Prompts   PromptRenderer
	// Selections supplies the committed ANALYZE tuple. Nil preserves the static
	// configuration path used by offline evaluation and older callers.
	Selections ModelSelectionProvider
	// Logger takes the gateway's warnings. Nil means slog.Default().
	Logger *slog.Logger
}

// Gateway runs model contracts against Glimmer. It does not decide: it
// returns the model's distribution to the Analyst.
type Gateway struct {
	cfg            Config
	deps           Deps
	breaker        breaker
	adapterBreaker breaker
	inFlight       inFlight
	fallbackMu     sync.RWMutex
	fallback       func(context.Context, int64, string) error
	// warnedQuantization is the last served quantization a mismatch was
	// logged for, so that one mismatch is logged once, not on every call.
	warnedQuantization atomic.Pointer[string]
}

// SetModelFallback installs the serialized control-plane fallback invoked when
// the currently selected adapter cannot be served. It does not reset either
// circuit breaker.
func (g *Gateway) SetModelFallback(fallback func(context.Context, int64, string) error) {
	g.fallbackMu.Lock()
	g.fallback = fallback
	g.fallbackMu.Unlock()
}

// CurrentModelSelection returns the tuple a new ANALYZE call will capture.
func (g *Gateway) CurrentModelSelection() protocol.ModelSelection {
	if g.deps.Selections == nil {
		return protocol.ModelSelection{}
	}
	return g.deps.Selections.CurrentModelSelection()
}

// New returns a gateway over a configuration from ParseConfig.
func New(cfg Config, deps Deps) (*Gateway, error) {
	if deps.Transport == nil || deps.Redactor == nil || deps.Recorder == nil || deps.Clock == nil || deps.IDs == nil || deps.Prompts == nil {
		return nil, fmt.Errorf("%w: a dependency is missing", ErrInvalidConfig)
	}
	if len(cfg.Profiles) != len(protocol.ModelContract("").EnumValues()) {
		return nil, fmt.Errorf("%w: not from ParseConfig", ErrInvalidConfig)
	}
	return &Gateway{cfg: cfg, deps: deps}, nil
}

// wireRequest is what leaves the node. Its payload is redacted.
type wireRequest struct {
	Contract         protocol.ModelContract    `json:"contract"`
	InferenceProfile protocol.InferenceProfile `json:"inference_profile"`
	AdapterName      string                    `json:"adapter_name,omitempty"`
	AdapterRevision  string                    `json:"adapter_revision,omitempty"`
	PromptVersion    string                    `json:"prompt_version,omitempty"`
	Messages         []Message                 `json:"messages,omitempty"`
	Payload          json.RawMessage           `json:"payload"`
}

// wireResponse is the endpoint's answer.
type wireResponse struct {
	Label         string             `json:"label"`
	Probabilities map[string]float64 `json:"probabilities"`
	Evidence      []string           `json:"evidence"`
	// LetterMass is the probability the contract's letters held at the
	// label position, before it was renormalised over them. A
	// TRIAGE_MESSAGE or CHOOSE_ORCHESTRATION_ACTION answer must carry it.
	LetterMass     *float64          `json:"letter_mass"`
	RequestID      protocol.OpaqueID `json:"request_id"`
	RuntimeVersion string            `json:"runtime_version"`
	GPU            string            `json:"gpu"`
	// Quantization is the one the endpoint serves the weights with. Empty
	// means as published.
	Quantization string `json:"quantization"`
}

// emptySHA256 is the digest of no bytes, the output digest of a call
// that got no answer.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func sha256Hex(b []byte) protocol.SHA256Hex {
	sum := sha256.Sum256(b)
	return protocol.SHA256Hex(hex.EncodeToString(sum[:]))
}

// Infer runs one model contract. Every validated request leaves exactly
// one complete call record, whatever the outcome, and the returned
// result carries it. The payload is redacted before anything is sent; if
// redaction fails nothing is sent. Only a call with status ok returns a
// label, a distribution and evidence. A TRIAGE_MESSAGE or
// CHOOSE_ORCHESTRATION_ACTION answer is ok only if readAnswer can read
// it, and a CHOOSE_ORCHESTRATION_ACTION result never carries the
// answer's reason. Each contract has its own deadline. After
// breakerThreshold consecutive endpoint failures the breaker opens and
// calls fail with ErrCircuitOpen without reaching the endpoint; an
// agent with an ANALYZE_AGENT_STATE call in flight gets ErrAgentBusy at
// once. Both still leave a record with status error. Infer never panics.
func (g *Gateway) Infer(ctx context.Context, req protocol.InferRequest) (res protocol.InferResult, err error) {
	return g.infer(ctx, req, nil)
}

// InferSelected runs ANALYZE under the exact immutable tuple the caller
// captured before constructing its payload. An activation during the call does
// not alter its routing or provenance.
func (g *Gateway) InferSelected(
	ctx context.Context,
	req protocol.InferRequest,
	selection protocol.ModelSelection,
) (protocol.InferResult, error) {
	if req.Contract != protocol.ModelContractAnalyzeAgentState || selection.Validate() != nil {
		return protocol.InferResult{}, ErrInvalidRequest
	}
	return g.infer(ctx, req, &selection)
}

// VerifyModelSelection performs one content-free ANALYZE probe through the
// candidate route. A successful, recorded answer confirms the serving system
// accepted the exact adapter name/revision before the control transaction may
// publish it.
func (g *Gateway) VerifyModelSelection(ctx context.Context, selection protocol.ModelSelection) error {
	payload := protocol.Payload(`{"harness":"generic","seconds_since_last_output":0,"output_bytes":{"last_500ms":0,"last_2s":0,"last_10s":0},"new_child_processes":[],"previous_state":"","previous_state_age_seconds":0,"screen_lines":[]}`)
	result, err := g.InferSelected(ctx, protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  protocol.AgentID("model-activation-probe"),
		Payload:  payload,
	}, selection)
	if err != nil || result.CallRecord.Status != protocol.GlimmerCallStatusOK ||
		result.CallRecord.SelectionGeneration != selection.Generation ||
		result.CallRecord.AdapterSHA256 != selection.AdapterSHA256 ||
		selection.Quantization == "" || result.CallRecord.Quantization != selection.Quantization {
		return ErrCallFailed
	}
	return nil
}

func (g *Gateway) infer(
	ctx context.Context,
	req protocol.InferRequest,
	selected *protocol.ModelSelection,
) (res protocol.InferResult, err error) {
	if err := req.Validate(); err != nil {
		return protocol.InferResult{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	var rec protocol.GlimmerCallRecord
	var promptVersion string
	defer func() {
		if recover() != nil {
			rec = g.failed(rec, protocol.GlimmerCallStatusError)
			res, err = g.finish(ctx, rec, promptVersion, nil, fmt.Errorf("%w: internal failure", ErrCallFailed))
		}
	}()
	profile := g.cfg.Profiles[req.Contract]
	if req.Contract == protocol.ModelContractAnalyzeAgentState {
		if selected == nil && g.deps.Selections != nil {
			selection := g.deps.Selections.CurrentModelSelection()
			selected = &selection
		}
		if selected != nil {
			profile = Profile{GatewayProfile: selected.Profile, AdapterSHA256: selected.AdapterSHA256}
		}
	}
	rec = g.baseRecord(req, profile, selected)

	if req.Contract == protocol.ModelContractAnalyzeAgentState {
		if !g.inFlight.acquire(req.AgentID) {
			rec = g.failed(rec, protocol.GlimmerCallStatusError)
			return g.finish(ctx, rec, promptVersion, nil, ErrAgentBusy)
		}
		defer g.inFlight.release(req.AgentID)
	}
	callBreaker := &g.breaker
	if profile.InferenceProfile == protocol.InferenceProfileAnalystLoRA {
		callBreaker = &g.adapterBreaker
	}
	allowed, probe := callBreaker.allow(g.deps.Clock.Now())
	if !allowed {
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrCircuitOpen)
		g.fallbackSelected(ctx, selected, "adapter circuit open")
		return res, err
	}
	verdict := outcomeNeutral
	defer func() { callBreaker.done(g.deps.Clock.Now(), probe, verdict) }()

	clean, version, count, rerr := g.sanitise(req.Contract, req.Payload)
	if rerr != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusRedactionFailed)
		return g.finish(ctx, rec, promptVersion, nil, ErrRedactionFailed)
	}
	rec.RedactionPolicyVersion = version
	rec.RedactionsCount = count
	// The template is applied to the sanitised payload only, so the
	// prompt holds nothing redaction did not see, and the input digest
	// covers the prompt and its version as sent.
	prompt, perr := g.render(req.Contract, clean, selected)
	if perr != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		return g.finish(ctx, rec, promptVersion, nil, fmt.Errorf("%w: prompt not rendered", ErrCallFailed))
	}
	// From here the call names its prompt, whatever the outcome.
	promptVersion = prompt.Version
	rec.PromptVersion = promptVersion
	body, merr := json.Marshal(wireRequest{
		Contract:         req.Contract,
		InferenceProfile: profile.InferenceProfile,
		AdapterName:      profile.AdapterName,
		AdapterRevision:  profile.AdapterRevision,
		PromptVersion:    prompt.Version,
		Messages:         prompt.Messages,
		Payload:          json.RawMessage(clean),
	})
	if merr != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		return g.finish(ctx, rec, promptVersion, nil, fmt.Errorf("%w: request not encoded", ErrCallFailed))
	}
	rec.InputSHA256 = sha256Hex(body)

	start := g.deps.Clock.Now()
	answer, timedOut, serr := g.send(ctx, contractTimeout(req.Contract), body)
	rec.LatencyMS = max(0, g.deps.Clock.Now().Sub(start).Milliseconds())
	// From here every outcome is the endpoint's, except a caller that
	// gave up.
	verdict = outcomeFailure
	switch {
	case timedOut:
		rec = g.failed(rec, protocol.GlimmerCallStatusTimeout)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrTimeout)
		g.fallbackSelected(ctx, selected, "adapter call timed out")
		return res, err
	case serr != nil:
		callerCancelled := ctx.Err() != nil
		if callerCancelled {
			verdict = outcomeNeutral
		}
		// The transport's own text is not wrapped: a transport other than
		// HTTPTransport may quote the endpoint.
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrCallFailed)
		if !callerCancelled {
			g.fallbackSelected(ctx, selected, "adapter call failed")
		}
		return res, err
	}
	rec.OutputSHA256 = sha256Hex(answer)

	var wire wireResponse
	dec := json.NewDecoder(bytes.NewReader(answer))
	dec.DisallowUnknownFields()
	if dec.Decode(&wire) != nil || !atEOF(dec) || len(wire.Probabilities) == 0 {
		rec = g.failed(rec, protocol.GlimmerCallStatusInvalid)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrInvalidOutput)
		g.fallbackSelected(ctx, selected, "adapter returned invalid output")
		return res, err
	}
	evidence, readable := readAnswer(req, wire)
	if !readable {
		rec = g.failed(rec, protocol.GlimmerCallStatusInvalid)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrInvalidOutput)
		g.fallbackSelected(ctx, selected, "adapter returned invalid output")
		return res, err
	}
	ok := rec
	ok.Status = protocol.GlimmerCallStatusOK
	ok.RequestID = wire.RequestID
	ok.RuntimeVersion = wire.RuntimeVersion
	ok.GPU = wire.GPU
	ok.Quantization = wire.Quantization
	ok.Probabilities = wire.Probabilities
	out := &protocol.InferResult{
		Contract:         req.Contract,
		InferenceProfile: profile.InferenceProfile,
		Label:            wire.Label,
		Probabilities:    wire.Probabilities,
		Evidence:         evidence,
		CallRecord:       ok,
	}
	if out.Validate() != nil || ok.Validate() != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusInvalid)
		res, err = g.finish(ctx, rec, promptVersion, nil, ErrInvalidOutput)
		g.fallbackSelected(ctx, selected, "adapter returned invalid output")
		return res, err
	}
	g.noteQuantization(ctx, wire.Quantization)
	verdict = outcomeSuccess
	return g.finish(ctx, ok, promptVersion, out, nil)
}

func (g *Gateway) fallbackSelected(ctx context.Context, selected *protocol.ModelSelection, reason string) {
	if selected == nil || selected.Profile.InferenceProfile != protocol.InferenceProfileAnalystLoRA {
		return
	}
	if g.deps.Selections != nil && g.deps.Selections.CurrentModelSelection().Generation != selected.Generation {
		return
	}
	g.fallbackMu.RLock()
	fallback := g.fallback
	g.fallbackMu.RUnlock()
	if fallback != nil {
		_ = fallback(context.WithoutCancel(ctx), selected.Generation, reason)
	}
}

// noteQuantization warns when the endpoint serves the weights with another
// quantization than the configuration declares: the record holds the one
// served. It warns once per served value, not on every call.
func (g *Gateway) noteQuantization(ctx context.Context, served string) {
	if served == g.cfg.Quantization {
		return
	}
	if last := g.warnedQuantization.Swap(&served); last != nil && *last == served {
		return
	}
	logger := g.deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.WarnContext(ctx, "Glimmer serves another quantization than declared",
		"declared", g.cfg.Quantization, "served", served)
}

// baseRecord is the record of a call before it runs: the profile and the
// model it targets, with digests of nothing yet.
func (g *Gateway) baseRecord(
	req protocol.InferRequest,
	p Profile,
	selection *protocol.ModelSelection,
) protocol.GlimmerCallRecord {
	rec := protocol.GlimmerCallRecord{
		CallID:                 g.deps.IDs.NewCallID(),
		Contract:               req.Contract,
		AgentID:                req.AgentID,
		InferenceProfile:       p.InferenceProfile,
		BaseModelRepo:          g.cfg.BaseModelRepo,
		BaseModelRevision:      g.cfg.BaseModelRevision,
		Quantization:           g.cfg.Quantization,
		Runtime:                g.cfg.Runtime,
		RedactionPolicyVersion: redact.PolicyVersion,
		InputSHA256:            emptySHA256,
		OutputSHA256:           emptySHA256,
		CreatedAt:              g.deps.Clock.Now(),
	}
	if selection != nil {
		rec.BaseModelRepo = selection.BaseModelRepo
		rec.BaseModelRevision = selection.BaseModelRevision
		rec.Runtime = selection.Runtime
		rec.Quantization = selection.Quantization
		rec.SelectionGeneration = selection.Generation
		rec.DatasetVersion = selection.DatasetVersion
		rec.DatasetSHA256 = selection.DatasetSHA256
		rec.TrainingRunID = selection.TrainingRunID
		rec.TrainingCodeRevision = selection.TrainingCodeRevision
		rec.EvaluationRunID = selection.EvaluationRunID
		rec.EvaluationSHA256 = selection.EvaluationSHA256
		rec.ProjectionPolicyVersion = selection.ProjectionPolicyVersion
		rec.ProjectionPolicySHA256 = selection.ProjectionPolicySHA256
		rec.ProfileRevision = selection.ProfileRevision
		rec.ProfileSHA256 = selection.ProfileSHA256
		rec.CalibrationRevision = selection.CalibrationRevision
		rec.CalibrationSHA256 = selection.CalibrationSHA256
		rec.CalibrationTemperature = selection.CalibrationTemperature
		rec.IdleThreshold = selection.IdleThreshold
		rec.AnalysisPolicyVersion = selection.AnalysisPolicyVersion
		rec.PromptVersion = selection.PromptVersion
	}
	if p.InferenceProfile == protocol.InferenceProfileAnalystLoRA {
		rec.AdapterName = p.AdapterName
		rec.AdapterRevision = p.AdapterRevision
		rec.AdapterSHA256 = p.AdapterSHA256
	}
	return rec
}

// failed marks rec with a failure status. A failed call keeps no answer
// metadata and no distribution.
func (g *Gateway) failed(rec protocol.GlimmerCallRecord, status protocol.GlimmerCallStatus) protocol.GlimmerCallRecord {
	rec.Status = status
	rec.RequestID, rec.RuntimeVersion, rec.GPU, rec.Probabilities = "", "", "", nil
	return rec
}

// finish stores the record and returns the result. A failed call
// returns a result that holds only its contract, profile and record.
func (g *Gateway) finish(
	ctx context.Context, rec protocol.GlimmerCallRecord, promptVersion string, out *protocol.InferResult, cause error,
) (protocol.InferResult, error) {
	if rec.Validate() != nil {
		return protocol.InferResult{}, fmt.Errorf("%w: record incomplete", ErrRecordFailed)
	}
	// The record is stored even when the caller has given up on the call.
	if g.record(context.WithoutCancel(ctx), rec, promptVersion) != nil {
		return protocol.InferResult{}, ErrRecordFailed
	}
	if out == nil {
		out = &protocol.InferResult{Contract: rec.Contract, InferenceProfile: rec.InferenceProfile, CallRecord: rec}
	}
	return *out, cause
}

// sanitise redacts the payload field by field, a panic counting as a
// failure.
func (g *Gateway) sanitise(c protocol.ModelContract, payload []byte) (out []byte, version string, count int64, err error) {
	defer func() {
		if recover() != nil {
			out, version, count, err = nil, "", 0, ErrRedactionFailed
		}
	}()
	out, version, count, err = sanitisePayload(g.deps.Redactor, c, payload)
	if err != nil {
		return nil, "", 0, ErrRedactionFailed
	}
	return out, version, count, nil
}

// render runs the prompt renderer, a panic or a malformed prompt
// counting as a failure.
func (g *Gateway) render(
	c protocol.ModelContract,
	clean []byte,
	selection *protocol.ModelSelection,
) (p Prompt, err error) {
	defer func() {
		if recover() != nil {
			p, err = Prompt{}, ErrCallFailed
		}
	}()
	if selection != nil && c == protocol.ModelContractAnalyzeAgentState {
		if renderer, ok := g.deps.Prompts.(VersionedPromptRenderer); ok {
			p, err = renderer.RenderPromptVersion(c, selection.PromptVersion, clean)
		} else {
			p, err = g.deps.Prompts.RenderPrompt(c, clean)
		}
	} else {
		p, err = g.deps.Prompts.RenderPrompt(c, clean)
	}
	if err != nil {
		return Prompt{}, ErrCallFailed
	}
	if !validPrompt(p) {
		return Prompt{}, ErrCallFailed
	}
	if selection != nil && c == protocol.ModelContractAnalyzeAgentState && p.Version != selection.PromptVersion {
		return Prompt{}, ErrCallFailed
	}
	return p, nil
}

// validPrompt reports whether p is either empty or a named, bounded set
// of system and user messages.
func validPrompt(p Prompt) bool {
	if p.Version == "" || len(p.Messages) == 0 {
		return p.Version == "" && len(p.Messages) == 0
	}
	if !validPromptVersion(p.Version) || len(p.Messages) > maxPromptMessages {
		return false
	}
	total := 0
	for _, m := range p.Messages {
		if (m.Role != RoleSystem && m.Role != RoleUser) || m.Content == "" {
			return false
		}
		total += len(m.Content)
	}
	return total <= maxPromptBytes
}

// send runs the transport under the contract deadline, measured on the
// gateway clock, and returns by then even if the transport ignores its
// context. A panic counts as a failure.
func (g *Gateway) send(ctx context.Context, timeout time.Duration, body []byte) (out []byte, timedOut bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	deadline := g.deps.Clock.After(timeout)
	type answer struct {
		body []byte
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- answer{err: ErrCallFailed}
			}
		}()
		b, err := g.deps.Transport.Send(ctx, body)
		done <- answer{b, err}
	}()
	select {
	case a := <-done:
		return a.body, false, a.err
	case <-deadline:
		return nil, true, ErrTimeout
	case <-ctx.Done():
		return nil, false, fmt.Errorf("%w: %w", ErrCallFailed, ctx.Err())
	}
}

// record stores rec and its prompt version, a panic counting as a failure.
func (g *Gateway) record(ctx context.Context, rec protocol.GlimmerCallRecord, promptVersion string) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrRecordFailed
		}
	}()
	if g.deps.Recorder.RecordCall(ctx, rec, promptVersion) != nil {
		return ErrRecordFailed
	}
	return nil
}
