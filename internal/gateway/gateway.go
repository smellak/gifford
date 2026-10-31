package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

// CallRecorder stores one call record.
type CallRecorder interface {
	RecordCall(ctx context.Context, rec protocol.GlimmerCallRecord) error
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

// maxPromptBytes bounds a rendered prompt.
const maxPromptBytes = 256 << 10

// CallIDs issues one fresh call id per call.
type CallIDs interface {
	NewCallID() protocol.OpaqueID
}

// Deps are the gateway's collaborators. Every one is required.
type Deps struct {
	Transport Transport
	Redactor  Redactor
	Recorder  CallRecorder
	Clock     Clock
	IDs       CallIDs
	Prompts   PromptRenderer
}

// Gateway runs model contracts against Glimmer. It does not decide: it
// returns the model's distribution to the Analyst.
type Gateway struct {
	cfg      Config
	deps     Deps
	breaker  breaker
	inFlight inFlight
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
	if err := req.Validate(); err != nil {
		return protocol.InferResult{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	var rec protocol.GlimmerCallRecord
	defer func() {
		if recover() != nil {
			rec = g.failed(rec, protocol.GlimmerCallStatusError)
			res, err = g.finish(ctx, rec, nil, fmt.Errorf("%w: internal failure", ErrCallFailed))
		}
	}()
	profile := g.cfg.Profiles[req.Contract]
	rec = g.baseRecord(req, profile)

	if req.Contract == protocol.ModelContractAnalyzeAgentState {
		if !g.inFlight.acquire(req.AgentID) {
			rec = g.failed(rec, protocol.GlimmerCallStatusError)
			return g.finish(ctx, rec, nil, ErrAgentBusy)
		}
		defer g.inFlight.release(req.AgentID)
	}
	allowed, probe := g.breaker.allow(g.deps.Clock.Now())
	if !allowed {
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		return g.finish(ctx, rec, nil, ErrCircuitOpen)
	}
	verdict := outcomeNeutral
	defer func() { g.breaker.done(g.deps.Clock.Now(), probe, verdict) }()

	clean, version, count, rerr := g.sanitise(req.Contract, req.Payload)
	if rerr != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusRedactionFailed)
		return g.finish(ctx, rec, nil, ErrRedactionFailed)
	}
	rec.RedactionPolicyVersion = version
	rec.RedactionsCount = count
	// The template is applied to the sanitised payload only, so the
	// prompt holds nothing redaction did not see, and the input digest
	// covers the prompt and its version as sent.
	prompt, perr := g.render(req.Contract, clean)
	if perr != nil {
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		return g.finish(ctx, rec, nil, fmt.Errorf("%w: prompt not rendered", ErrCallFailed))
	}
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
		return g.finish(ctx, rec, nil, fmt.Errorf("%w: request not encoded", ErrCallFailed))
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
		return g.finish(ctx, rec, nil, ErrTimeout)
	case serr != nil:
		if ctx.Err() != nil {
			verdict = outcomeNeutral
		}
		// The transport's own text is not wrapped: a transport other than
		// HTTPTransport may quote the endpoint.
		rec = g.failed(rec, protocol.GlimmerCallStatusError)
		return g.finish(ctx, rec, nil, ErrCallFailed)
	}
	rec.OutputSHA256 = sha256Hex(answer)

	var wire wireResponse
	dec := json.NewDecoder(bytes.NewReader(answer))
	dec.DisallowUnknownFields()
	if dec.Decode(&wire) != nil || !atEOF(dec) || len(wire.Probabilities) == 0 {
		rec = g.failed(rec, protocol.GlimmerCallStatusInvalid)
		return g.finish(ctx, rec, nil, ErrInvalidOutput)
	}
	evidence, readable := readAnswer(req, wire)
	if !readable {
		rec = g.failed(rec, protocol.GlimmerCallStatusInvalid)
		return g.finish(ctx, rec, nil, ErrInvalidOutput)
	}
	ok := rec
	ok.Status = protocol.GlimmerCallStatusOK
	ok.RequestID = wire.RequestID
	ok.RuntimeVersion = wire.RuntimeVersion
	ok.GPU = wire.GPU
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
		return g.finish(ctx, rec, nil, ErrInvalidOutput)
	}
	verdict = outcomeSuccess
	return g.finish(ctx, ok, out, nil)
}

// baseRecord is the record of a call before it runs: the profile and the
// model it targets, with digests of nothing yet.
func (g *Gateway) baseRecord(req protocol.InferRequest, p Profile) protocol.GlimmerCallRecord {
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
func (g *Gateway) finish(ctx context.Context, rec protocol.GlimmerCallRecord, out *protocol.InferResult, cause error) (protocol.InferResult, error) {
	if rec.Validate() != nil {
		return protocol.InferResult{}, fmt.Errorf("%w: record incomplete", ErrRecordFailed)
	}
	// The record is stored even when the caller has given up on the call.
	if g.record(context.WithoutCancel(ctx), rec) != nil {
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
func (g *Gateway) render(c protocol.ModelContract, clean []byte) (p Prompt, err error) {
	defer func() {
		if recover() != nil {
			p, err = Prompt{}, ErrCallFailed
		}
	}()
	p, err = g.deps.Prompts.RenderPrompt(c, clean)
	if err != nil {
		return Prompt{}, ErrCallFailed
	}
	if !validPrompt(p) {
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
	if len(p.Version) > 64 || !protocol.OpaqueID(p.Version).Valid() || len(p.Messages) > maxPromptMessages {
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

// record stores rec, a panic counting as a failure.
func (g *Gateway) record(ctx context.Context, rec protocol.GlimmerCallRecord) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrRecordFailed
		}
	}()
	if g.deps.Recorder.RecordCall(ctx, rec) != nil {
		return ErrRecordFailed
	}
	return nil
}
