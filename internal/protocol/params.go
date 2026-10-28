package protocol

// This file holds the Control API params roots of §6.8: one root per
// method, Go type named as the CamelCase of the root. ttl_ms absent or 0
// is permanent unless stated; reason is operator text, O, ≤ 280 unless
// marked R.

// EmptyParams carries no fields.
type EmptyParams struct{}

// Validate checks the empty params.
func (EmptyParams) Validate() error { return checkRoot(&EmptyParams{}) }

// NodeParams addresses one node.
type NodeParams struct {
	NodeID NodeID `json:"node_id"`
}

// Validate checks the node params.
func (p NodeParams) Validate() error {
	return checkRoot(&p)
}

// SetGlobalParams writes the global row.
type SetGlobalParams struct {
	Mode            ChannelMode `json:"mode"`
	Reason          string      `json:"reason,omitempty" jsonschema:"maxLength=280"`
	TTLMS           *int64      `json:"ttl_ms,omitempty" jsonschema:"minimum=0,maximum=2592000000"`
	ThenMode        ChannelMode `json:"then_mode,omitempty"`
	ExpectedVersion int64       `json:"expected_version" jsonschema:"minimum=0"`
}

// Validate checks the global params.
func (p SetGlobalParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	return checkDrainingThenOff(p.Mode, p.ThenMode)
}

// checkDrainingThenOff enforces the --then off rule shared by the global
// and node setters: only OFF, only with mode DRAINING.
func checkDrainingThenOff(mode, thenMode ChannelMode) error {
	if thenMode == "" {
		return nil
	}
	if thenMode != ChannelModeOff {
		return &ValidationError{Path: pathThenMode, Reason: "only OFF"}
	}
	if mode != ChannelModeDraining {
		return &ValidationError{Path: pathThenMode, Reason: "only with mode DRAINING"}
	}
	return nil
}

// SetNodeParams writes one node row: the node plus the global fields.
type SetNodeParams struct {
	NodeID          NodeID      `json:"node_id"`
	Mode            ChannelMode `json:"mode"`
	Reason          string      `json:"reason,omitempty" jsonschema:"maxLength=280"`
	TTLMS           *int64      `json:"ttl_ms,omitempty" jsonschema:"minimum=0,maximum=2592000000"`
	ThenMode        ChannelMode `json:"then_mode,omitempty"`
	ExpectedVersion int64       `json:"expected_version" jsonschema:"minimum=0"`
}

// Validate checks the node params.
func (p SetNodeParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	return checkDrainingThenOff(p.Mode, p.ThenMode)
}

// EffectiveParams previews the delivery gate for one pair and plane.
type EffectiveParams struct {
	FromAgent AgentID `json:"from_agent"`
	ToAgent   AgentID `json:"to_agent"`
	Plane     Plane   `json:"plane"`
}

// Validate checks the effective params.
func (p EffectiveParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.Plane != PlaneMessage && p.Plane != PlaneArtifact {
		return &ValidationError{Path: pathPlane, Reason: reasonMessageOrArtifact}
	}
	return nil
}

// ExplainParams asks for one explanation in exactly one form: the pair,
// the message, or the artifact. Plane, thread and loop only go with the
// pair form.
type ExplainParams struct {
	FromAgent  AgentID  `json:"from_agent,omitempty"`
	ToAgent    AgentID  `json:"to_agent,omitempty"`
	Plane      Plane    `json:"plane,omitempty"`
	ThreadID   ThreadID `json:"thread_id,omitempty"`
	LoopID     LoopID   `json:"loop_id,omitempty"`
	MessageID  ULID     `json:"message_id,omitempty"`
	ArtifactID OpaqueID `json:"artifact_id,omitempty"`
}

// Validate checks the explanation form.
func (p ExplainParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	pair := p.FromAgent != "" && p.ToAgent != ""
	single := (p.MessageID == "") != (p.ArtifactID == "")
	switch {
	case pair && p.MessageID == "" && p.ArtifactID == "":
		return nil
	case !pair && p.FromAgent == "" && p.ToAgent == "" && single:
		if p.Plane != "" || p.ThreadID != "" || p.LoopID != "" {
			return &ValidationError{Path: pathFromAgent, Reason: "plane, thread and loop go with the pair form"}
		}
		return nil
	default:
		return &ValidationError{Path: pathFromAgent, Reason: "one form: the pair, the message, or the artifact"}
	}
}

// AgentParams addresses one agent.
type AgentParams struct {
	AgentID AgentID `json:"agent_id"`
}

// Validate checks the agent params.
func (p AgentParams) Validate() error {
	return checkRoot(&p)
}

// SetAgentChannelParams writes one agent row: at least one of the flags.
type SetAgentChannelParams struct {
	AgentID         AgentID `json:"agent_id"`
	TX              *bool   `json:"tx,omitempty"`
	RX              *bool   `json:"rx,omitempty"`
	AutoDelivery    *bool   `json:"auto_delivery,omitempty"`
	AutoContinue    *bool   `json:"auto_continue,omitempty"`
	TTLMS           *int64  `json:"ttl_ms,omitempty" jsonschema:"minimum=0,maximum=2592000000"`
	Reason          string  `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersion int64   `json:"expected_version" jsonschema:"minimum=0"`
}

// Validate checks the agent params.
func (p SetAgentChannelParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.TX == nil && p.RX == nil && p.AutoDelivery == nil && p.AutoContinue == nil {
		return &ValidationError{Path: "tx", Reason: "at least one of the four flags"}
	}
	return nil
}

// SetAgentModeParams writes one agent row from a macro.
type SetAgentModeParams struct {
	AgentID         AgentID   `json:"agent_id"`
	Mode            AgentMode `json:"mode"`
	TTLMS           *int64    `json:"ttl_ms,omitempty" jsonschema:"minimum=0,maximum=2592000000"`
	Reason          string    `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersion int64     `json:"expected_version" jsonschema:"minimum=0"`
}

// Validate checks the agent mode params.
func (p SetAgentModeParams) Validate() error {
	return checkRoot(&p)
}

// LinkParams addresses one directional link.
type LinkParams struct {
	FromAgent AgentID `json:"from_agent"`
	ToAgent   AgentID `json:"to_agent"`
}

// Validate checks the link params.
func (p LinkParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.FromAgent == p.ToAgent {
		return &ValidationError{Path: pathFromAgent, Reason: reasonFromDiffersTo}
	}
	return nil
}

// ListLinksParams filters links by participant.
type ListLinksParams struct {
	AgentID AgentID `json:"agent_id,omitempty"`
}

// Validate checks the list params.
func (p ListLinksParams) Validate() error {
	return checkRoot(&p)
}

// SetLinkParams writes link rows: plane all writes both planes, and the
// versions map carries exactly one link: key per row written.
type SetLinkParams struct {
	FromAgent        AgentID         `json:"from_agent"`
	ToAgent          AgentID         `json:"to_agent"`
	Mode             LinkMode        `json:"mode"`
	Plane            Plane           `json:"plane"`
	Bidirectional    bool            `json:"bidirectional,omitempty"`
	TTLMS            *int64          `json:"ttl_ms,omitempty" jsonschema:"minimum=0,maximum=2592000000"`
	Reason           string          `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersions ControlVersions `json:"expected_versions"`
}

// Validate checks the link params.
func (p SetLinkParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.FromAgent == p.ToAgent {
		return &ValidationError{Path: pathFromAgent, Reason: reasonFromDiffersTo}
	}
	return checkLinkVersionKeys(p.ExpectedVersions, linkRowKeys(p.FromAgent, p.ToAgent, p.Plane, p.Bidirectional))
}

// linkRowKeys returns the link: keys of the rows a link call writes:
// from→to, plus to→from when bidirectional, each on plane (or on message
// and artifact when plane is all).
func linkRowKeys(from, to AgentID, plane Plane, bidirectional bool) []string {
	planes := []Plane{plane}
	if plane == PlaneAll {
		planes = []Plane{PlaneMessage, PlaneArtifact}
	}
	rows := len(planes)
	if bidirectional {
		rows *= 2
	}
	keys := make([]string, 0, rows)
	for _, p := range planes {
		keys = append(keys, "link:"+string(from)+linkArrow+string(to)+":"+string(p))
	}
	if bidirectional {
		for _, p := range planes {
			keys = append(keys, "link:"+string(to)+linkArrow+string(from)+":"+string(p))
		}
	}
	return keys
}

// checkLinkVersionKeys requires exactly the given keys, no more and no
// less: every row written carries its version, 0 when the row is new.
func checkLinkVersionKeys(versions ControlVersions, keys []string) error {
	if len(versions) != len(keys) {
		return &ValidationError{Path: pathExpectedVersions, Reason: "one link: key per row written"}
	}
	for _, key := range keys {
		if _, ok := versions[key]; !ok {
			return &ValidationError{Path: pathExpectedVersions, Reason: "one link: key per row written"}
		}
	}
	return nil
}

// DeleteLinkOverrideParams deletes link rows named by their keys.
type DeleteLinkOverrideParams struct {
	FromAgent        AgentID         `json:"from_agent"`
	ToAgent          AgentID         `json:"to_agent"`
	Plane            Plane           `json:"plane"`
	ExpectedVersions ControlVersions `json:"expected_versions"`
}

// Validate checks the delete params.
func (p DeleteLinkOverrideParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if len(p.ExpectedVersions) == 0 {
		return &ValidationError{Path: pathExpectedVersions, Reason: "names the rows deleted"}
	}
	return checkLinkVersionKeys(p.ExpectedVersions, linkRowKeys(p.FromAgent, p.ToAgent, p.Plane, false))
}

// CreateLoopParams opens one governed loop. Budget overrides reuse the
// RequestedBudget of §5.5: absent dimensions keep the profile value, and
// any override stores custom.
type CreateLoopParams struct {
	ThreadID               ThreadID         `json:"thread_id"`
	Participants           []AgentID        `json:"participants" jsonschema:"minItems=2,maxItems=32,uniqueItems=true"`
	Mode                   Autonomy         `json:"mode"`
	BudgetProfile          BudgetProfile    `json:"budget_profile,omitempty"`
	Budget                 *RequestedBudget `json:"budget,omitempty"`
	Reason                 string           `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedLoopGeneration int64            `json:"expected_loop_generation" jsonschema:"minimum=0"`
}

// Validate checks the creation params.
func (p CreateLoopParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	switch p.BudgetProfile {
	case "", BudgetProfileDemo, BudgetProfileDefault, BudgetProfileStrict:
	default:
		return &ValidationError{Path: "budget_profile", Reason: "demo, default or strict"}
	}
	return nil
}

// LoopParams addresses one loop.
type LoopParams struct {
	LoopID LoopID `json:"loop_id"`
}

// Validate checks the loop params.
func (p LoopParams) Validate() error {
	return checkRoot(&p)
}

// ListLoopsParams filters loops.
type ListLoopsParams struct {
	Active   bool     `json:"active,omitempty"`
	ThreadID ThreadID `json:"thread_id,omitempty"`
	AgentID  AgentID  `json:"agent_id,omitempty"`
}

// Validate checks the list params.
func (p ListLoopsParams) Validate() error {
	return checkRoot(&p)
}

// SetLoopModeParams moves a loop between AUTO, STEP, PAUSED and DRAINING.
type SetLoopModeParams struct {
	LoopID          LoopID   `json:"loop_id"`
	Mode            LoopMode `json:"mode"`
	Reason          string   `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersion int64    `json:"expected_version" jsonschema:"minimum=1"`
}

// Validate checks the mode params.
func (p SetLoopModeParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	switch p.Mode {
	case LoopModeAuto, LoopModeStep, LoopModePaused, LoopModeDraining:
	default:
		return &ValidationError{Path: pathMode, Reason: "AUTO, STEP, PAUSED or DRAINING"}
	}
	return nil
}

// AdvanceLoopParams grants step tokens to a STEP loop.
type AdvanceLoopParams struct {
	LoopID          LoopID `json:"loop_id"`
	Steps           int64  `json:"steps" jsonschema:"minimum=1,maximum=100"`
	ExpectedVersion int64  `json:"expected_version" jsonschema:"minimum=1"`
}

// Validate checks the advance params.
func (p AdvanceLoopParams) Validate() error {
	return checkRoot(&p)
}

// ExtendLoopBudgetParams grows loop budget dimensions.
type ExtendLoopBudgetParams struct {
	LoopID          LoopID `json:"loop_id"`
	AddTurns        *int64 `json:"add_turns,omitempty" jsonschema:"minimum=1,maximum=1000"`
	AddMessages     *int64 `json:"add_messages,omitempty" jsonschema:"minimum=1,maximum=1000"`
	AddDurationMS   *int64 `json:"add_duration_ms,omitempty" jsonschema:"minimum=1000,maximum=86400000"`
	Reason          string `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersion int64  `json:"expected_version" jsonschema:"minimum=1"`
}

// Validate checks the extension params.
func (p ExtendLoopBudgetParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.AddTurns == nil && p.AddMessages == nil && p.AddDurationMS == nil {
		return &ValidationError{Path: "add_turns", Reason: "at least one add_* dimension"}
	}
	return nil
}

// StopLoopParams stops a loop with a mandatory reason.
type StopLoopParams struct {
	LoopID          LoopID `json:"loop_id"`
	Reason          string `json:"reason" jsonschema:"minLength=1,maxLength=280"`
	CancelTurn      bool   `json:"cancel_turn,omitempty"`
	ExpectedVersion int64  `json:"expected_version" jsonschema:"minimum=1"`
}

// Validate checks the stop params.
func (p StopLoopParams) Validate() error {
	return checkRoot(&p)
}

// CompleteLoopParams completes a loop.
type CompleteLoopParams struct {
	LoopID          LoopID `json:"loop_id"`
	Reason          string `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpectedVersion int64  `json:"expected_version" jsonschema:"minimum=1"`
}

// Validate checks the completion params.
func (p CompleteLoopParams) Validate() error {
	return checkRoot(&p)
}

// ListApprovalsParams filters approval requests.
type ListApprovalsParams struct {
	State   ApprovalState `json:"state,omitempty"`
	AgentID AgentID       `json:"agent_id,omitempty"`
	LoopID  LoopID        `json:"loop_id,omitempty"`
}

// Validate checks the list params.
func (p ListApprovalsParams) Validate() error {
	return checkRoot(&p)
}

// ApprovalParams addresses one approval request.
type ApprovalParams struct {
	ApprovalID ApprovalID `json:"approval_id"`
}

// Validate checks the approval params.
func (p ApprovalParams) Validate() error {
	return checkRoot(&p)
}

// ResolveApprovalParams resolves one pending request with an offered
// option; the option parameter may differ from the offered one.
type ResolveApprovalParams struct {
	ApprovalID ApprovalID     `json:"approval_id"`
	Option     ApprovalOption `json:"option"`
	Reason     string         `json:"reason,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the resolution params.
func (p ResolveApprovalParams) Validate() error {
	return checkRoot(&p)
}

// ApproveMessageParams approves a message, once or for a thread TTL.
type ApproveMessageParams struct {
	MessageID   ULID   `json:"message_id"`
	ThreadTTLMS *int64 `json:"thread_ttl_ms,omitempty" jsonschema:"minimum=1,maximum=2592000000"`
}

// Validate checks the approval params.
func (p ApproveMessageParams) Validate() error {
	return checkRoot(&p)
}

// RejectMessageParams rejects a message with a mandatory reason.
type RejectMessageParams struct {
	MessageID ULID   `json:"message_id"`
	Reason    string `json:"reason" jsonschema:"minLength=1,maxLength=280"`
}

// Validate checks the rejection params.
func (p RejectMessageParams) Validate() error {
	return checkRoot(&p)
}

// ReleaseHoldParams releases exactly one held object.
type ReleaseHoldParams struct {
	MessageID  ULID     `json:"message_id,omitempty"`
	ArtifactID OpaqueID `json:"artifact_id,omitempty"`
	Reason     string   `json:"reason,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the release params.
func (p ReleaseHoldParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if (p.MessageID == "") == (p.ArtifactID == "") {
		return &ValidationError{Path: pathMessageID, Reason: reasonExactlyOneObject}
	}
	return nil
}
