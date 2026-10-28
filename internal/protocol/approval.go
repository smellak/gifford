package protocol

import (
	"fmt"
	"time"
)

// This file holds the approval types of §6.4: one table for everything
// that waits for a human.

// ApprovalRequest is one row of the approvals table.
type ApprovalRequest struct {
	ApprovalID  ApprovalID         `json:"approval_id"`
	Source      ApprovalSource     `json:"source"`
	ObjectType  ApprovalObjectType `json:"object_type"`
	ObjectID    string             `json:"object_id" jsonschema:"minLength=1,maxLength=300"`
	AgentID     AgentID            `json:"agent_id,omitempty"`
	LoopID      LoopID             `json:"loop_id,omitempty"`
	Options     []ApprovalOption   `json:"options" jsonschema:"minItems=1,maxItems=8,uniqueItems=true"`
	RequestedBy AgentID            `json:"requested_by,omitempty"`
	RequestNote string             `json:"request_note,omitempty" jsonschema:"maxLength=500"`
	State       ApprovalState      `json:"state"`
	Resolution  *ApprovalResolution `json:"resolution,omitempty"`
	ResolvedBy  string             `json:"resolved_by,omitempty" jsonschema:"minLength=1,maxLength=128"`
	ExpiresAt   time.Time          `json:"expires_at,omitzero"`
	CreatedAt   time.Time          `json:"created_at"`
	ResolvedAt  time.Time          `json:"resolved_at,omitzero"`
}

// Validate checks the request.
func (r ApprovalRequest) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	offered := offeredKinds(r.Source)
	for i, opt := range r.Options {
		if !offered[optionKind(opt)] {
			return &ValidationError{Path: fmt.Sprintf("options[%d]", i), Reason: "kind not offered for source"}
		}
	}
	switch r.State {
	case ApprovalStatePending:
		if r.Resolution != nil || r.ResolvedBy != "" || !r.ResolvedAt.IsZero() {
			return &ValidationError{Path: "resolution", Reason: "PENDING carries no resolution"}
		}
	case ApprovalStateApproved, ApprovalStateRejected:
		if r.Resolution == nil || r.ResolvedBy == "" || r.ResolvedAt.IsZero() {
			return &ValidationError{Path: "resolution", Reason: "resolved states carry a resolution"}
		}
		if !offered[optionKind(r.Resolution.Option)] {
			return &ValidationError{Path: "resolution", Reason: "resolution kind not offered"}
		}
	case ApprovalStateExpired, ApprovalStateSuperseded:
		if r.Resolution != nil {
			return &ValidationError{Path: "resolution", Reason: "only APPROVED/REJECTED carry a resolution"}
		}
	}
	if r.ObjectType == ApprovalObjectTypeThreadLink {
		if r.Source != ApprovalSourceLink {
			return &ValidationError{Path: "source", Reason: "thread_link comes from link"}
		}
		if r.State != ApprovalStateApproved {
			return &ValidationError{Path: "state", Reason: "thread_link is APPROVED"}
		}
		if r.ExpiresAt.IsZero() {
			return &ValidationError{Path: "expires_at", Reason: "thread_link carries an expiry"}
		}
	}
	if r.ObjectType == ApprovalObjectTypeLoop {
		if r.Source != ApprovalSourceLoopBudget && r.Source != ApprovalSourceLoopPattern && r.Source != ApprovalSourceAgentRequest {
			return &ValidationError{Path: "source", Reason: "loop objects come from loop_budget, loop_pattern or agent_request"}
		}
		if string(r.LoopID) != r.ObjectID {
			return &ValidationError{Path: "loop_id", Reason: "loop objects address their loop"}
		}
	}
	if r.Source == ApprovalSourceLoopStep && r.ObjectType != ApprovalObjectTypeMessage && r.ObjectType != ApprovalObjectTypeArtifact {
		return &ValidationError{Path: "object_type", Reason: "loop_step covers messages or artifacts"}
	}
	return nil
}

// optionKind returns the kind of an approval option: the text before the
// first colon, or the whole token.
func optionKind(opt ApprovalOption) string {
	s := string(opt)
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i]
		}
	}
	return s
}

// ApprovalResolution is the recorded resolution of an approval.
type ApprovalResolution struct {
	Option ApprovalOption `json:"option"`
	Reason string         `json:"reason,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the resolution.
func (r ApprovalResolution) Validate() error {
	return checkRoot(&r)
}

// ApprovalList lists approval requests.
type ApprovalList struct {
	Approvals []ApprovalRequest `json:"approvals"`
}

// Validate checks the list.
func (l ApprovalList) Validate() error {
	return checkRoot(&l)
}
