package controlapi

import (
	"context"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
)

// PaneOperationActor is trusted, non-wire audit identity derived from the
// authenticated Unix-socket peer. Administrative params never carry it.
type PaneOperationActor struct {
	Actor channel.Actor
	UID   uint32
	GID   uint32
	PID   int
}

// PaneProvisionService owns explicit pane incorporation.
type PaneProvisionService interface {
	Provision(context.Context, protocol.PaneOperationParams, PaneOperationActor) (protocol.PaneOperationResult, error)
}

// PaneOwnershipService owns explicit, recoverable ownership transfers.
type PaneOwnershipService interface {
	Take(context.Context, protocol.PaneOperationParams, PaneOperationActor) (protocol.PaneOperationResult, error)
	Return(context.Context, protocol.PaneOperationParams, PaneOperationActor) (protocol.PaneOperationResult, error)
}

// PaneRecoveryService owns explicit uncertain-attempt recovery. Reconcile is
// conditional on the inspected attempt; repair targets corrupt state without
// trusting an attempt identity from that state.
type PaneRecoveryService interface {
	Reconcile(context.Context, protocol.PaneAttemptParams, PaneOperationActor) (protocol.PaneOperationResult, error)
	Repair(context.Context, protocol.PaneOperationParams, PaneOperationActor) (protocol.PaneOperationResult, error)
}
