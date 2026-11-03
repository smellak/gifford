package tmuxlock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/clock"
)

var (
	// ErrWriterUnprovisioned means the node did not find the complete,
	// published pane inventory required by an ownership operation.
	ErrWriterUnprovisioned = errors.New("pane-lock writer unprovisioned")
	// ErrWriterDeferred means another operation owns the pane or left a
	// recoverable operation that this call is not authorized to finish.
	ErrWriterDeferred = errors.New("pane-lock writer deferred")
	// ErrWriterInvalid means existing inventory or records are corrupt or do
	// not describe the requested pane.
	ErrWriterInvalid = errors.New("pane-lock writer invalid")
)

// NodeWriterOptions configure node-owned lifecycle writes. Protocol identity
// and the mode-file path have no defaults.
type NodeWriterOptions struct {
	Root        string
	Host        string
	PeerOwner   string
	PeerWriters []string
	ModePath    string
}

type writerBoundary string

const (
	boundaryRegister writerBoundary = "register-durable"
	boundaryState    writerBoundary = "state-durable"
	boundaryClear    writerBoundary = "clear-durable"
)

type writerHooks struct {
	now      func() time.Time
	random   io.Reader
	boundary func(writerBoundary) error
	modeUID  uint32
	modeGID  uint32
	testMode bool
}

// NodeWriter performs explicit, authenticated ownership transfers. It never
// takes ownership automatically.
type NodeWriter struct {
	root      string
	host      string
	peerOwner string
	modePath  string
	policy    validatedRecordPolicy
	hooks     writerHooks
}

// NewNodeWriter constructs the node's ownership writer.
func NewNodeWriter(options NodeWriterOptions) (*NodeWriter, error) {
	return newNodeWriter(options, writerHooks{})
}

func newNodeWriter(options NodeWriterOptions, hooks writerHooks) (*NodeWriter, error) {
	if options.Root == "" {
		options.Root = DefaultRoot
	}
	policy, err := validateRecordPolicy(RecordPolicy{
		PeerOwner: options.PeerOwner, PeerWriters: options.PeerWriters,
	})
	if !validManagedPath(options.Root) || !validManagedPath(options.ModePath) ||
		!hostPattern.MatchString(options.Host) || err != nil {
		return nil, fmt.Errorf("new pane-lock writer: %w", ErrInvalidBinding)
	}
	if hooks.now == nil {
		hooks.now = clock.System{}.Now
	}
	if hooks.random == nil {
		hooks.random = rand.Reader
	}
	if !hooks.testMode {
		hooks.modeUID = 0
		hooks.modeGID = 0
	}
	return &NodeWriter{
		root: options.Root, host: options.Host, peerOwner: options.PeerOwner,
		modePath: options.ModePath, policy: policy, hooks: hooks,
	}, nil
}

// Take explicitly transfers one incorporated pane from the configured peer
// to this node.
func (w *NodeWriter) Take(ctx context.Context, binding LiveBinding) (OwnershipResult, error) {
	return w.transfer(ctx, binding, w.peerOwner, nodeOwner, "take")
}

// Return explicitly transfers one incorporated pane from this node to the
// configured peer. Published inventory remains unchanged.
func (w *NodeWriter) Return(ctx context.Context, binding LiveBinding) (OwnershipResult, error) {
	return w.transfer(ctx, binding, nodeOwner, w.peerOwner, "return")
}

// OwnershipResult is the content-free acknowledgement of an ownership
// transfer. Changed is false only for an exact idempotent replay.
type OwnershipResult struct {
	Changed  bool   `json:"changed"`
	Owner    string `json:"owner"`
	OwnerGen uint32 `json:"owner_gen"`
}

func (w *NodeWriter) transfer(
	ctx context.Context,
	binding LiveBinding,
	source string,
	target string,
	operation string,
) (OwnershipResult, error) {
	if w == nil || ctx == nil || binding.Host != w.host {
		return OwnershipResult{}, fmt.Errorf("%s pane ownership: %w", operation, ErrInvalidBinding)
	}
	if err := validateLiveBinding(binding); err != nil {
		return OwnershipResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return OwnershipResult{}, fmt.Errorf("%s pane ownership context: %w", operation, err)
	}
	if err := requireProtocolModePlatform(
		w.modePath, w.root, w.hooks.modeUID, w.hooks.modeGID,
	); err != nil {
		return OwnershipResult{}, fmt.Errorf("validate pane-lock mode: %w", ErrWriterInvalid)
	}
	return w.transferPlatform(ctx, binding, source, target)
}

func (w *NodeWriter) newTxID() (string, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(w.hooks.random, raw); err != nil {
		return "", fmt.Errorf("generate pane-lock transaction: %w", err)
	}
	const alphabet = "0123456789abcdef"
	out := make([]byte, 32)
	for index, value := range raw {
		out[index*2] = alphabet[value>>4]
		out[index*2+1] = alphabet[value&0x0f]
	}
	return string(out), nil
}

func (w *NodeWriter) crossed(boundary writerBoundary) error {
	if w.hooks.boundary == nil {
		return nil
	}
	return w.hooks.boundary(boundary)
}
