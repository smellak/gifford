package tmuxlock

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
)

// PanePeer identifies the process asking for a panel operation. It carries
// the same SO_PEERCRED facts the panel guard authorizes on, plus the
// non-wire actor ID the caller derives from peer.actor() (uid and pid, in
// the same "uid:<uid>:pid:<pid>" form the other services use) so the
// authorizer can leave it in the telemetry. The struct lives here instead
// of reusing controlapi.Peer because controlapi already depends
// (transitively) on tmuxlock, so tmuxlock cannot import controlapi without
// an import cycle. The production authorizer (controlapi.PaneOperationAuthorizer)
// satisfies the production role through a trivial adapter at its own call
// site; this interface only states the seam.
type PanePeer struct {
	PID      int
	UID      uint32
	GID      uint32
	Verified bool
	Actor    string
}

// PaneAuthorizer grants or denies a panel operation for a peer. A nil
// authorizer denies everything: provision never runs unauthenticated.
type PaneAuthorizer interface {
	AuthorizePane(peer PanePeer) error
}

// ProvisionSpec asks the node to provision (alta) one panel: create-or-validate
// the owner directory, the socket directory and the three panel files, exactly
// as the Alta commit order requires (panel files first, durable, then the
// indexes in a later step). Owner is the record owner name (lowercase owner
// token such as the node roles use), while Binding.OwnerUID selects the owner
// directory and the named ACL entry. Both come from the authenticated admin
// request; provision never invents the mapping.
type ProvisionSpec struct {
	Root       string
	Binding    PanelBinding
	PanePID    int64
	PaneStart  int64
	BootID     string
	Host       string
	BindUID    uint32
	Owner      string
	Clock      clock.Clock
	Peer       PanePeer
	Authorizer PaneAuthorizer
}

// ProvisionResult reports the canonical paths, whether this call created at
// least one panel file, and the index publication facts the node persists.
// Created=false means every panel file already existed, validated strictly,
// and cohered with the request; the publication still runs convergently.
type ProvisionResult struct {
	Created     bool
	OwnerDir    string
	SocketDir   string
	Lock        string
	State       string
	Register    string
	IndexLock   string
	Publication IndexPublication
}

// panelContent is the exact byte payload provision installs or validates.
type panelContent struct {
	owner    string
	binding  recordBinding
	lock     []byte
	state    []byte
	register []byte
}

// IndexPublication reports one Alta index publication: the panel key the
// pane_index_publications row stores, the sequence numbers each index now
// carries, and the timestamp captured right after the U/index directory
// fsync, which the node persists with the row.
type IndexPublication struct {
	PanelKey    string
	OwnerSeq    uint64
	EnrolledSeq uint64
	PublishedAt time.Time
}

// Provision provisions one panel: authorize first (no filesystem side effects
// on deny), then create-or-validate the directories and files, durably.
// Repeating provision for the same owner and binding is a no-op that
// revalidates everything; any mismatch with already installed files is an
// error and leaves them untouched.
func Provision(spec ProvisionSpec) (ProvisionResult, error) {
	if spec.Authorizer == nil {
		return ProvisionResult{}, errors.New("pane provision denied: no authorizer")
	}
	if err := spec.Authorizer.AuthorizePane(spec.Peer); err != nil {
		return ProvisionResult{}, fmt.Errorf("pane provision denied: %w", err)
	}
	parts, err := validateProvisionSpec(spec)
	if err != nil {
		return ProvisionResult{}, err
	}
	if spec.Clock == nil {
		spec.Clock = clock.System{}
	}
	content, err := buildPanelContent(spec, parts)
	if err != nil {
		return ProvisionResult{}, err
	}
	return provisionPlatform(spec, parts, content)
}

// validateProvisionSpec checks the portable shape of the request and returns
// the derived path parts. OwnerUID zero is rejected fail-closed: no panel is
// ever owned by root, and the panel authorizer already excludes root peers.
func validateProvisionSpec(spec ProvisionSpec) (lockPathParts, error) {
	if spec.Root == "" || !filepath.IsAbs(spec.Root) || filepath.Clean(spec.Root) != spec.Root ||
		strings.ContainsRune(spec.Root, 0) || strings.ContainsAny(spec.Root, "\r\n") {
		return lockPathParts{}, errors.New("invalid provision root")
	}
	if spec.Binding.OwnerUID == 0 {
		return lockPathParts{}, errors.New("invalid provision owner uid")
	}
	if !validOwner(spec.Owner) {
		return lockPathParts{}, errors.New("invalid provision owner")
	}
	parts, err := deriveParts(spec.Binding)
	if err != nil {
		return lockPathParts{}, err
	}
	if spec.PanePID <= 0 || spec.PanePID > maxRecordPID {
		return lockPathParts{}, errors.New("invalid provision pane pid")
	}
	if !validStart(strconv.FormatInt(spec.PaneStart, 10)) || spec.PaneStart <= 0 {
		return lockPathParts{}, errors.New("invalid provision pane start")
	}
	if !bootIDPattern.MatchString(spec.BootID) || !hostPattern.MatchString(spec.Host) {
		return lockPathParts{}, errors.New("invalid provision binding identity")
	}
	return parts, nil
}

// buildPanelContent renders the exact bytes provision installs: an empty
// lock, the provision-vector state record and the register record. Both
// records are strict-decoded right after encoding so an encoder drift fails
// here, before touching the filesystem.
func buildPanelContent(spec ProvisionSpec, parts lockPathParts) (panelContent, error) {
	socketSHA := parts.hash
	binding := recordBinding{
		BootID: spec.BootID, Host: spec.Host, Pane: mustPaneNumber(parts),
		PanePID: spec.PanePID, PaneStart: strconv.FormatInt(spec.PaneStart, 10),
		SocketSHA: socketSHA, UID: spec.BindUID,
	}
	timestamp := spec.Clock.Now()
	stateBytes, err := encodeStateRecord(initialState(binding, spec.Owner, timestamp))
	if err != nil {
		return panelContent{}, fmt.Errorf("encode provision state: %w", err)
	}
	registerBytes, err := encodeRegisterRecord(registerRecord{
		Binding: binding, Owner: spec.Owner,
	})
	if err != nil {
		return panelContent{}, fmt.Errorf("encode provision register: %w", err)
	}
	if _, err := decodeStateRecord(stateBytes); err != nil {
		return panelContent{}, fmt.Errorf("self-check provision state: %w", err)
	}
	register, err := decodeRegisterRecord(registerBytes)
	if err != nil {
		return panelContent{}, fmt.Errorf("self-check provision register: %w", err)
	}
	content := panelContent{
		owner: spec.Owner, binding: binding,
		state: stateBytes, register: registerBytes,
	}
	if err := checkPanelCoherence(content, register, mustDecodeState(stateBytes)); err != nil {
		return panelContent{}, err
	}
	return content, nil
}

// mustPaneNumber re-derives the pane number from validated parts. deriveParts
// already proved the digits canonical, so ParseUint cannot fail here.
func mustPaneNumber(parts lockPathParts) uint32 {
	digits := parts.file[:len(parts.file)-len(".lock")]
	number, err := strconv.ParseUint(digits, 10, 31)
	if err != nil {
		panic("tmuxlock: validated pane digits do not parse")
	}
	return uint32(number)
}

// mustDecodeState decodes bytes that were just strict-encoded above.
func mustDecodeState(raw []byte) stateRecord {
	record, err := decodeStateRecord(raw)
	if err != nil {
		panic("tmuxlock: self-encoded state does not decode")
	}
	return record
}

// checkPanelCoherence requires the two authority records to name the same
// owner, epoch, generation and binding the caller requested. It deliberately
// does not pin epoch or generation to zero: after a transfer or repair the
// repeat provision must still recognize the panel it was asked about.
func checkPanelCoherence(content panelContent, register registerRecord, state stateRecord) error {
	if register.Owner != content.owner || state.Owner != content.owner {
		return errors.New("provision panel owner mismatch")
	}
	if register.Epoch != state.Epoch || register.OwnerGen != state.OwnerGen {
		return errors.New("provision panel generation mismatch")
	}
	if register.Binding != content.binding || state.binding() != content.binding {
		return errors.New("provision panel binding mismatch")
	}
	return nil
}
