package tmuxinstance

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// State is where an instance is in its life.
type State string

// State values.
const (
	StateActive  State = "ACTIVE"
	StateInvalid State = "INVALID"
)

// Reason says why an instance stopped being ACTIVE.
type Reason string

// Reason values.
const (
	ReasonBootChanged      Reason = "boot_changed"
	ReasonSocketChanged    Reason = "socket_changed"
	ReasonOwnerChanged     Reason = "owner_changed"
	ReasonPaneGone         Reason = "pane_gone"
	ReasonPanePIDChanged   Reason = "pane_pid_changed"
	ReasonPaneStartChanged Reason = "pane_start_changed"
	ReasonHarnessChanged   Reason = "harness_changed"
	ReasonAdminRevoked     Reason = "admin_revoked"
)

var (
	// ErrNotRegistered means the agent or instance has no ACTIVE row.
	ErrNotRegistered = errors.New("no active tmux instance")
	// ErrInvalidated means the instance is INVALID: nothing may be written
	// to its pane, and its session must end.
	ErrInvalidated = errors.New("tmux instance invalidated")
	// ErrRefused means a registration does not match what tmux and /proc
	// show, or names an agent or uid the node never registers.
	ErrRefused = errors.New("tmux instance registration refused")
	// ErrInvalidArgument means a malformed registration, instance or
	// observation.
	ErrInvalidArgument = errors.New("invalid tmux instance argument")
)

// Instance is one agent CLI running in a tmux pane on this machine.
type Instance struct {
	ID        protocol.ULID
	AgentID   protocol.AgentID
	MachineID string
	BootID    string
	// OwnerUser owns the tmux server. It decides the sudo user and the
	// socket, and is never the agent's identity.
	OwnerUser string
	OwnerUID  uint32
	// Socket is the absolute path of the tmux server socket.
	Socket  string
	PaneID  protocol.TmuxPaneID
	PanePID int64
	// PaneStart is the start time of PanePID, in clock ticks after boot.
	PaneStart int64
	PaneComm  string
	Harness   protocol.Harness
	// HarnessVersion is stated at registration, never guessed.
	HarnessVersion string
	HarnessPID     int64
	HarnessStart   int64
	HarnessComm    string
	// HarnessArgv0 and HarnessArgv1 are all the registry keeps of the CLI's
	// command line: the basename of argv[0], and of argv[1] when argv[0] is
	// an interpreter.
	HarnessArgv0 string
	HarnessArgv1 string
	// AgentUID and AgentGID are the CLI's own: the agent's identity.
	AgentUID uint32
	AgentGID uint32
	// SessionID is the tmux_attach session bound by BindSession, if any.
	SessionID     protocol.OpaqueID
	State         State
	InvalidReason Reason
}

// Process is the identity of the agent's CLI process, for the session's
// process_json.
func (i Instance) Process() protocol.ProcessIdentity {
	return protocol.ProcessIdentity{
		PID: i.HarnessPID, ProcessStartTime: i.HarnessStart,
		UID: int64(i.AgentUID), GID: int64(i.AgentGID),
	}
}

// PaneRow is one pane that tmux list-panes printed for a server.
type PaneRow struct {
	PaneID  protocol.TmuxPaneID
	PanePID int64
}

// PaneLister lists the panes of one tmux server, run as its owner.
type PaneLister interface {
	ListPanes(ctx context.Context, owner, socket string) ([]PaneRow, error)
}

// PaneObservation is what tmux showed for the pane under the per-pane lock,
// right before a write. An empty PaneID means tmux no longer finds the pane.
// Width and Height are the caller's; Verify does not compare them.
type PaneObservation struct {
	PaneID  protocol.TmuxPaneID
	PanePID int64
	Width   int
	Height  int
}

// Registration is what the administrator states about one agent instance,
// from the census. Register checks every field against tmux and /proc.
type Registration struct {
	AgentID protocol.AgentID
	// AgentUser runs the agent's CLI; OwnerUser owns the tmux server.
	AgentUser string
	OwnerUser string
	// Socket is the absolute path of the tmux server socket.
	Socket string
	PaneID protocol.TmuxPaneID
	// PaneComm names the pane's own process and HarnessComm the CLI's. They
	// are equal when the CLI is the pane's process, and differ for a
	// launcher that keeps the CLI as its direct child.
	PaneComm    string
	HarnessComm string
	// HarnessArgv0 is the basename of the CLI's argv[0]. When it is an
	// interpreter (node, bun, python), HarnessArgv1 is the basename of its
	// program, argv[1]; otherwise HarnessArgv1 is empty.
	HarnessArgv0   string
	HarnessArgv1   string
	Harness        protocol.Harness
	HarnessVersion string
	// Source says where the registration came from.
	Source string
	// RegisteredBy is the administrator's uid: the owner of the statements
	// file.
	RegisteredBy uint32
}

var (
	userPattern   = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
	commPattern   = regexp.MustCompile(`^[[:graph:]]{1,15}$`)
	textPattern   = regexp.MustCompile(`^[[:graph:]]{1,128}$`)
	socketPattern = regexp.MustCompile(`^/[[:graph:]]{1,106}$`)
)

// shells are never accepted as the CLI's process: a notice typed into a
// shell would run as a command. A shell may hold the pane, with the CLI as
// its only direct child of the stated name, as when the CLI was started from
// a shell prompt. Then the notice still never reaches the shell as a
// command: Verify, before every write, invalidates the instance once that
// child exits or changes; the writer types only while the CLI holds the
// pane's foreground and is not stopped; and it presses Enter only after it
// verified the typed line. At worst an unfinished line, without Enter, is
// left in the shell.
var shells = map[string]struct{}{
	"ash": {}, "bash": {}, "busybox": {}, "csh": {}, "dash": {}, "fish": {},
	"ksh": {}, "mksh": {}, "sh": {}, "tcsh": {}, "zsh": {},
}

// validProgram accepts what programOf can return for a CLI: a basename as
// argv0, and a basename that is no option as argv1 exactly when argv0 is an
// interpreter.
func validProgram(argv0, argv1 string) bool {
	if !validBasename(argv0) {
		return false
	}
	if !interpreterPattern.MatchString(argv0) {
		return argv1 == ""
	}
	return validBasename(argv1) && argv1[0] != '-'
}

// validBasename accepts a file name of 1 to 128 printable bytes: no space,
// no slash, and neither . nor ..
func validBasename(name string) bool {
	return textPattern.MatchString(name) && !strings.Contains(name, "/") && name != "." && name != ".."
}

// canonicalPane accepts the pane ids tmux prints: %0, or digits without a
// leading zero. The per-pane lock is keyed by that form, so a registered
// pane must have exactly one spelling.
func canonicalPane(pane protocol.TmuxPaneID) bool {
	return pane.Valid() && (len(pane) == 2 || pane[1] != '0')
}

func (r Registration) validate() error {
	var errs []error
	if !r.AgentID.Valid() {
		errs = append(errs, errors.New("agent id"))
	}
	if !userPattern.MatchString(r.AgentUser) || !userPattern.MatchString(r.OwnerUser) {
		errs = append(errs, errors.New("user name"))
	}
	if !socketPattern.MatchString(r.Socket) || filepath.Clean(r.Socket) != r.Socket {
		errs = append(errs, errors.New("socket must be a clean absolute path"))
	}
	if !canonicalPane(r.PaneID) {
		errs = append(errs, errors.New("pane id"))
	}
	if !commPattern.MatchString(r.PaneComm) || !commPattern.MatchString(r.HarnessComm) {
		errs = append(errs, errors.New("process name"))
	}
	if !validProgram(r.HarnessArgv0, r.HarnessArgv1) {
		errs = append(errs, errors.New("harness argv0 and argv1"))
	}
	if !r.Harness.Valid() {
		errs = append(errs, errors.New("harness"))
	}
	if !textPattern.MatchString(r.HarnessVersion) || !textPattern.MatchString(r.Source) {
		errs = append(errs, errors.New("harness version or source"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("registration: %w: %w", ErrInvalidArgument, errors.Join(errs...))
	}
	if _, shell := shells[r.HarnessComm]; shell {
		return coded(RefusalShell, fmt.Errorf("registration: CLI process %q is a shell: %w", r.HarnessComm, ErrRefused))
	}
	return nil
}
