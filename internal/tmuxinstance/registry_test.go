package tmuxinstance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

const (
	testBoot    = "11111111-2222-3333-4444-555555555555"
	testMachine = "0123456789abcdef0123456789abcdef"
	testSockets = "/tmp/tmux-1001"
	reservedUID = 999

	testUser   = "dev"
	nodeComm   = "node"
	codexComm  = "codex"
	claudeComm = "claude"
	ocComm     = "opencode"
	agentCLI   = "agent-cli"
	cliJS      = "cli.js"
	shellComm  = "bash"
	testSource = "census"
)

type fakeClock struct{ now time.Time }

func (c fakeClock) Now() time.Time { return c.now }

type fakeSystem struct {
	mu       sync.Mutex
	boot     string
	machine  string
	procs    map[int64]procInfo
	kids     map[int64][]int64
	dirs     map[string]dirInfo
	users    map[string]uint32
	procErr  error
	childErr error
}

// newFakeSystem is an npm Codex: the pane runs a node launcher (500) whose
// direct child is the CLI (501); the CLI's own helper (502) is a grandchild.
// Pane %1 runs a native CLI (600), and pane %5 a CLI that node runs from its
// script (700).
func newFakeSystem() *fakeSystem {
	return &fakeSystem{
		boot: testBoot, machine: testMachine,
		procs: map[int64]procInfo{
			500: {ppid: 400, start: 100, uid: 1001, gid: 1001, comm: nodeComm, argv0: nodeComm, argv1: codexComm},
			501: {ppid: 500, start: 120, uid: 1001, gid: 1001, comm: codexComm, argv0: codexComm},
			502: {ppid: 501, start: 130, uid: 1001, gid: 1001, comm: codexComm, argv0: codexComm},
			600: {ppid: 400, start: 140, uid: 1001, gid: 1001, comm: claudeComm, argv0: claudeComm},
			700: {ppid: 400, start: 150, uid: 1001, gid: 1001, comm: nodeComm, argv0: nodeComm, argv1: agentCLI},
		},
		kids: map[int64][]int64{500: {501}, 501: {502}},
		dirs: map[string]dirInfo{
			testSockets: {uid: 1001, perm: 0o700, dir: true},
		},
		users: map[string]uint32{testUser: 1001, "ops": 1002},
	}
}

func (s *fakeSystem) edit(f func(*fakeSystem)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

func (s *fakeSystem) bootID() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boot, nil
}

func (s *fakeSystem) machineID() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.machine, nil
}

func (s *fakeSystem) process(pid int64) (procInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.procErr != nil {
		return procInfo{}, s.procErr
	}
	info, ok := s.procs[pid]
	if !ok {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, errProcessGone)
	}
	return info, nil
}

func (s *fakeSystem) children(pid int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.childErr != nil {
		return nil, s.childErr
	}
	return append([]int64(nil), s.kids[pid]...), nil
}

func (s *fakeSystem) socketDir(socket string) (dirInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.dirs[filepath.Dir(socket)]
	if !ok {
		return dirInfo{}, fmt.Errorf("lstat %s: %w", socket, fs.ErrNotExist)
	}
	return info, nil
}

func (s *fakeSystem) lookupUser(name string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uid, ok := s.users[name]
	if !ok {
		return 0, fmt.Errorf("user %q: %w", name, fs.ErrNotExist)
	}
	return uid, nil
}

type fakePanes struct {
	mu   sync.Mutex
	rows map[string][]PaneRow
}

func (p *fakePanes) ListPanes(_ context.Context, owner, socket, _ string) ([]PaneRow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows, ok := p.rows[owner+" "+socket]
	if !ok {
		return nil, errors.New("no server running")
	}
	return append([]PaneRow(nil), rows...), nil
}

type fixture struct {
	reg    *Registry
	sys    *fakeSystem
	panes  *fakePanes
	name   string
	socket string
}

// openDB is a fresh, migrated node database. Subtests of one table share
// it: each fixture has its own agents and socket, so their rows never meet.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	store, err := eventstore.Open(context.Background(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store.DB()
}

func newFixture(t *testing.T, db *sql.DB, name string) fixture {
	t.Helper()
	socket := testSockets + "/" + name
	sys := newFakeSystem()
	panes := &fakePanes{rows: map[string][]PaneRow{
		testUser + " " + socket: {{PaneID: "%1", PanePID: 600}, {PaneID: "%3", PanePID: 500}, {PaneID: "%5", PanePID: 700}},
	}}
	reg, err := newRegistry(Options{
		DB: db, Clock: fakeClock{now: time.Date(2026, 11, 2, 21, 0, 0, 0, time.UTC)},
		Panes: panes, ExcludedAgents: []protocol.AgentID{"excluded-1"},
		ReservedUIDs: []uint32{reservedUID},
	}, sys)
	require.NoError(t, err)
	return fixture{reg: reg, sys: sys, panes: panes, name: name, socket: socket}
}

func (f fixture) codex() Registration {
	return Registration{
		AgentID: protocol.AgentID("codex-" + f.name), AgentUser: testUser, OwnerUser: testUser,
		Socket: f.socket, PaneID: "%3", PaneComm: nodeComm, HarnessComm: codexComm, HarnessArgv0: codexComm,
		Harness: protocol.HarnessCodex, HarnessVersion: "0.154.0", Source: testSource,
	}
}

func (f fixture) claude() Registration {
	return Registration{
		AgentID: protocol.AgentID("claude-" + f.name), AgentUser: testUser, OwnerUser: testUser,
		Socket: f.socket, PaneID: "%1", PaneComm: claudeComm, HarnessComm: claudeComm, HarnessArgv0: claudeComm,
		Harness: protocol.HarnessClaudeCode, HarnessVersion: "2.1.280", Source: testSource,
	}
}

// script is a CLI that node runs from its script, in pane %5.
func (f fixture) script() Registration {
	return Registration{
		AgentID: protocol.AgentID("script-" + f.name), AgentUser: testUser, OwnerUser: testUser,
		Socket: f.socket, PaneID: "%5", PaneComm: nodeComm, HarnessComm: nodeComm,
		HarnessArgv0: nodeComm, HarnessArgv1: agentCLI,
		Harness: protocol.HarnessOpencode, HarnessVersion: "1.18.29", Source: testSource,
	}
}

// rows are this fixture's instances, ACTIVE and INVALID.
func (f fixture) rows(t *testing.T) []Instance {
	t.Helper()
	all, err := f.reg.List(context.Background())
	require.NoError(t, err)
	var mine []Instance
	for _, inst := range all {
		if inst.Socket == f.socket {
			mine = append(mine, inst)
		}
	}
	return mine
}

func TestRegisterLauncherChild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	inst, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	require.True(t, inst.ID.Valid())
	require.Equal(t, StateActive, inst.State)
	require.Equal(t, int64(500), inst.PanePID)
	require.Equal(t, int64(100), inst.PaneStart)
	require.Equal(t, int64(501), inst.HarnessPID, "the CLI is the launcher's direct child, not its helper")
	require.Equal(t, testBoot, inst.BootID)
	require.Equal(t, testMachine, inst.MachineID)
	require.Equal(t, protocol.ProcessIdentity{PID: 501, ProcessStartTime: 120, UID: 1001, GID: 1001}, inst.Process())

	found, err := f.reg.Lookup(ctx, f.codex().AgentID)
	require.NoError(t, err)
	require.Equal(t, inst, found)
	active, err := f.reg.ListActive(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{inst}, active)
}

func TestRegisterKeepsTheHelperBank(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "bank")
	reg := f.codex()
	reg.Bank = "s02"
	inst, err := f.reg.Register(ctx, reg)
	require.NoError(t, err)
	require.Equal(t, "s02", inst.Bank)

	found, err := f.reg.Lookup(ctx, reg.AgentID)
	require.NoError(t, err)
	require.Equal(t, "s02", found.Bank, "the bank survives the database round trip")
}

func TestRegisterPaneProcessIsTheCLI(t *testing.T) {
	t.Parallel()
	f := newFixture(t, openDB(t), "work")
	inst, err := f.reg.Register(context.Background(), f.claude())
	require.NoError(t, err)
	require.Equal(t, inst.PanePID, inst.HarnessPID)
	require.Equal(t, int64(600), inst.HarnessPID)
}

func TestRegisterRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		reg    func(*Registration)
		system func(*fakeSystem)
		want   error
		code   RefusalCode
	}{
		{name: "an excluded agent", reg: func(r *Registration) { r.AgentID = "excluded-1" }, want: ErrRefused, code: RefusalExcluded},
		{name: "an unknown agent user", reg: func(r *Registration) { r.AgentUser = "nobody" }, want: fs.ErrNotExist, code: RefusalUnknownUser},
		{name: "root as the agent's uid", system: func(s *fakeSystem) {
			s.users[testUser] = 0
			s.procs[501] = procInfo{ppid: 500, start: 120, comm: codexComm}
		}, want: ErrRefused, code: RefusalRootUser},
		{name: "root as the owner's uid", reg: func(r *Registration) { r.OwnerUser = "toor" }, system: func(s *fakeSystem) {
			s.users["toor"] = 0
		}, want: ErrRefused, code: RefusalRootUser},
		{name: "a reserved uid", system: func(s *fakeSystem) { s.users[testUser] = reservedUID }, want: ErrRefused, code: RefusalReservedUID},
		{name: "a symlinked socket directory", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1001, perm: 0o700, dir: true, symlink: true}
		}, want: ErrRefused, code: RefusalSocketDir},
		{name: "a socket directory of another user", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1002, perm: 0o700, dir: true}
		}, want: ErrRefused, code: RefusalSocketDir},
		{name: "a socket directory others can enter", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1001, perm: 0o755, dir: true}
		}, want: ErrRefused, code: RefusalSocketDir},
		{name: "a missing socket directory", system: func(s *fakeSystem) {
			delete(s.dirs, testSockets)
		}, want: ErrRefused, code: RefusalSocketDir},
		{name: "a socket no server listens on", reg: func(r *Registration) { r.Socket = testSockets + "/none" }, code: RefusalTmuxUnreachable},
		{name: "a pane the server does not list", reg: func(r *Registration) { r.PaneID = "%9" }, want: ErrRefused, code: RefusalPaneMissing},
		{name: "a pane whose process is gone", system: func(s *fakeSystem) { delete(s.procs, 500) }, want: ErrRefused, code: RefusalPaneMissing},
		{name: "an unreadable /proc", system: func(s *fakeSystem) { s.procErr = errors.New("proc unreadable") }, code: RefusalInternal},
		{name: "a pane running another process", reg: func(r *Registration) { r.PaneComm = "python3" }, want: ErrRefused, code: RefusalPaneComm},
		{name: "a shell as the CLI's process", reg: func(r *Registration) { r.HarnessComm = shellComm }, want: ErrRefused, code: RefusalShell},
		{name: "a shell in the pane and as the CLI", reg: func(r *Registration) {
			r.PaneComm, r.HarnessComm = shellComm, shellComm
		}, want: ErrRefused, code: RefusalShell},
		{name: "a shell under a launcher", reg: func(r *Registration) { r.HarnessComm = "zsh" }, want: ErrRefused, code: RefusalShell},
		{name: "a shell stated for a pane that runs a launcher", reg: func(r *Registration) { r.PaneComm = shellComm }, want: ErrRefused, code: RefusalPaneComm},
		{name: "no direct child with the CLI's name", reg: func(r *Registration) { r.HarnessComm = "codex-code-mode" }, want: ErrRefused, code: RefusalHarnessMissing},
		{name: "two direct children with the CLI's name", system: func(s *fakeSystem) {
			s.procs[503] = procInfo{ppid: 500, start: 125, uid: 1001, gid: 1001, comm: codexComm}
			s.kids[500] = []int64{501, 503}
		}, want: ErrRefused, code: RefusalHarnessMissing},
		{name: "a CLI of another user", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 500, start: 120, uid: 1002, gid: 1002, comm: codexComm, argv0: codexComm}
		}, want: ErrRefused, code: RefusalAgentUID},
		{name: "a CLI started as another program", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 500, start: 120, uid: 1001, gid: 1001, comm: codexComm, argv0: "codex-code-mode-host"}
		}, want: ErrRefused, code: RefusalHarnessProgram},
		{name: "a stated program the CLI does not run", reg: func(r *Registration) { r.HarnessArgv0 = "codex-cli" }, want: ErrRefused, code: RefusalHarnessProgram},
		{name: "no stated program", reg: func(r *Registration) { r.HarnessArgv0 = "" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "an interpreter without its script", reg: func(r *Registration) {
			r.PaneComm, r.HarnessComm, r.HarnessArgv0 = nodeComm, nodeComm, nodeComm
		}, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "a script for a program that is no interpreter", reg: func(r *Registration) { r.HarnessArgv1 = cliJS }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "an option as the script", reg: func(r *Registration) {
			r.PaneComm, r.HarnessComm, r.HarnessArgv0, r.HarnessArgv1 = nodeComm, nodeComm, nodeComm, "--inspect"
		}, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "a path as the program", reg: func(r *Registration) { r.HarnessArgv0 = "/usr/bin/codex" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "a relative socket", reg: func(r *Registration) { r.Socket = "tmux-1001/work" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "a pane id without %", reg: func(r *Registration) { r.PaneID = "3" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "a zero-padded pane id", reg: func(r *Registration) { r.PaneID = "%03" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "an unknown harness", reg: func(r *Registration) { r.Harness = "vim" }, want: ErrInvalidArgument, code: RefusalInvalid},
		{name: "no harness version", reg: func(r *Registration) { r.HarnessVersion = "" }, want: ErrInvalidArgument, code: RefusalInvalid},
	}
	db := openDB(t)
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, db, fmt.Sprintf("refuse-%d", i))
			reg := f.codex()
			if tt.reg != nil {
				tt.reg(&reg)
			}
			if tt.system != nil {
				f.sys.edit(tt.system)
			}
			_, err := f.reg.Register(context.Background(), reg)
			require.Error(t, err)
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			}
			require.Equal(t, tt.code, RefusalOf(err))
			require.Empty(t, f.rows(t))
			_, err = f.reg.Lookup(context.Background(), reg.AgentID)
			require.ErrorIs(t, err, ErrNotRegistered)
		})
	}
}

func TestRegisterAScriptCLI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "script")
	inst, err := f.reg.Register(ctx, f.script())
	require.NoError(t, err)
	require.Equal(t, int64(700), inst.HarnessPID, "the pane's own node process is the CLI")
	require.Equal(t, nodeComm, inst.HarnessArgv0)
	require.Equal(t, agentCLI, inst.HarnessArgv1)
	found, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, inst, found, "the basenames are stored")

	other := f.script()
	other.AgentID, other.HarnessArgv1 = "script-other", "other-cli"
	f.setPanes(PaneRow{PaneID: "%5", PanePID: 700}, PaneRow{PaneID: "%6", PanePID: 700})
	other.PaneID = "%6"
	_, err = f.reg.Register(ctx, other)
	require.ErrorIs(t, err, ErrRefused, "node running another script")
	require.Equal(t, RefusalHarnessProgram, RefusalOf(err))
	require.NotContains(t, err.Error(), agentCLI, "the error names no argument")
}

func TestVerifyNoticesAnotherScript(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "exec")
	inst, err := f.reg.Register(ctx, f.script())
	require.NoError(t, err)
	obs := PaneObservation{PaneID: inst.PaneID, PanePID: inst.PanePID}
	require.NoError(t, f.reg.Verify(ctx, inst, obs))

	f.sys.edit(func(s *fakeSystem) {
		p := s.procs[700]
		p.argv1 = "other-cli"
		s.procs[700] = p
	})
	require.ErrorIs(t, f.reg.Verify(ctx, inst, obs), ErrInvalidated,
		"an exec keeps the pid, the start time and the comm, not the script")
	rows := f.rows(t)
	require.Len(t, rows, 1)
	require.Equal(t, ReasonHarnessChanged, rows[0].InvalidReason)
}

// TestRegisterMeasuredCLIs uses the comm and argv[0] measured for real CLIs,
// each started from bash in its pane, so that the CLI is the shell's only
// direct child. npm's Codex is node running its codex script. tmux and ps see
// the kernel's 15-byte comm, and argv[0] keeps the whole name.
func TestRegisterMeasuredCLIs(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	for _, cli := range []struct {
		name, comm, argv0, argv1 string
		harness                  protocol.Harness
	}{
		{name: claudeComm, comm: claudeComm, argv0: claudeComm, harness: protocol.HarnessClaudeCode},
		{name: codexComm, comm: nodeComm, argv0: nodeComm, argv1: codexComm, harness: protocol.HarnessCodex},
		{name: "muse", comm: "muse-bin-1.4.13", argv0: "muse-bin-1.4.13-R6996.1", harness: protocol.HarnessMuseCode},
		{name: ocComm, comm: ocComm, argv0: ocComm, harness: protocol.HarnessOpencode},
	} {
		t.Run(cli.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := newFixture(t, db, "measured-"+cli.name)
			f.sys.edit(func(s *fakeSystem) {
				s.procs[800] = procInfo{ppid: 400, start: 160, uid: 1001, gid: 1001, comm: shellComm, argv0: shellComm}
				s.procs[801] = procInfo{ppid: 800, start: 170, uid: 1001, gid: 1001, comm: cli.comm, argv0: cli.argv0, argv1: cli.argv1}
				s.kids[800] = []int64{801}
			})
			f.setPanes(PaneRow{PaneID: "%8", PanePID: 800})
			reg := Registration{
				AgentID: protocol.AgentID(cli.name + "-measured"), AgentUser: testUser, OwnerUser: testUser,
				Socket: f.socket, PaneID: "%8", PaneComm: shellComm, HarnessComm: cli.comm,
				HarnessArgv0: cli.argv0, HarnessArgv1: cli.argv1,
				Harness: cli.harness, HarnessVersion: "1.0", Source: testSource,
			}
			if cli.comm != cli.argv0 {
				truncated := reg
				truncated.HarnessArgv0 = cli.comm
				_, err := f.reg.Register(ctx, truncated)
				require.Equal(t, RefusalHarnessProgram, RefusalOf(err), "the comm is not the whole argv[0]")
			}
			inst, err := f.reg.Register(ctx, reg)
			require.NoError(t, err, "the CLI is the shell's only direct child")
			require.Equal(t, int64(800), inst.PanePID)
			require.Equal(t, int64(801), inst.HarnessPID)
			require.Equal(t, cli.comm, inst.HarnessComm)
			require.Equal(t, cli.argv0, inst.HarnessArgv0)
			require.Equal(t, cli.argv1, inst.HarnessArgv1)
			obs := PaneObservation{PaneID: inst.PaneID, PanePID: inst.PanePID}
			require.NoError(t, f.reg.Verify(ctx, inst, obs))

			f.sys.edit(func(s *fakeSystem) {
				delete(s.procs, 801)
				s.kids[800] = nil
			})
			require.ErrorIs(t, f.reg.Verify(ctx, inst, obs), ErrInvalidated, "the CLI exited and left the shell")
			rows := f.rows(t)
			require.Len(t, rows, 1)
			require.Equal(t, ReasonHarnessChanged, rows[0].InvalidReason)
		})
	}
}

func TestRegisterOneActivePerAgentAndPane(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	first, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)

	_, err = f.reg.Register(ctx, f.codex())
	require.ErrorIs(t, err, ErrRefused, "the same agent twice")
	require.Equal(t, RefusalAlreadyActive, RefusalOf(err))
	other := f.codex()
	other.AgentID = "codex-other"
	_, err = f.reg.Register(ctx, other)
	require.ErrorIs(t, err, ErrRefused, "another agent on the same pane")
	require.Equal(t, RefusalAlreadyActive, RefusalOf(err))

	_, err = f.reg.Revoke(ctx, f.codex().AgentID)
	require.NoError(t, err)
	again, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err, "a revoked instance frees its agent and pane")
	require.NotEqual(t, first.ID, again.ID)
	all, err := f.reg.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	states := map[protocol.ULID]State{all[0].ID: all[0].State, all[1].ID: all[1].State}
	require.Equal(t, map[protocol.ULID]State{first.ID: StateInvalid, again.ID: StateActive}, states)
}

// Two users' tmux servers both have a pane %3. Each registers as its own
// instance, and the one whose CLI exits is the only one invalidated.
func TestRegisterKeepsServersApart(t *testing.T) {
	t.Parallel()
	const otherOwner = "ops" // uid 1002 in newFakeSystem
	ctx := context.Background()
	f := newFixture(t, openDB(t), "apart")
	otherSocket := "/tmp/tmux-1002/" + f.name
	f.sys.edit(func(s *fakeSystem) {
		s.procs[700] = procInfo{ppid: 400, start: 160, uid: 1002, gid: 1002, comm: claudeComm, argv0: claudeComm}
		s.dirs["/tmp/tmux-1002"] = dirInfo{uid: 1002, perm: 0o700, dir: true}
	})
	f.panes.mu.Lock()
	f.panes.rows[otherOwner+" "+otherSocket] = []PaneRow{{PaneID: "%3", PanePID: 700}}
	f.panes.mu.Unlock()
	other := f.claude()
	other.AgentID = protocol.AgentID("claude-" + otherOwner + "-" + f.name)
	other.AgentUser, other.OwnerUser = otherOwner, otherOwner
	other.Socket, other.PaneID = otherSocket, "%3"

	mine, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	theirs, err := f.reg.Register(ctx, other)
	require.NoError(t, err, "the same pane id on another server is another pane")
	for _, want := range []Instance{mine, theirs} {
		found, err := f.reg.Lookup(ctx, want.AgentID)
		require.NoError(t, err)
		require.Equal(t, want, found)
	}
	require.Equal(t, f.socket, mine.Socket)
	require.Equal(t, int64(500), mine.PanePID)
	require.Equal(t, otherSocket, theirs.Socket)
	require.Equal(t, int64(700), theirs.PanePID)

	f.sys.edit(func(s *fakeSystem) { delete(s.procs, 700) })
	require.ErrorIs(t, f.reg.Verify(ctx, theirs, observed(theirs)), ErrInvalidated)
	require.NoError(t, f.reg.Verify(ctx, mine, observed(mine)))
	active, err := f.reg.ListActive(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{mine}, active)
}

func observed(inst Instance) PaneObservation {
	return PaneObservation{PaneID: inst.PaneID, PanePID: inst.PanePID, Width: 120, Height: 40}
}

func TestVerifyInvalidates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		obs    func(*PaneObservation)
		system func(*fakeSystem)
		want   Reason
	}{
		{name: "tmux no longer finds the pane", obs: func(o *PaneObservation) { o.PaneID = "" }, want: ReasonPaneGone},
		{name: "another pane id", obs: func(o *PaneObservation) { o.PaneID = "%4" }, want: ReasonPaneGone},
		{name: "another pane process", obs: func(o *PaneObservation) { o.PanePID = 777 }, want: ReasonPanePIDChanged},
		{name: "a reboot", system: func(s *fakeSystem) { s.boot = "99999999-2222-3333-4444-555555555555" }, want: ReasonBootChanged},
		{name: "a socket directory of another owner", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1002, perm: 0o700, dir: true}
		}, want: ReasonOwnerChanged},
		{name: "a socket directory others can enter", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1001, perm: 0o755, dir: true}
		}, want: ReasonSocketChanged},
		{name: "a socket directory replaced by a symlink", system: func(s *fakeSystem) {
			s.dirs[testSockets] = dirInfo{uid: 1001, perm: 0o700, dir: true, symlink: true}
		}, want: ReasonSocketChanged},
		{name: "a socket directory gone", system: func(s *fakeSystem) {
			delete(s.dirs, testSockets)
		}, want: ReasonSocketChanged},
		{name: "the pane process exited", system: func(s *fakeSystem) { delete(s.procs, 500) }, want: ReasonPaneGone},
		{name: "the pane PID reused", system: func(s *fakeSystem) {
			s.procs[500] = procInfo{ppid: 400, start: 900, uid: 1001, gid: 1001, comm: nodeComm, argv0: nodeComm, argv1: codexComm}
		}, want: ReasonPaneStartChanged},
		{name: "the pane process ran another program", system: func(s *fakeSystem) {
			s.procs[500] = procInfo{ppid: 400, start: 100, uid: 1001, gid: 1001, comm: shellComm, argv0: shellComm}
		}, want: ReasonHarnessChanged},
		{name: "the CLI exited", system: func(s *fakeSystem) { delete(s.procs, 501) }, want: ReasonHarnessChanged},
		{name: "the CLI PID reused", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 500, start: 950, uid: 1001, gid: 1001, comm: codexComm, argv0: codexComm}
		}, want: ReasonHarnessChanged},
		{name: "the CLI changed user", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 500, start: 120, uid: 1002, gid: 1001, comm: codexComm, argv0: codexComm}
		}, want: ReasonHarnessChanged},
		{name: "the CLI left the launcher", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 1, start: 120, uid: 1001, gid: 1001, comm: codexComm, argv0: codexComm}
		}, want: ReasonHarnessChanged},
		{name: "the CLI ran another program of the same name", system: func(s *fakeSystem) {
			s.procs[501] = procInfo{ppid: 500, start: 120, uid: 1001, gid: 1001, comm: codexComm, argv0: "codex-code-mode-host"}
		}, want: ReasonHarnessChanged},
	}
	db := openDB(t)
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := newFixture(t, db, fmt.Sprintf("verify-%d", i))
			inst, err := f.reg.Register(ctx, f.codex())
			require.NoError(t, err)
			require.NoError(t, f.reg.Verify(ctx, inst, observed(inst)))

			obs := observed(inst)
			if tt.obs != nil {
				tt.obs(&obs)
			}
			if tt.system != nil {
				f.sys.edit(tt.system)
			}
			err = f.reg.Verify(ctx, inst, obs)
			require.ErrorIs(t, err, ErrInvalidated)
			require.ErrorContains(t, err, string(tt.want))

			all := f.rows(t)
			require.Len(t, all, 1)
			require.Equal(t, StateInvalid, all[0].State)
			require.Equal(t, tt.want, all[0].InvalidReason)
			_, err = f.reg.Lookup(ctx, inst.AgentID)
			require.ErrorIs(t, err, ErrNotRegistered)
			require.ErrorIs(t, f.reg.Verify(ctx, inst, observed(inst)), ErrInvalidated, "INVALID is final")
			require.ErrorIs(t, f.reg.BindSession(ctx, inst.ID, "ses-1"), ErrInvalidated)
		})
	}
}

func TestVerifyReadErrorKeepsTheInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	inst, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	f.sys.edit(func(s *fakeSystem) { s.procErr = fs.ErrPermission })

	err = f.reg.Verify(ctx, inst, observed(inst))
	require.ErrorIs(t, err, fs.ErrPermission)
	require.NotErrorIs(t, err, ErrInvalidated)
	found, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, StateActive, found.State)
}

func TestReportPaneLockIncidentInvalidatesOnlyTheExactBinding(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		reason tmuxlock.GuardReason
		want   Reason
	}{
		"mode":          {reason: tmuxlock.GuardModeInvalid, want: ReasonPaneModeInvalid},
		"inventory":     {reason: tmuxlock.GuardInventoryInvalid, want: ReasonPaneInventoryInvalid},
		"panel":         {reason: tmuxlock.GuardPanelInvalid, want: ReasonPanePanelInvalid},
		"register":      {reason: tmuxlock.GuardRegisterInvalid, want: ReasonPaneRegisterInvalid},
		"state":         {reason: tmuxlock.GuardStateInvalid, want: ReasonPaneStateInvalid},
		"owner copy":    {reason: tmuxlock.GuardOwnerCopyMismatch, want: ReasonPaneOwnerCopyMismatch},
		"state binding": {reason: tmuxlock.GuardStateBindingMismatch, want: ReasonPaneStateBindingMismatch},
		"live binding":  {reason: tmuxlock.GuardBindingMismatch, want: ReasonPaneBindingMismatch},
		"generation":    {reason: tmuxlock.GuardGenerationExhausted, want: ReasonPaneGenerationExhausted},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			f := newFixture(t, openDB(t), "incident")
			inst, err := f.reg.Register(ctx, f.codex())
			require.NoError(t, err)
			binding := tmuxlock.LiveBinding{
				PanelBinding: tmuxlock.PanelBinding{
					OwnerUID: inst.OwnerUID, Socket: inst.Socket, PaneID: string(inst.PaneID),
				},
				Host: "node-1", BootID: inst.BootID,
				PanePID: inst.PanePID, PaneStart: inst.PaneStart,
			}

			stale := binding
			stale.PaneStart++
			require.ErrorIs(t,
				f.reg.ReportPaneLockIncident(ctx, stale, test.reason), ErrNotRegistered)
			active, err := f.reg.Lookup(ctx, inst.AgentID)
			require.NoError(t, err)
			require.Equal(t, StateActive, active.State)

			require.NoError(t, f.reg.ReportPaneLockIncident(ctx, binding, test.reason))
			rows := f.rows(t)
			require.Len(t, rows, 1)
			require.Equal(t, StateInvalid, rows[0].State)
			require.Equal(t, test.want, rows[0].InvalidReason)
		})
	}
}

func TestReportPaneLockIncidentRejectsDeferredReason(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	f := newFixture(t, openDB(t), "incident-deferred")
	inst, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	binding := tmuxlock.LiveBinding{
		PanelBinding: tmuxlock.PanelBinding{
			OwnerUID: inst.OwnerUID, Socket: inst.Socket, PaneID: string(inst.PaneID),
		},
		Host: "node-1", BootID: inst.BootID,
		PanePID: inst.PanePID, PaneStart: inst.PaneStart,
	}

	require.ErrorIs(t,
		f.reg.ReportPaneLockIncident(ctx, binding, tmuxlock.GuardPending), ErrInvalidArgument)
	active, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, StateActive, active.State)
}

func TestVerifyRefusesAStaleCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	inst, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	stale := inst
	stale.HarnessPID = 502

	require.ErrorIs(t, f.reg.Verify(ctx, stale, observed(stale)), ErrInvalidArgument)
	found, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, StateActive, found.State)
}

func TestSweepReturnsOnlyWhatItInvalidated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	codex, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)
	claude, err := f.reg.Register(ctx, f.claude())
	require.NoError(t, err)

	gone, err := f.reg.Sweep(ctx)
	require.NoError(t, err)
	require.Empty(t, gone)

	f.sys.edit(func(s *fakeSystem) { delete(s.procs, 501) })
	gone, err = f.reg.Sweep(ctx)
	require.NoError(t, err)
	require.Len(t, gone, 1)
	require.Equal(t, codex.ID, gone[0].ID)
	require.Equal(t, ReasonHarnessChanged, gone[0].InvalidReason)

	gone, err = f.reg.Sweep(ctx)
	require.NoError(t, err)
	require.Empty(t, gone, "an INVALID instance is reported once")
	active, err := f.reg.ListActive(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{claude}, active)
}

func TestBindSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	inst, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)

	require.NoError(t, f.reg.BindSession(ctx, inst.ID, "ses-1"))
	found, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, protocol.OpaqueID("ses-1"), found.SessionID)
	require.NoError(t, f.reg.Verify(ctx, inst, observed(inst)), "a bound session does not change the identity")

	require.ErrorIs(t, f.reg.BindSession(ctx, inst.ID, "not a session"), ErrInvalidArgument)
	require.ErrorIs(t, f.reg.BindSession(ctx, "01JC0000000000000000000000", "ses-1"), ErrNotRegistered)
}

func TestRevoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "work")
	_, err := f.reg.Register(ctx, f.codex())
	require.NoError(t, err)

	revoked, err := f.reg.Revoke(ctx, f.codex().AgentID)
	require.NoError(t, err)
	require.Equal(t, StateInvalid, revoked.State)
	require.Equal(t, ReasonAdminRevoked, revoked.InvalidReason)
	_, err = f.reg.Revoke(ctx, f.codex().AgentID)
	require.ErrorIs(t, err, ErrNotRegistered)
}

func TestCanonicalPane(t *testing.T) {
	t.Parallel()
	for pane, want := range map[protocol.TmuxPaneID]bool{
		"%0": true, "%7": true, "%10": true, "%123456789": true,
		"%00": false, "%07": false, "%1234567890": false, "7": false, "%": false, "%-1": false,
	} {
		require.Equal(t, want, canonicalPane(pane), pane)
	}
}
