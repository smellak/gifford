package tmuxinstance

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	sudoTestComm  = "sudo"
	vimTestComm   = "vim"
	sleepTestComm = "sleep"
)

// sudoFixture is the sudo launcher monitor pane: sudo (800) whose only child is sudo (801),
// whose only child is bash (802), whose only child is the CLI (803).
func sudoFixture(t *testing.T) (fixture, Registration) {
	t.Helper()
	db := openDB(t)
	f := newFixture(t, db, sudoTestComm)
	f.panes.mu.Lock()
	f.panes.rows[testUser+" "+f.socket] = append(
		f.panes.rows[testUser+" "+f.socket],
		PaneRow{PaneID: "%7", PanePID: 800},
	)
	f.panes.mu.Unlock()
	f.sys.edit(func(s *fakeSystem) {
		s.procs[800] = procInfo{ppid: 400, start: 200, uid: 1001, gid: 1001, euid: 0, egid: 0, comm: sudoTestComm, argv0: sudoTestComm}
		s.procs[801] = procInfo{ppid: 800, start: 210, uid: 0, gid: 0, euid: 0, egid: 0, comm: sudoTestComm, argv0: sudoTestComm}
		s.procs[802] = procInfo{ppid: 801, start: 220, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: shellComm, argv0: shellComm}
		s.procs[803] = procInfo{ppid: 802, start: 230, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: claudeComm, argv0: claudeComm}
		s.kids[800] = []int64{801}
		s.kids[801] = []int64{802}
		s.kids[802] = []int64{803}
	})
	reg := Registration{
		AgentID: protocol.AgentID("sudo-" + f.name), AgentUser: "ops", OwnerUser: testUser,
		Socket: f.socket, PaneID: "%7", PaneComm: sudoTestComm,
		HarnessComm: claudeComm, HarnessArgv0: claudeComm,
		Harness: protocol.HarnessClaudeCode, HarnessVersion: "2.1.280",
		MonitorProfile: MonitorProfileSudoShell, Source: testSource,
	}
	return f, reg
}

func sudoObservation() PaneObservation {
	return PaneObservation{PaneID: "%7", PanePID: 800}
}

func TestRegisterSudoMonitorChain(t *testing.T) {
	t.Parallel()
	f, reg := sudoFixture(t)
	ctx := context.Background()
	inst, err := f.reg.Register(ctx, reg)
	require.NoError(t, err)
	require.Equal(t, StateActive, inst.State)
	require.Equal(t, MonitorProfileSudoShell, inst.MonitorProfile)
	require.Equal(t, int64(803), inst.HarnessPID)
	require.Equal(t, int64(230), inst.HarnessStart)

	stored, err := f.reg.Lookup(ctx, inst.AgentID)
	require.NoError(t, err)
	require.Equal(t, MonitorProfileSudoShell, stored.MonitorProfile)

	nodes, err := f.reg.selectMonitorChain(ctx, inst.ID)
	require.NoError(t, err)
	require.Len(t, nodes, sudoChainLength)
	require.Equal(t, []int64{800, 801, 802, 803},
		[]int64{nodes[0].pid, nodes[1].pid, nodes[2].pid, nodes[3].pid})
	require.Equal(t, []string{sudoTestComm, sudoTestComm, shellComm, claudeComm},
		[]string{nodes[0].comm, nodes[1].comm, nodes[2].comm, nodes[3].comm})
	require.Equal(t, uint32(0), nodes[0].euid, "the pane sudo runs effectively as root")
	require.Equal(t, []int64{400, 800, 801, 802},
		[]int64{nodes[0].ppid, nodes[1].ppid, nodes[2].ppid, nodes[3].ppid})

	require.NoError(t, f.reg.Verify(ctx, inst, sudoObservation()))
}

func TestRegisterSudoChainLinkFailureRollsBack(t *testing.T) {
	t.Parallel()
	f, reg := sudoFixture(t)
	ctx := context.Background()
	_, err := f.reg.db.ExecContext(ctx, `CREATE TRIGGER fail_third_monitor_link
		BEFORE INSERT ON tmux_monitor_chains WHEN NEW.position = 2
		BEGIN SELECT RAISE(ABORT, 'third link refused'); END`)
	require.NoError(t, err)
	_, err = f.reg.Register(ctx, reg)
	require.Error(t, err, "the third link insert must fail the registration")
	var instances, links int
	require.NoError(t, f.reg.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tmux_instances`).Scan(&instances))
	require.NoError(t, f.reg.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tmux_monitor_chains`).Scan(&links))
	require.Zero(t, instances, "a failed registration keeps no instance row")
	require.Zero(t, links, "a failed registration keeps no chain rows")
}

func TestRegisterSudoMonitorChainRefusals(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		reg    func(*Registration)
		system func(*fakeSystem)
		code   RefusalCode
	}{
		{"second link is not sudo", nil, func(s *fakeSystem) {
			s.procs[801] = procInfo{ppid: 800, start: 210, uid: 1001, gid: 1001, euid: 1001, egid: 1001, comm: shellComm, argv0: shellComm}
		}, RefusalChainShape},
		{"third link is not a shell", nil, func(s *fakeSystem) {
			s.procs[802] = procInfo{ppid: 801, start: 220, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: vimTestComm, argv0: vimTestComm}
		}, RefusalChainShape},
		{"fourth link is not the CLI", nil, func(s *fakeSystem) {
			s.procs[803] = procInfo{ppid: 802, start: 230, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: "htop", argv0: "htop"}
		}, RefusalChainShape},
		{"extra child at the pane", nil, func(s *fakeSystem) {
			s.procs[804] = procInfo{ppid: 800, start: 240, uid: 1001, gid: 1001, euid: 1001, egid: 1001, comm: sleepTestComm, argv0: sleepTestComm}
			s.kids[800] = []int64{801, 804}
		}, RefusalChainShape},
		{"extra child at the shell", nil, func(s *fakeSystem) {
			s.procs[804] = procInfo{ppid: 802, start: 240, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: sleepTestComm, argv0: sleepTestComm}
			s.kids[802] = []int64{803, 804}
		}, RefusalChainShape},
		{"missing shell link", nil, func(s *fakeSystem) {
			s.procs[803] = procInfo{ppid: 801, start: 230, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: claudeComm, argv0: claudeComm}
			s.kids[801] = []int64{803}
			delete(s.kids, 802)
			delete(s.procs, 802)
		}, RefusalChainShape},
		{"missing sudo link", nil, func(s *fakeSystem) {
			s.procs[802] = procInfo{ppid: 800, start: 220, uid: 1002, gid: 1002, euid: 1002, egid: 1002, comm: shellComm, argv0: shellComm}
			s.kids[800] = []int64{802}
			delete(s.kids, 801)
			delete(s.procs, 801)
		}, RefusalChainShape},
		{"same tree without the profile", func(r *Registration) {
			r.MonitorProfile = ""
		}, nil, RefusalHarnessMissing},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, reg := sudoFixture(t)
			if test.reg != nil {
				test.reg(&reg)
			}
			if test.system != nil {
				f.sys.edit(test.system)
			}
			_, err := f.reg.Register(context.Background(), reg)
			require.ErrorIs(t, err, ErrRefused)
			require.Equal(t, test.code, RefusalOf(err))
		})
	}
}

func TestRegisterSudoMonitorChainUnknownProfile(t *testing.T) {
	t.Parallel()
	f, reg := sudoFixture(t)
	reg.MonitorProfile = "sudo_monitor_shell_v9"
	_, err := f.reg.Register(context.Background(), reg)
	require.ErrorIs(t, err, ErrInvalidArgument)
	require.Equal(t, RefusalInvalid, RefusalOf(err))
}

func TestVerifySudoMonitorChainInvalidates(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*fakeSystem)
	}{
		{"uid changes on the shell", func(s *fakeSystem) {
			info := s.procs[802]
			info.uid = 1003
			s.procs[802] = info
		}},
		{"effective uid changes on sudo", func(s *fakeSystem) {
			info := s.procs[801]
			info.euid = 1002
			s.procs[801] = info
		}},
		{"effective gid changes on the pane", func(s *fakeSystem) {
			info := s.procs[800]
			info.egid = 1001
			s.procs[800] = info
		}},
		{"pid reuse with another start", func(s *fakeSystem) {
			info := s.procs[803]
			info.start = 231
			s.procs[803] = info
		}},
		{"comm changes on the shell", func(s *fakeSystem) {
			info := s.procs[802]
			info.comm, info.argv0 = "dash", "dash"
			s.procs[802] = info
		}},
		{"edge breaks by reparenting", func(s *fakeSystem) {
			info := s.procs[803]
			info.ppid = 800
			s.procs[803] = info
		}},
		{"link disappears", func(s *fakeSystem) {
			delete(s.procs, 802)
		}},
		{"CLI program changes", func(s *fakeSystem) {
			info := s.procs[803]
			info.argv0 = "claude-new"
			s.procs[803] = info
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, reg := sudoFixture(t)
			ctx := context.Background()
			inst, err := f.reg.Register(ctx, reg)
			require.NoError(t, err)
			f.sys.edit(test.mutate)
			err = f.reg.Verify(ctx, inst, sudoObservation())
			require.ErrorIs(t, err, ErrInvalidated)
			stored, gerr := f.reg.get(ctx, inst.ID)
			require.NoError(t, gerr)
			require.Equal(t, StateInvalid, stored.State)
			require.Equal(t, ReasonChainChanged, stored.InvalidReason)
		})
	}
}

func TestSweepInvalidatesDriftedChain(t *testing.T) {
	t.Parallel()
	f, reg := sudoFixture(t)
	ctx := context.Background()
	inst, err := f.reg.Register(ctx, reg)
	require.NoError(t, err)
	f.sys.edit(func(s *fakeSystem) {
		info := s.procs[801]
		info.start = 211
		s.procs[801] = info
	})
	invalidated, err := f.reg.Sweep(ctx)
	require.NoError(t, err)
	require.Len(t, invalidated, 1)
	require.Equal(t, inst.ID, invalidated[0].ID)
	require.Equal(t, ReasonChainChanged, invalidated[0].InvalidReason)
}

func TestStatementsParseMonitorProfile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	content := `schema_version: 1
instances:
- agent_id: sudo-statement
  agent_user: ops
  owner_user: dev
  socket: /tmp/tmux-1001/sudo
  pane_id: "%7"
  pane_comm: sudo
  harness_comm: claude
  harness_argv0: claude
  harness: claude-code
  harness_version: 2.1.280
  monitor_profile: sudo_monitor_shell_v1
  generation: 1
`
	path := filepath.Join(dir, "instances.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	regs, err := LoadStatements(path)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, MonitorProfileSudoShell, regs[0].MonitorProfile)

	bad := `schema_version: 1
instances:
- agent_id: sudo-statement
  agent_user: ops
  owner_user: dev
  socket: /tmp/tmux-1001/sudo
  pane_id: "%7"
  pane_comm: sudo
  harness_comm: claude
  harness_argv0: claude
  harness: claude-code
  harness_version: 2.1.280
  monitor_profile: sudo_monitor_shell_v9
  generation: 1
`
	badPath := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(badPath, []byte(bad), 0o600))
	_, err = LoadStatements(badPath)
	require.ErrorIs(t, err, ErrInvalidArgument)
}
