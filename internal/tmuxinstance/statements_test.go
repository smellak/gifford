package tmuxinstance

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const statementsName = "instances.yaml"

// statementEntry renders one instances.yaml entry for fixture f, for a
// native CLI: its argv[0] is its comm.
func statementEntry(f fixture, agent, pane, paneComm, harnessComm, harness, generation string) string {
	return "  - agent_id: " + agent + "\n" +
		"    agent_user: " + testUser + "\n    owner_user: " + testUser + "\n" +
		"    socket: " + f.socket + "\n    pane_id: \"" + pane + "\"\n" +
		"    pane_comm: " + paneComm + "\n    harness_comm: " + harnessComm + "\n" +
		"    harness_argv0: " + harnessComm + "\n" +
		"    harness: " + harness + "\n    harness_version: \"1.0\"\n" +
		"    generation: " + generation + "\n"
}

// privateDir is a directory only its owner may write: t.TempDir follows the
// umask, which may let the group write.
func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	return dir
}

func writeStatements(t *testing.T, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(privateDir(t), statementsName)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

func TestLoadStatements(t *testing.T) {
	t.Parallel()
	f := fixture{name: "load", socket: testSockets + "/load"}
	body := "schema_version: 1\ninstances:\n" +
		statementEntry(f, "codex-load", "%3", nodeComm, codexComm, "codex", "1") +
		statementEntry(f, "claude-load", "%1", claudeComm, claudeComm, "claude-code", "1")
	regs, err := LoadStatements(writeStatements(t, body, 0o600))
	require.NoError(t, err)
	require.Len(t, regs, 2)
	require.Equal(t, protocol.AgentID("codex-load"), regs[0].AgentID)
	require.Equal(t, protocol.TmuxPaneID("%3"), regs[0].PaneID)
	require.Equal(t, uint32(os.Geteuid()), regs[0].RegisteredBy, "the file's owner is the administrator")
	require.True(t, strings.HasPrefix(regs[0].Source, statementSourcePrefix))
	require.NotEqual(t, regs[0].Source, regs[1].Source)

	again, err := LoadStatements(writeStatements(t, body, 0o644))
	require.NoError(t, err, "others may read it")
	require.Equal(t, regs, again, "the same statements give the same sources")
	bumped, err := LoadStatements(writeStatements(t, "schema_version: 1\ninstances:\n"+
		statementEntry(f, "codex-load", "%3", nodeComm, codexComm, "codex", "2"), 0o600))
	require.NoError(t, err)
	require.NotEqual(t, regs[0].Source, bumped[0].Source, "a bumped generation is a new statement")

	empty, err := LoadStatements(writeStatements(t, "schema_version: 1\ninstances: []\n", 0o600))
	require.NoError(t, err)
	require.Empty(t, empty)

	_, err = LoadStatements(filepath.Join(t.TempDir(), statementsName))
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestLoadStatementsBank(t *testing.T) {
	t.Parallel()
	f := fixture{name: "bank", socket: testSockets + "/bank"}
	entry := statementEntry(f, "codex-bank", "%3", nodeComm, codexComm, "codex", "1")
	plain := "schema_version: 1\ninstances:\n" + entry
	regs, err := LoadStatements(writeStatements(t, plain, 0o600))
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Empty(t, regs[0].Bank, "bank is optional")

	banked := strings.Replace(plain, "    pane_comm: node\n",
		"    pane_comm: node\n    bank: s02\n", 1)
	regs, err = LoadStatements(writeStatements(t, banked, 0o600))
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, "s02", regs[0].Bank)

	for name, body := range map[string]string{
		"an uppercase bank": strings.Replace(banked, "bank: s02", "bank: S02", 1),
		"a bank with space": strings.Replace(banked, "bank: s02", "bank: s 02", 1),
		"an overlong bank":  strings.Replace(banked, "bank: s02", "bank: "+strings.Repeat("a", 33), 1),
		"an unknown field":  strings.Replace(banked, "bank: s02", "bank: s02\n    zone: a", 1),
	} {
		_, err := LoadStatements(writeStatements(t, body, 0o600))
		require.Error(t, err, name)
	}
}

func TestLoadAScriptStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "script-load")
	entry := strings.Replace(statementEntry(f, "script-load", "%5", nodeComm, nodeComm, ocComm, "1"),
		"harness_argv0: node\n", "harness_argv0: node\n    harness_argv1: "+agentCLI+"\n", 1)
	regs := loadFor(t, entry)
	require.Equal(t, nodeComm, regs[0].HarnessArgv0)
	require.Equal(t, agentCLI, regs[0].HarnessArgv1)
	got, err := f.reg.Reconcile(ctx, regs)
	require.NoError(t, err)
	require.Len(t, got.Registered, 1)

	other := loadFor(t, strings.Replace(entry, agentCLI, "other-cli", 1))
	require.NotEqual(t, regs[0].Source, other[0].Source, "the script is part of the statement")
	got, err = f.reg.Reconcile(ctx, other)
	require.NoError(t, err)
	require.Len(t, got.Revoked, 1)
	require.Equal(t, []Refusal{{AgentID: "script-load", Code: RefusalHarnessProgram}}, got.Refused)
}

func TestLoadStatementsUnderAShell(t *testing.T) {
	t.Parallel()
	f := fixture{name: "shell", socket: testSockets + "/shell"}
	codex := strings.Replace(statementEntry(f, "codex-shell", "%2", shellComm, nodeComm, "codex", "1"),
		"harness_argv0: node\n", "harness_argv0: node\n    harness_argv1: codex\n", 1)
	muse := strings.Replace(statementEntry(f, "muse-shell", "%4", shellComm, "muse-bin-1.4.13", "muse-code", "1"),
		"harness_argv0: muse-bin-1.4.13\n", "harness_argv0: muse-bin-1.4.13-R6996.1\n", 1)
	regs, err := LoadStatements(writeStatements(t, "schema_version: 1\ninstances:\n"+
		statementEntry(f, "claude-shell", "%1", shellComm, claudeComm, "claude-code", "1")+codex+
		statementEntry(f, "opencode-shell", "%3", shellComm, ocComm, ocComm, "1")+muse, 0o600))
	require.NoError(t, err, "a shell may hold the pane of a CLI that is not one")
	require.Len(t, regs, 4)
	for _, reg := range regs {
		require.Equal(t, shellComm, reg.PaneComm)
	}
	require.Equal(t, codexComm, regs[1].HarnessArgv1)
	require.Equal(t, "muse-bin-1.4.13-R6996.1", regs[3].HarnessArgv0)
}

func TestLoadStatementsRefuses(t *testing.T) {
	t.Parallel()
	f := fixture{name: "bad", socket: testSockets + "/bad"}
	entry := statementEntry(f, "codex-bad", "%3", nodeComm, codexComm, "codex", "1")
	valid := "schema_version: 1\ninstances:\n" + entry
	for name, body := range map[string]string{
		"another schema version":            "schema_version: 2\ninstances:\n" + entry,
		"no schema version":                 "instances:\n" + entry,
		"an unknown field":                  valid + "    group: tmux\n",
		"generation 0":                      strings.Replace(valid, "generation: 1", "generation: 0", 1),
		"no generation":                     strings.Replace(valid, "    generation: 1\n", "", 1),
		"a negative generation":             strings.Replace(valid, "generation: 1", "generation: -1", 1),
		"an unquoted pane id":               strings.Replace(valid, `"%3"`, "%3", 1),
		"a zero-padded pane id":             strings.Replace(valid, `"%3"`, `"%03"`, 1),
		"a relative socket":                 strings.Replace(valid, "socket: /", "socket: ", 1),
		"no harness_argv0":                  strings.Replace(valid, "    harness_argv0: codex\n", "", 1),
		"a path as argv0":                   strings.Replace(valid, "harness_argv0: codex", "harness_argv0: /usr/bin/codex", 1),
		"argv1 for a native CLI":            strings.Replace(valid, "harness_argv0: codex\n", "harness_argv0: codex\n    harness_argv1: cli.js\n", 1),
		"an interpreter without its script": strings.Replace(valid, "harness_argv0: codex", "harness_argv0: node", 1),
		"an option as the script": strings.Replace(valid, "harness_argv0: codex\n",
			"harness_argv0: node\n    harness_argv1: \"--inspect\"\n", 1),
		"the same agent twice": "schema_version: 1\ninstances:\n" + entry +
			statementEntry(f, "codex-bad", "%1", claudeComm, claudeComm, "claude-code", "1"),
		"the same pane twice": "schema_version: 1\ninstances:\n" + entry +
			statementEntry(f, "codex-other", "%3", nodeComm, codexComm, "codex", "1"),
	} {
		_, err := LoadStatements(writeStatements(t, body, 0o600))
		require.ErrorIs(t, err, ErrInvalidArgument, name)
	}
	_, err := LoadStatements(writeStatements(t, strings.Replace(valid, "harness_comm: codex", "harness_comm: bash", 1), 0o600))
	require.ErrorIs(t, err, ErrRefused, "a shell as the CLI")
	_, err = LoadStatements(writeStatements(t, strings.Replace(strings.Replace(valid,
		"pane_comm: node", "pane_comm: zsh", 1), "harness_comm: codex", "harness_comm: zsh", 1), 0o600))
	require.ErrorIs(t, err, ErrRefused, "a shell in the pane and as the CLI")
	_, err = LoadStatements(writeStatements(t, valid, 0o620))
	require.ErrorIs(t, err, ErrInvalidArgument, "group-writable")
	open := privateDir(t)
	require.NoError(t, os.Chmod(open, 0o770))
	shared := filepath.Join(open, statementsName)
	require.NoError(t, os.WriteFile(shared, []byte(valid), 0o600))
	_, err = LoadStatements(shared)
	require.ErrorIs(t, err, ErrInvalidArgument, "a group-writable directory")
	link := filepath.Join(privateDir(t), statementsName)
	require.NoError(t, os.Symlink(writeStatements(t, valid, 0o600), link))
	_, err = LoadStatements(link)
	require.ErrorIs(t, err, ErrInvalidArgument, "a symlink")
	_, err = LoadStatements(writeStatements(t, valid+strings.Repeat("#", maxStatementsBytes), 0o600))
	require.ErrorIs(t, err, ErrInvalidArgument, "larger than the bound")
}

// setPanes replaces what tmux lists on the fixture's socket.
func (f fixture) setPanes(rows ...PaneRow) {
	f.panes.mu.Lock()
	defer f.panes.mu.Unlock()
	f.panes.rows[testUser+" "+f.socket] = rows
}

// refusalCodes are the recorded refusals, by agent.
func (f fixture) refusalCodes(t *testing.T) map[protocol.AgentID]RefusalCode {
	t.Helper()
	rows, err := f.reg.refusals(context.Background())
	require.NoError(t, err)
	out := make(map[protocol.AgentID]RefusalCode, len(rows))
	for agent, row := range rows {
		out[agent] = row.code
	}
	return out
}

func loadFor(t *testing.T, entries ...string) []Registration {
	t.Helper()
	regs, err := LoadStatements(writeStatements(t, "schema_version: 1\ninstances:\n"+strings.Join(entries, ""), 0o600))
	require.NoError(t, err)
	return regs
}

func TestReconcileFollowsTheStatements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "reconcile")
	codex := statementEntry(f, "codex-reconcile", "%3", nodeComm, codexComm, "codex", "1")
	claude := statementEntry(f, "claude-reconcile", "%1", claudeComm, claudeComm, "claude-code", "1")

	got, err := f.reg.Reconcile(ctx, loadFor(t, codex, claude))
	require.NoError(t, err)
	require.Len(t, got.Registered, 2)
	require.Empty(t, got.Revoked)
	require.Empty(t, got.Refused)

	got, err = f.reg.Reconcile(ctx, loadFor(t, codex, claude))
	require.NoError(t, err)
	require.Equal(t, Reconciliation{}, got, "unchanged statements change nothing")

	got, err = f.reg.Reconcile(ctx, loadFor(t, codex))
	require.NoError(t, err)
	require.Len(t, got.Revoked, 1, "a statement removed revokes its instance")
	require.Equal(t, protocol.AgentID("claude-reconcile"), got.Revoked[0].AgentID)
	require.Equal(t, ReasonAdminRevoked, got.Revoked[0].InvalidReason)
	require.Empty(t, got.Registered)

	moved := statementEntry(f, "codex-reconcile", "%7", nodeComm, codexComm, "codex", "1")
	f.setPanes(PaneRow{PaneID: "%1", PanePID: 600}, PaneRow{PaneID: "%7", PanePID: 500})
	got, err = f.reg.Reconcile(ctx, loadFor(t, moved))
	require.NoError(t, err)
	require.Len(t, got.Revoked, 1, "a changed statement replaces its instance")
	require.Len(t, got.Registered, 1)
	require.Equal(t, protocol.TmuxPaneID("%7"), got.Registered[0].PaneID)

	got, err = f.reg.Reconcile(ctx, nil)
	require.NoError(t, err)
	require.Len(t, got.Revoked, 1, "an empty list revokes every instance")
	active, err := f.reg.ListActive(ctx)
	require.NoError(t, err)
	require.Empty(t, active)
}

func TestReconcileNeverReRegistersAnInvalidatedStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "invalid")
	statement := statementEntry(f, "codex-invalid", "%3", nodeComm, codexComm, "codex", "1")
	got, err := f.reg.Reconcile(ctx, loadFor(t, statement))
	require.NoError(t, err)
	require.Len(t, got.Registered, 1)
	inst := got.Registered[0]
	require.ErrorIs(t, f.reg.Verify(ctx, inst, PaneObservation{PaneID: inst.PaneID, PanePID: 999}), ErrInvalidated)

	got, err = f.reg.Reconcile(ctx, loadFor(t, statement))
	require.NoError(t, err)
	require.Equal(t, Reconciliation{}, got, "the same statement stays invalidated")
	require.Empty(t, f.refusalCodes(t), "an invalidated statement is not a refusal")

	bumped := statementEntry(f, "codex-invalid", "%3", nodeComm, codexComm, "codex", "2")
	got, err = f.reg.Reconcile(ctx, loadFor(t, bumped))
	require.NoError(t, err)
	require.Len(t, got.Registered, 1, "a bumped generation registers it again")
	require.NotEqual(t, inst.ID, got.Registered[0].ID)
}

func TestReconcileRecordsEachRefusalOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "retry")
	const agent protocol.AgentID = "codex-retry"
	waiting := statementEntry(f, string(agent), "%8", nodeComm, codexComm, "codex", "1")

	got, err := f.reg.Reconcile(ctx, loadFor(t, waiting))
	require.NoError(t, err)
	require.Equal(t, []Refusal{{AgentID: agent, Code: RefusalPaneMissing}}, got.Refused, "the pane does not exist yet")
	require.Empty(t, f.rows(t), "a refusal leaves no instance")
	require.Equal(t, map[protocol.AgentID]RefusalCode{agent: RefusalPaneMissing}, f.refusalCodes(t))

	got, err = f.reg.Reconcile(ctx, loadFor(t, waiting))
	require.NoError(t, err)
	require.Equal(t, Reconciliation{}, got, "the same refusal is reported once")

	f.sys.edit(func(s *fakeSystem) { s.dirs[testSockets] = dirInfo{uid: 1002, perm: 0o700, dir: true} })
	got, err = f.reg.Reconcile(ctx, loadFor(t, waiting))
	require.NoError(t, err)
	require.Equal(t, []Refusal{{AgentID: agent, Code: RefusalSocketDir}}, got.Refused, "a new code is a new refusal")
	require.Equal(t, map[protocol.AgentID]RefusalCode{agent: RefusalSocketDir}, f.refusalCodes(t))

	f.sys.edit(func(s *fakeSystem) { s.dirs[testSockets] = dirInfo{uid: 1001, perm: 0o700, dir: true} })
	f.setPanes(PaneRow{PaneID: "%1", PanePID: 600}, PaneRow{PaneID: "%8", PanePID: 500})
	got, err = f.reg.Reconcile(ctx, loadFor(t, waiting))
	require.NoError(t, err)
	require.Len(t, got.Registered, 1, "the next pass tries a refused statement again")
	require.Empty(t, got.Refused)
	require.Empty(t, f.refusalCodes(t), "a registered statement keeps no refusal")
}

func TestReconcileForgetsTheRefusalOfARemovedStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "forget")
	codex := statementEntry(f, "codex-forget", "%3", nodeComm, codexComm, "codex", "1")
	missing := statementEntry(f, "claude-forget", "%9", claudeComm, claudeComm, "claude-code", "1")

	got, err := f.reg.Reconcile(ctx, loadFor(t, codex, missing))
	require.NoError(t, err)
	require.Len(t, got.Registered, 1, "one refusal does not stop the other statements")
	require.Equal(t, []Refusal{{AgentID: "claude-forget", Code: RefusalPaneMissing}}, got.Refused)

	got, err = f.reg.Reconcile(ctx, loadFor(t, codex))
	require.NoError(t, err)
	require.Equal(t, Reconciliation{}, got)
	require.Empty(t, f.refusalCodes(t))
}

func TestReconcileRefusesAnAgentStatedTwice(t *testing.T) {
	t.Parallel()
	f := newFixture(t, openDB(t), "twice")
	regs := loadFor(t, statementEntry(f, "codex-twice", "%3", nodeComm, codexComm, "codex", "1"))
	_, err := f.reg.Reconcile(context.Background(), append(regs, regs[0]))
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestTheRefusalTableTakesOnlyTheCodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFixture(t, openDB(t), "codes")
	for i, code := range []RefusalCode{
		RefusalInvalid, RefusalShell, RefusalExcluded, RefusalUnknownUser, RefusalRootUser,
		RefusalReservedUID, RefusalSocketDir, RefusalTmuxUnreachable, RefusalPaneMissing,
		RefusalPaneComm, RefusalHarnessMissing, RefusalHarnessProgram, RefusalAgentUID, RefusalAlreadyActive,
		RefusalInternal,
	} {
		reg := f.codex()
		reg.AgentID = protocol.AgentID(fmt.Sprintf("codes-%d", i))
		require.NoError(t, f.reg.recordRefusal(ctx, reg, code), code)
	}
	require.Error(t, f.reg.recordRefusal(ctx, f.codex(), "pane %3 on /tmp/tmux-1001/codes"), "a code with content")
}
