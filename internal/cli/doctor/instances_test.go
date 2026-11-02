package doctor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

const stamp = "2026-11-02T21:00:00.000000000Z"

func statement(agent, pane, generation string) string {
	return "  - agent_id: " + agent + "\n    agent_user: dev\n    owner_user: dev\n" +
		"    socket: /tmp/tmux-1001/default\n    pane_id: \"" + pane + "\"\n" +
		"    pane_comm: node\n    harness_comm: codex\n    harness: codex\n" +
		"    harness_version: \"1.0\"\n    generation: " + generation + "\n"
}

// doctorHome points the XDG variables at fresh directories and writes the
// instances file, when body is not empty.
func doctorHome(t *testing.T, body string) (configDir, dataDir string) {
	t.Helper()
	cfgHome, dataHome := t.TempDir(), t.TempDir()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", cfgHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	configDir, dataDir = filepath.Join(cfgHome, "gifford"), filepath.Join(dataHome, "gifford")
	require.NoError(t, os.Mkdir(configDir, 0o700))
	require.NoError(t, os.Mkdir(dataDir, 0o700))
	if body != "" {
		require.NoError(t, os.WriteFile(filepath.Join(configDir, "instances.yaml"), []byte(body), 0o600))
	}
	return configDir, dataDir
}

func runDoctor(t *testing.T) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	return out.String(), err
}

func TestDoctorReportsEachStatement(t *testing.T) {
	ctx := context.Background()
	configDir, dataDir := doctorHome(t, "schema_version: 1\ninstances:\n"+
		statement("codex-active", "%1", "1")+statement("codex-invalid", "%2", "1")+
		statement("codex-refused", "%3", "1")+statement("codex-pending", "%4", "1"))
	regs, err := tmuxinstance.LoadStatements(filepath.Join(configDir, "instances.yaml"))
	require.NoError(t, err)

	dbPath := filepath.Join(dataDir, databaseName)
	store, err := eventstore.Open(ctx, dbPath)
	require.NoError(t, err)
	insert := `INSERT INTO tmux_instances (instance_id, agent_id, machine_id, boot_id, owner_user,
		owner_uid, socket, pane_id, pane_pid, pane_start, pane_comm, harness, harness_version,
		harness_pid, harness_start, harness_comm, agent_uid, agent_gid, source, registered_by,
		state, invalid_reason, created_at, updated_at)
		VALUES (?, ?, 'm', 'b', 'dev', 1001, '/tmp/tmux-1001/default', ?, 500, 100, 'node', 'codex',
		'1.0', 501, 120, 'codex', 1001, 1001, ?, 0, ?, ?, ?, ?)`
	_, err = store.DB().ExecContext(ctx, insert, "01JC0000000000000000000001", regs[0].AgentID, regs[0].PaneID,
		regs[0].Source, "ACTIVE", nil, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(ctx, insert, "01JC0000000000000000000002", regs[1].AgentID, regs[1].PaneID,
		regs[1].Source, "INVALID", "harness_changed", stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(ctx, `INSERT INTO tmux_instance_refusals (agent_id, source, code, refused_at)
		VALUES (?, ?, 'pane_missing', ?)`, regs[2].AgentID, regs[2].Source, stamp)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	report, err := runDoctor(t)
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec, report)
	require.Equal(t, exitcode.Error, ec.Code)
	for _, line := range []string{
		"INSTANCES   OK   4 statements (" + filepath.Join(configDir, "instances.yaml") + ")\n",
		"INSTANCE    OK   codex-active on pane %1 since " + stamp + "\n",
		"INSTANCE    FAIL codex-invalid invalidated: harness_changed since " + stamp +
			"; raise its generation to register it again\n",
		"INSTANCE    FAIL codex-refused refused: pane_missing since " + stamp + "\n",
		"INSTANCE    WAIT codex-pending: no pass has reconciled it yet\n",
	} {
		require.Contains(t, report, line)
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "doctor only reads the database")
}

func TestDoctorInstancesWithoutAFileOrADatabase(t *testing.T) {
	configDir, _ := doctorHome(t, "")
	report, err := runDoctor(t)
	require.NoError(t, err, report)
	require.Contains(t, report, "INSTANCES   SKIP no "+filepath.Join(configDir, "instances.yaml")+"\n")

	doctorHome(t, "schema_version: 1\ninstances:\n"+statement("codex-early", "%1", "1"))
	report, err = runDoctor(t)
	require.NoError(t, err, report)
	require.Contains(t, report, "INSTANCES   WAIT no node database yet: no statement is reconciled\n")
}

func TestDoctorFailsOnAnInvalidInstancesFile(t *testing.T) {
	doctorHome(t, "schema_version: 1\ninstances:\n"+statement("codex-zero", "%1", "0"))
	report, err := runDoctor(t)
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec, report)
	require.Contains(t, report, "INSTANCES   FAIL ")
	require.Contains(t, report, "generation must be at least 1")
}

func TestOpenReadDBRefusesWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), databaseName)
	store, err := eventstore.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	db, err := openReadDB(ctx, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	_, err = db.ExecContext(ctx, `DELETE FROM tmux_instance_refusals`)
	require.Error(t, err)
	_, err = openReadDB(ctx, filepath.Join(t.TempDir(), databaseName))
	require.ErrorIs(t, err, os.ErrNotExist)
}
