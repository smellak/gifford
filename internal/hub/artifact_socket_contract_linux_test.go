//go:build linux

package hub_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

type artifactSocketSendResult struct {
	ID         protocol.OpaqueID      `json:"artifact_id"`
	State      protocol.ArtifactState `json:"state"`
	ChunkCount int64                  `json:"chunk_count"`
	ChunksSent int64                  `json:"chunks_sent"`
}

// Both agents execute all four Cobra commands over the same real shared
// socket. No artifact operation is replaced by a direct Service call.
func TestArtifactSharedSocketRoundTrip(t *testing.T) {
	f := newArtifactSocketFixture(t)
	a, b := f.agent(t, artifactAgentA), f.agent(t, artifactAgentB)
	initial := artifactJSON[protocol.ListPendingArtifactsResult](t, b.artifact(t, f.socket, "list"))
	require.Empty(t, initial.Artifacts)
	// Cross a chunk boundary so receipt also verifies chunk assembly.
	plaintext := bytes.Repeat([]byte("artifact-socket-roundtrip\x00"), 50_000)
	path := filepath.Join(t.TempDir(), "payload.bin")
	require.NoError(t, os.WriteFile(path, plaintext, 0o600))
	sent := artifactJSON[artifactSocketSendResult](t,
		a.artifact(t, f.socket, "send", path, "--to", string(artifactAgentB), "--purpose", "roundtrip"))
	require.True(t, sent.ID.Valid())
	require.Equal(t, protocol.ArtifactStateLocalAvailable, sent.State)
	require.Greater(t, sent.ChunkCount, int64(1))
	listed := artifactJSON[protocol.ListPendingArtifactsResult](t, b.artifact(t, f.socket, "list"))
	require.Len(t, listed.Artifacts, 1)
	require.Equal(t, sent.ID, listed.Artifacts[0].ArtifactID)
	require.Equal(t, artifactAgentA, listed.Artifacts[0].Sender)
	outDir := t.TempDir()
	received := artifactJSON[struct {
		Path   string `json:"path"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	}](t, b.artifact(t, f.socket, "receive", string(sent.ID), "--out", outDir))
	require.Equal(t, filepath.Join(outDir, "payload.bin"), received.Path)
	actual, err := os.ReadFile(received.Path)
	require.NoError(t, err)
	require.Equal(t, plaintext, actual)
	digest := sha256.Sum256(plaintext)
	require.Equal(t, hex.EncodeToString(digest[:]), received.SHA256)
	require.EqualValues(t, len(plaintext), received.Size)
	for _, agent := range []*artifactSocketChild{a, b} {
		status := artifactJSON[protocol.ArtifactStatusResult](t,
			agent.artifact(t, f.socket, "status", string(sent.ID)))
		require.NoError(t, status.Validate())
		require.Equal(t, sent.ID, status.ArtifactID)
		require.Equal(t, protocol.ArtifactStateAcknowledged, status.State)
		require.Equal(t, protocol.ArtifactStateAcknowledged, status.Recipients[0].State)
		require.NotNil(t, status.Recipients[0].AcknowledgedAt)
	}
}

func TestArtifactSharedSocketNoIdentityNoKeys(t *testing.T) {
	f := newArtifactSocketFixture(t)
	anonymous := startArtifactSocketChild(t)
	identity := anonymous.request(t, artifactChildRequest{Action: artifactWhoAmI, Socket: f.socket})
	require.NotEmpty(t, identity.Error, "the real socket must reject this process before exercising Cobra")
	require.Empty(t, identity.Output)
	response := anonymous.artifact(t, f.socket, "list")
	requireArtifactNoKeys(t, anonymous, f)
	require.NotEmpty(t, response.Error)
	// An unimplemented flag is not proof that the CLI enforced authentication.
	require.NotContains(t, response.Error, "unknown flag")
	require.Contains(t, response.Error, "artifact identity")
}

func TestArtifactSharedSocketKeepsMCP(t *testing.T) {
	f := newArtifactSocketFixture(t)
	a, b := f.agent(t, artifactAgentA), f.agent(t, artifactAgentB)
	assertArtifactMCPResponse(t, a.request(t, artifactChildRequest{Action: artifactMCPStart, Socket: f.socket}))
	before, ok := f.server.RegisteredEndpoint(artifactAgentA, "session-"+protocol.OpaqueID(artifactAgentA))
	require.True(t, ok, "a real live MCP lease is the precondition")
	require.Equal(t, a.process.PID, before.Process.PID)
	_ = artifactJSON[protocol.ListPendingArtifactsResult](t, b.artifact(t, f.socket, "list"))
	path := filepath.Join(t.TempDir(), "mcp.txt")
	require.NoError(t, os.WriteFile(path, []byte("MCP must remain connected"), 0o600))
	sent := artifactJSON[artifactSocketSendResult](t,
		a.artifact(t, f.socket, "send", path, "--to", string(artifactAgentB), "--purpose", "preserve MCP"))
	require.Equal(t, protocol.ArtifactStateLocalAvailable, sent.State)
	// The CLI link has closed. This is the original MCP server/connection:
	// the helper refuses a second start and does not redial.
	assertArtifactMCPResponse(t, a.request(t, artifactChildRequest{Action: artifactMCPList}))
	after, ok := f.server.RegisteredEndpoint(artifactAgentA, before.SessionID)
	require.True(t, ok)
	require.Equal(t, before, after)
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM sessions WHERE session_id = ?", before.SessionID).Scan(&state))
	require.Equal(t, string(protocol.SessionStateConnected), state)
}

func TestArtifactSharedSocketSendIsIdempotent(t *testing.T) {
	f := newArtifactSocketFixture(t)
	a, b := f.agent(t, artifactAgentA), f.agent(t, artifactAgentB)
	_ = artifactJSON[protocol.ListPendingArtifactsResult](t, b.artifact(t, f.socket, "list"))
	path := filepath.Join(t.TempDir(), "repeat.txt")
	require.NoError(t, os.WriteFile(path, []byte("one file, two sends"), 0o600))
	args := []string{"send", path, "--to", string(artifactAgentB), "--purpose", "idempotency"}
	first := artifactJSON[artifactSocketSendResult](t, a.artifact(t, f.socket, args...))
	second := artifactJSON[artifactSocketSendResult](t, a.artifact(t, f.socket, args...))
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, protocol.ArtifactStateLocalAvailable, second.State)
	require.Zero(t, second.ChunksSent, "a retry must not upload a second copy")
	listed := artifactJSON[protocol.ListPendingArtifactsResult](t, b.artifact(t, f.socket, "list"))
	require.Len(t, listed.Artifacts, 1)
	require.Equal(t, first.ID, listed.Artifacts[0].ArtifactID)
	var transfers, recipients int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM artifact_transfers").Scan(&transfers))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM artifact_recipients").Scan(&recipients))
	require.Equal(t, 1, transfers)
	require.Equal(t, 1, recipients)
}

func assertArtifactMCPResponse(t *testing.T, response artifactChildResponse) {
	t.Helper()
	value := artifactJSON[map[string]json.RawMessage](t, response)
	require.NotContains(t, value, "error")
	var result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	require.NoError(t, json.Unmarshal(value["result"], &result))
	require.Len(t, result.Content, 1)
	require.Equal(t, "text", result.Content[0].Type)
	require.True(t, json.Valid([]byte(result.Content[0].Text)))
}

// These controls do not use the missing flag. They prove the fixture has
// kernel-backed identity and a bound MCP, not that artifact --socket works.
func TestArtifactSharedSocketIdentityControl(t *testing.T) {
	f := newArtifactSocketFixture(t)
	a, b := f.agent(t, artifactAgentA), f.agent(t, artifactAgentB)
	require.NotEqual(t, a.process.PID, b.process.PID)
	for _, pair := range []struct {
		child *artifactSocketChild
		agent protocol.AgentID
	}{{a, artifactAgentA}, {b, artifactAgentB}} {
		response := pair.child.request(t, artifactChildRequest{Action: artifactWhoAmI, Socket: f.socket})
		require.Empty(t, response.Error)
		require.Equal(t, string(pair.agent), response.Output)
	}
}

func TestArtifactSharedSocketUnboundControl(t *testing.T) {
	f := newArtifactSocketFixture(t)
	// A sibling's live session must not authorize this anonymous process.
	_ = f.agent(t, artifactAgentB)
	anonymous := startArtifactSocketChild(t)
	response := anonymous.request(t, artifactChildRequest{Action: artifactWhoAmI, Socket: f.socket})
	require.NotEmpty(t, response.Error)
	require.Empty(t, response.Output)
	requireArtifactNoKeys(t, anonymous, f)
}

func TestArtifactSharedSocketMCPControl(t *testing.T) {
	f := newArtifactSocketFixture(t)
	a := f.agent(t, artifactAgentA)
	assertArtifactMCPResponse(t, a.request(t, artifactChildRequest{Action: artifactMCPStart, Socket: f.socket}))
	identity, ok := f.server.RegisteredEndpoint(artifactAgentA, "session-"+protocol.OpaqueID(artifactAgentA))
	require.True(t, ok)
	require.Equal(t, hub.EndpointProcess{
		PID: a.process.PID, StartTime: a.process.ProcessStartTime,
	}, identity.Process)
	assertArtifactMCPResponse(t, a.request(t, artifactChildRequest{Action: artifactMCPList}))
}
