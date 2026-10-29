package policy

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

func TestSaveIsIdempotentAndKeepsCanonicalContent(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	compiled, err := compile([]Rule{{
		ID: "allow", Effect: EffectAllow, Peer: "node-a", Action: ActionAck, Agent: "agent-*",
	}})
	require.NoError(t, err)
	createdAt := time.Date(2026, time.October, 29, 1, 2, 3, 4, time.UTC)
	require.NoError(t, Save(t.Context(), store.DB(), compiled, createdAt))
	require.NoError(t, Save(t.Context(), store.DB(), compiled, createdAt.Add(time.Hour)))

	var count int64
	var digest, canonical, stamp string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*), digest, compiled_json, created_at FROM policy_versions WHERE version = ?",
		compiled.Version).Scan(&count, &digest, &canonical, &stamp))
	require.Equal(t, int64(1), count)
	require.Equal(t, compiled.Digest, digest)
	require.Equal(t, compiled.CompiledJSON, canonical)
	require.Equal(t, eventstore.FormatTime(createdAt), stamp)
}

func TestSaveRejectsShortVersionCollision(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	compiled, err := compile(nil)
	require.NoError(t, err)
	createdAt := time.Date(2026, time.October, 29, 1, 2, 3, 4, time.UTC)
	require.NoError(t, Save(t.Context(), store.DB(), compiled, createdAt))
	collision := *compiled
	collision.Digest = strings.Repeat("f", 64)
	err = Save(t.Context(), store.DB(), &collision, createdAt)
	require.ErrorIs(t, err, ErrInvalidPolicy)
}
