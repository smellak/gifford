package policy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
)

// Store is the transaction surface used to persist a compiled policy.
type Store interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Save inserts a policy version once and verifies that an existing short
// version maps to the same full digest and canonical JSON.
func Save(ctx context.Context, store Store, policy *Policy, createdAt time.Time) error {
	if store == nil || !validCompiledPolicy(policy) {
		return fmt.Errorf("save policy: %w", ErrInvalidPolicy)
	}
	_, err := store.ExecContext(ctx, `INSERT INTO policy_versions
		(version, digest, compiled_json, created_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(version) DO NOTHING`, policy.Version, policy.Digest,
		policy.CompiledJSON, eventstore.FormatTime(createdAt))
	if err != nil {
		return fmt.Errorf("insert policy version %s: %w", policy.Version, err)
	}
	var digest, compiledJSON string
	if err := store.QueryRowContext(ctx,
		"SELECT digest, compiled_json FROM policy_versions WHERE version = ?", policy.Version).
		Scan(&digest, &compiledJSON); err != nil {
		return fmt.Errorf("read policy version %s: %w", policy.Version, err)
	}
	if digest != policy.Digest || compiledJSON != policy.CompiledJSON {
		return fmt.Errorf("policy version %s maps to different content: %w", policy.Version, ErrInvalidPolicy)
	}
	return nil
}

func validCompiledPolicy(policy *Policy) bool {
	if policy == nil {
		return false
	}
	digest := sha256.Sum256([]byte(policy.CompiledJSON))
	digestHex := hex.EncodeToString(digest[:])
	return policy.Digest == digestHex && policy.Version == versionPrefix+digestHex[:12]
}
