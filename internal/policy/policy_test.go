package policy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func writePolicyFile(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
}

func TestEvaluateDenyPrecedesAllow(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "policies")
	writePolicyFile(t, dir, "rules.yaml", `schema_version: 1
rules:
  - id: allow-team
    effect: allow
    peer: "*"
    action: deliver_message
    agent: "team-*"
  - id: deny-one
    effect: deny
    peer: node-a
    action: deliver_message
    agent: team-secret
`)
	compiled, err := Load(dir)
	require.NoError(t, err)

	denied := Evaluate(compiled, "node-a", "org-a", ActionDeliverMessage, "team-secret")
	require.Equal(t, DecisionDeny, denied.Decision)
	require.Equal(t, "deny-one", denied.RuleID)
	require.Equal(t, compiled.Version, denied.Version)

	allowed := Evaluate(compiled, "node-b", "org-b", ActionDeliverMessage, "team-public")
	require.Equal(t, DecisionAllow, allowed.Decision)
	require.Equal(t, "allow-team", allowed.RuleID)

	noRule := Evaluate(compiled, "node-b", "org-b", ActionAck, "team-public")
	require.Equal(t, Evaluation{Decision: DecisionDeny, Version: compiled.Version}, noRule)
}

func TestEvaluatePeerScopesAndAgentPatterns(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "policies")
	writePolicyFile(t, dir, "scopes.yaml", `schema_version: 1
rules:
  - id: by-node
    effect: allow
    peer: node-a
    action: ack
    agent: node-agent
  - id: by-org
    effect: allow
    peer: org:org-a
    action: fetch_keys
    agent: "org-?"
  - id: wildcard
    effect: allow
    peer: "*"
    action: list_agents
    agent: "public-*"
`)
	compiled, err := Load(dir)
	require.NoError(t, err)

	require.Equal(t, DecisionAllow,
		Evaluate(compiled, "node-a", "other", ActionAck, "node-agent").Decision)
	require.Equal(t, DecisionAllow,
		Evaluate(compiled, "node-b", "org-a", ActionFetchKeys, "org-x").Decision)
	require.Equal(t, DecisionDeny,
		Evaluate(compiled, "node-b", "org-b", ActionFetchKeys, "org-x").Decision)
	require.Equal(t, DecisionAllow,
		Evaluate(compiled, "node-c", "org-c", ActionListAgents, "public-one").Decision)
	require.Equal(t, DecisionDeny,
		Evaluate(compiled, "node-c", "org-c", ActionListAgents, "private-one").Decision)
}

func TestEmptyPolicyDeniesEverything(t *testing.T) {
	t.Parallel()

	compiled, err := Load(filepath.Join(t.TempDir(), "missing"))
	require.NoError(t, err)
	require.Empty(t, compiled.Rules())
	require.Equal(t, "[]", compiled.CompiledJSON)
	require.True(t, strings.HasPrefix(compiled.Version, versionPrefix))
	require.Len(t, compiled.Version, len(versionPrefix)+12)
	decision := Evaluate(compiled, "node-a", "org-a", ActionReadPolicies, "agent-a")
	require.Equal(t, DecisionDeny, decision.Decision)
	require.Empty(t, decision.RuleID)
}

func TestCanonicalVersionIgnoresFileOrderAndWhitespace(t *testing.T) {
	t.Parallel()

	firstDir := filepath.Join(t.TempDir(), "first")
	writePolicyFile(t, firstDir, "a.yaml", `schema_version: 1
rules:
  - id: a-rule
    effect: allow
    peer: node-a
    action: ack
    agent: agent-a
`)
	writePolicyFile(t, firstDir, "b.yaml", `schema_version: 1
rules:
  - id: b-rule
    effect: deny
    peer: "*"
    action: revoke
    agent: "agent-*"
`)

	secondDir := filepath.Join(t.TempDir(), "second")
	writePolicyFile(t, secondDir, "a.yaml", `schema_version: 1
rules: [{id: b-rule, effect: deny, peer: "*", action: revoke, agent: "agent-*"}]
`)
	writePolicyFile(t, secondDir, "z.yaml", `schema_version: 1
rules:

  - agent: agent-a
    action: ack
    peer: node-a
    effect: allow
    id: a-rule
`)
	first, err := Load(firstDir)
	require.NoError(t, err)
	second, err := Load(secondDir)
	require.NoError(t, err)
	require.Equal(t, first.Version, second.Version)
	require.Equal(t, first.Digest, second.Digest)
	require.Equal(t, first.CompiledJSON, second.CompiledJSON)
	const canonical = `[{"id":"a-rule","effect":"allow","peer":"node-a","action":"ack","agent":"agent-a"},{"id":"b-rule","effect":"deny","peer":"*","action":"revoke","agent":"agent-*"}]`
	if first.CompiledJSON != canonical {
		t.Fatalf("canonical JSON = %q, want %q", first.CompiledJSON, canonical)
	}

	writePolicyFile(t, secondDir, "a.yaml", `schema_version: 1
rules: [{id: b-rule, effect: allow, peer: "*", action: revoke, agent: "agent-*"}]
`)
	changed, err := Load(secondDir)
	require.NoError(t, err)
	require.NotEqual(t, first.Version, changed.Version)
}

func TestLoadRejectsWholeInvalidPolicy(t *testing.T) {
	t.Parallel()
	const badFilename = "bad.yaml"

	for name, files := range map[string]map[string]string{
		"missing schema version": {
			badFilename: "rules: []\n",
		},
		"wrong schema version": {
			badFilename: "schema_version: 2\nrules: []\n",
		},
		"string schema version": {
			badFilename: "schema_version: '1'\nrules: []\n",
		},
		"missing rules": {
			badFilename: "schema_version: 1\n",
		},
		"unknown top-level key": {
			badFilename: "schema_version: 1\nrules: []\nsurprise: true\n",
		},
		"unknown key": {
			badFilename: "schema_version: 1\nrules:\n  - id: bad\n    effect: allow\n    peer: '*'\n    action: ack\n    agent: agent-a\n    extra: true\n",
		},
		"unknown action": {
			badFilename: "schema_version: 1\nrules:\n  - id: bad\n    effect: allow\n    peer: '*'\n    action: execute\n    agent: agent-a\n",
		},
		"invalid pattern": {
			badFilename: "schema_version: 1\nrules:\n  - id: bad\n    effect: allow\n    peer: '*'\n    action: ack\n    agent: '[abc'\n",
		},
		"invalid peer": {
			badFilename: "schema_version: 1\nrules:\n  - id: bad\n    effect: allow\n    peer: 'org:'\n    action: ack\n    agent: agent-a\n",
		},
		"duplicate id": {
			"a.yaml": "schema_version: 1\nrules:\n  - id: duplicate\n    effect: allow\n    peer: node-a\n    action: ack\n    agent: agent-a\n",
			"b.yaml": "schema_version: 1\nrules:\n  - id: duplicate\n    effect: deny\n    peer: node-b\n    action: ack\n    agent: agent-b\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "policies")
			for filename, body := range files {
				writePolicyFile(t, dir, filename, body)
			}
			compiled, err := Load(dir)
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.Nil(t, compiled)
		})
	}
}
