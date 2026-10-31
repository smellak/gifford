# Freeze

What is frozen in this repository, and what actually enforces it. Every
statement can be checked with the command or file it names.

## Contract tags

`git tag -l` lists six annotated tags, all on the history of `main`:

| Tag | Commit | Files in `schemas/v1` | Its message says it freezes |
|---|---|---:|---|
| `contracts-v1` | `66bc2d3` | 39 | the base protocol contracts, SchemaVersion 1 and WireVersion 1 |
| `contracts-v1.1` | `5cafed4` | 83 | additive channel-control and Control API contracts |
| `contracts-v1.2` | `f2dc20c` | 96 | additive notification read contracts |
| `contracts-v1.3` | `90778fa` | 98 | the additive state observation report and v1.3 agent frame |
| `contracts-v1.4` | `b109140` | 101 | the additive agent-socket v1.4 support announcement and durable event-result contracts |
| `contracts-v1.4.1` | `1876f4b` | 101 | additive durable Proxy event validation and capture marker contracts |

There is no `ml-contract-v1` and no `feature-freeze` tag, although
`docs/repo-rules.md` names both as milestone tags. Nothing freezes the model
contract yet ([ML_CONTRACT.md](ML_CONTRACT.md)).

## `schemas/v1`

`main` has 103 files in `schemas/v1`, all JSON Schema, and nothing else under
`schemas/`. They are generated from the Go types listed in
`internal/protocol/roots.go`: `TestGenerateSchemas` fails when a file differs
from what the types generate, and CI runs `task generate:check`, which
regenerates and fails on any diff. `schema_version` is 1 throughout, and an
unknown version fails with `ErrUnsupportedVersion`.

`git diff --name-status <tag> main -- schemas/v1` gives, for each tag:

- `contracts-v1`, `v1.2`, `v1.3`, `v1.4` and `v1.4.1`: only added files. No
  file of those trees has changed or gone.
- `contracts-v1.1`: one changed file, `rpc_request.schema.json`. Before
  `contracts-v1.2`, commits `0026b32` and `fe26af1` added nine names to its
  method enum: `node.enroll_token`, `node.join`, `node.peers`, `node.revoke`,
  `agent.list`, `message.list`, `decision.list`, `decision.get` and
  `model.status`. It has not changed since `contracts-v1.2`. The comments in
  `internal/protocol/doc.go` and `rpc.go` still call it frozen at the v1.1
  method set, and the `contracts-v1.2` tag message says earlier schema bytes
  are unchanged; measured against the `contracts-v1.1` tree, neither is true.
- Two files on `main` are in no tag: `delivery_list_result.schema.json` and
  `rpc_request_v1_4.schema.json`, added by `1a40a54` after `contracts-v1.4.1`.
  `internal/protocol/doc.go` says `contracts-v1.4` adds them; that tag's tree
  does not contain them.

The milestone labels in `roots.go` do not always match the tag trees either:
nine roots labelled `contracts-v1.1` (the params and results of `agent.list`,
`message.list`, `decision.list` and `decision.get`, and `model_status_result`)
first appear in the `contracts-v1.2` tree, and the two untagged files above are
labelled `contracts-v1.4`.

## Tests that guard the schemas

All of these run in `go test ./...`, which the `verify` job runs.

- `TestPreV12SchemasRemainFrozen`, `TestPreV13SchemasRemainFrozen` and
  `TestPreV14SchemasRemainFrozen` (`internal/protocol/roots_test.go`) hash the
  name and bytes of every root up to a milestone label and compare the result
  with a fixed SHA-256. They pin the bytes on `main` as `roots.go` labels them,
  so the digest for the v1 and v1.1 roots includes the widened
  `rpc_request.schema.json`. No digest test covers the roots labelled
  `contracts-v1.4`, and no test compares a file with a tag's tree.
- `internal/protocol/fixtures_test.go` checks four boundaries: `rpc_request`
  rejects `notification.list` and `rpc_request_v1_2` rejects `delivery.list`,
  and the v1 and v1.3 agent frames reject the operations added in v1.3 and
  v1.4; the next schema accepts each. `TestFixtures` requires a fixture
  directory for every root, validates the fixtures against their schemas,
  round-trips the valid ones through the Go types and rejects the invalid
  ones.
- These tests pin bytes, not meaning. Since commit `60b1beb` the node marks a
  delivery a person approved by setting the `idempotency_key` of the
  `contracts-v1` prepare frame to `approved:<delivery_id>`, a value the frozen
  pattern already allows, and the agent socket lets only that order through a
  pause (`internal/protocol/agentsocket.go`, `internal/agentsock/delivery.go`).
  No schema file changed, and the tests above still pass.

## Frozen package and owners

- `internal/protocol` is the frozen package. Its `doc.go` sets the rule: after
  the freeze any change is breaking, is committed with `!`, bumps the schema
  version, is generated into `schemas/vN/`, updates the fixtures and notifies
  every package owner.
- `.github/CODEOWNERS` assigns every path to one maintainer account, frozen or
  not. Its comments mark `/internal/protocol/` as "frozen at contracts-v1; any
  change needs all owners", and `/schemas/` and `/migrations/` as contract
  changes that notify all owners. Neither the file nor the comment blocks a
  change by itself.
- What blocks a merge is branch protection. On 2026-10-31, `main` takes
  changes only through a pull request with three passing status checks,
  administrators included: `verify`, `cross-compile (darwin)` and
  `cross-compile (windows)`. It requires no approving review and no
  code-owner review, does not require a branch to be up to date before it
  merges, and refuses force pushes and deletion.
