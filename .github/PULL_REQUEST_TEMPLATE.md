<!-- One PR per plan subphase. Keep it self-contained and reviewer-facing. -->

Subphase: <!-- e.g. 2.4 — Single writer and two-phase commit -->
Package(s): <!-- e.g. internal/proxy -->

### What and why
<!-- The change and the reason, for a reviewer who has not read the package. Link PRD sections (§9.1.2). -->

### How I tested this (observed, not expected)
<!-- Only what you actually ran and saw. Commands + observed output or screenshots.
     Do not paste "go test ./... passes": CI shows that. If untested, say "Not tested" and why. -->

### Contracts and invariants
- [ ] No change to internal/protocol or schemas/ — or this PR is `!` and bumps schema_version
- [ ] No writes to any PTY outside the single writer; no `send-keys`; `pipe-pane` only with `-O`
- [ ] No delivery path without a `channel.Decision{Allow}` parameter; no writes to the control tables outside internal/channel; no MCP tool that changes a control (R46–R48)
- [ ] Nothing from .gitignore (captures, recordings, datasets, keys, .db) is included

### Definition of Done
- [ ] Tests for the new behaviour; `task lint && task test:race` green locally
- [ ] Golden files regenerated on purpose (explain which and why)
- [ ] Docs/ADR updated if a pending decision was resolved

### Authorship
Who wrote this:
- [ ] A human wrote it.
- [ ] An agent wrote it under close human direction.
- [ ] An agent wrote it independently.

Agent and model (if any): <!-- e.g. Muse Code / Muse Spark 1.2, Claude Code / Opus 5.5 -->
