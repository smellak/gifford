# Gifford

Gifford lets interactive coding agents (Muse Code, Claude Code, Codex, OpenCode,
Antigravity) message each other without ever typing over the human at the
keyboard. A PTY proxy owns the keyboard, Muse Glimmer reads the screens, and
deterministic guards decide what gets delivered and when.

> **Status:** active hackathon sprint. Follow `main` and the open pull requests
> for the live implementation.

## AI contributors

Gifford is being built openly by coding agents running on three independent
harnesses. Each harness has a project-owned GitHub App identity in the commit
history:

| AI coding harness | Provider | GitHub contributor identity |
|---|---|---|
| Muse Code | Meta | [`gifford-muse-agent[bot]`](https://github.com/apps/gifford-muse-agent) |
| Codex | OpenAI | [`gifford-codex-agent[bot]`](https://github.com/apps/gifford-codex-agent) |
| Claude Code | Anthropic | [`gifford-claude-agent[bot]`](https://github.com/apps/gifford-claude-agent) |

These are identities owned by this project, not official vendor accounts. See
[`AUTHORS.md`](AUTHORS.md) for the attribution rules and the preserved mapping
of commits made before the App identities were introduced.

## Built during the hackathon (Oct 27 – Nov 4, 2026)

Every line in this repository was written during the event window. The commit
history is the evidence and is never rewritten (no squash, no rebase-merge).
Most of the code is written by AI coding agents on three harnesses (Muse Code,
Codex and Claude Code) under the direction of a human integrator. Each commit
names the harness that wrote it (see `AUTHORS.md`), and each pull request
declares its authorship (see `CONTRIBUTING.md`).

## Install

```sh
go install github.com/smellak/gifford/cmd/gifford@latest
```

## Basic flow

```sh
gifford doctor            # check the environment and create the state directories
gifford daemon            # start the local daemon
gifford run -- claude     # run a harness behind the PTY proxy
gifford panel             # watch agents, messages and decisions
gifford ctl --help        # control the daemon from scripts
```

## Model

The analyst runs on **Muse Glimmer** (Meta, open weights), served with vLLM on
Modal. Evidence of the model in use goes to `docs/RESULTS.md`.

## Third-party

See `NOTICE`. Dependencies are declared in `go.mod`.

## Documentation

- `AGENTS.md`: guide for implementing agents
- `CONTRIBUTING.md`: branches, commits, pull requests, authorship
- `docs/`: layout, testing, commit messages, decision records

## License

Apache-2.0. See `LICENSE`.
