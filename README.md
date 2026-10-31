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

From scratch, with Go 1.27 or later:

```sh
go install github.com/smellak/gifford/cmd/gifford@latest
gifford init              # write config.yaml and create the state directories
gifford doctor            # check them
gifford up                # start the node in the background
gifford status            # is the node up? its agents and the channel state
gifford run --agent claude-1 --harness claude-code -- claude
                          # in another terminal: run a harness behind the PTY proxy
gifford down              # stop the node
```

- **`init`** writes `config.yaml` with `schema_version: 1` and a node id. Pass `--node-id <id>` to choose the id; otherwise one is generated. It never overwrites an existing `config.yaml`.
- **`up`** runs the same process as `gifford hub`, detached from the terminal. Its log is `hub.log` in the data directory. `gifford up --dev` runs the node in the foreground instead.
- **`run`**: `--harness` is one of `claude-code`, `codex`, `muse-code`, `opencode`, `antigravity` or `generic`.

## Basic flow

```sh
gifford panel             # watch agents, messages and decisions
gifford ctl --help        # control the node from scripts
```

## Roadmap

These commands are not in v0.1-hackathon. They stay out of `--help`, and calling one prints "not implemented in v0.1-hackathon":
- `gifford relay` and `gifford install-systemd`;
- `gifford capture attach`, `export`, `ingest`, `reconcile`, `status`, `stop` and `supervise`;
- governed loops: `gifford ctl loop` and its subcommands;
- the rest of manual control:
  - `gifford ctl deliver-next`, `hold-message`, `release-hold`, `approve-message`, `reject-message` and `unlock`;
  - `gifford ctl inspect`, `replay`, `retry-artifact` and `revoke-node`;
- `gifford ctl model activate` and `rollback`;
- `gifford node keys`;
- `gifford dataset redact` and `gifford label transcript`.

## Send a file

Run artifact commands inside the sending or receiving agent's process tree.
Transfers are end-to-end encrypted and repeating the same `send` resumes at
the first chunk the hub has not confirmed.

```sh
gifford artifact send ./report.pdf --to reviewer --purpose "review"
gifford artifact list
gifford artifact receive art-0123456789abcdef0123456789abcdef --out ./downloads
gifford artifact status art-0123456789abcdef0123456789abcdef
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
