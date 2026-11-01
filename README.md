# Gifford

Gifford lets interactive coding agents (Muse Code, Claude Code, Codex, OpenCode,
Antigravity) message each other without ever typing over the human at the
keyboard. A PTY proxy owns the keyboard, deterministic guards decide what gets
delivered and when, and an optional analyst (Muse Glimmer) reads the screens to
judge whether an agent is ready for a message.

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
Codex and Claude Code) under the direction of a human maintainer, and the agents
also review and integrate it. Each commit names the harness that wrote it (see
`AUTHORS.md`), and each pull request declares its authorship (see
`CONTRIBUTING.md`).

## Install

From scratch, with Go 1.27 or later:

```sh
go install github.com/smellak/gifford/cmd/gifford@latest
export PATH="$PATH:$(go env GOPATH)/bin"   # go install puts gifford there
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
- **Muse Code**: turn off *Prompt hint* (Settings → Prompt hint, or `"tui": {"prompt_hint_enabled": false}` in `~/.config/muse/settings.json`). Muse draws its tip in the input box, and Gifford reads it as the human's draft: it keeps that screen from the Analyst and holds deliveries while the tip shows.
- **Muse Code on Ubuntu 24.04**: the default AppArmor setting restricts unprivileged user namespaces, so Muse's shell sandbox fails with `bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted` and no shell command runs. Either run Muse with `--disable-sandbox`, preferably under a dedicated user, or allow bwrap through an AppArmor profile.
- **Antigravity**: allow Gifford's tools in `~/.gemini/antigravity-cli/settings.json`. Without it, agy 1.4.8 asks before every MCP tool call, so it cannot take a message without a person. The narrowest setting that lets it read, acknowledge and answer is:
  ```json
  {"permissions": {"allow": ["mcp(gifford/list_pending_messages)", "mcp(gifford/read_message)", "mcp(gifford/ack_message)", "mcp(gifford/send_message)"]}}
  ```
  Merge it into the file's existing keys, and keep the file 0600. Every other tool keeps asking.
- **OpenCode**: a new session draws an example prompt inside the input box until its first exchange. Gifford reads it as text in the box, keeps that screen from the Analyst and holds automatic deliveries until then. Send one prompt first.
- **Node.js launchers**: when the command after `--` is a Node.js script that starts the harness as a child process instead of replacing itself, the harness runs as two processes. The process guard binds exactly one harness process, so it defers every automatic delivery, and `gifford run` warns when the command is a Node.js launcher script. Run the harness binary itself as the command. Measured with Codex 0.154.0 installed by npm on linux-x64: `codex` is such a launcher. Run its native binary with the arguments you would give `codex`, and set the two variables the launcher sets, `CODEX_MANAGED_BY_NPM=1` and `CODEX_MANAGED_PACKAGE_ROOT`, the package's real path:
  ```sh
  root="$(npm root -g)/@openai/codex"
  CODEX_MANAGED_BY_NPM=1 CODEX_MANAGED_PACKAGE_ROOT="$(realpath "$root")" \
    gifford run --agent codex-1 --harness codex -- \
    "$root/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/bin/codex"
  ```
  Set the variables before `gifford run`, never as `-- env …`: `gifford run` measures the harness version by running the command with `--version`, so the command must be the binary itself. Even then, Codex 0.154.0 starts a helper process at its first tool call, `codex-code-mode-host` (process name `codex-code-mode`), and it stays alive: later automatic deliveries defer too while it runs. Turning off `features.code_mode_host` removes it, but then Codex cannot use tools.
- **After restarting the node** (`gifford down` and `gifford up`), restart the harnesses too: their `gifford mcp` keeps the connection to the node that stopped, and every tool call answers "Hub unavailable" until the harness restarts.

## Basic flow

```sh
gifford panel             # watch agents, messages and decisions
gifford ctl --help        # control the node from scripts
```

## Roadmap

These commands are not in v0.1-hackathon. They stay out of `--help`, and calling one prints "not implemented in v0.1-hackathon":
- `gifford relay` and `gifford install-systemd`;
- `gifford capture attach`, `export`, `ingest`, `reconcile`, `status`, `stop` and `supervise`;
- governed loops: `gifford ctl loop create`, `next` and `complete`;
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

The analyst is optional and off after `gifford init`. When `state_analysis` is
configured, it runs on **Muse Glimmer** (Meta, open weights) behind the model
gateway; the reference server in `ml/serve/` runs it with vLLM on Modal. Without
it, Gifford does not deliver on its own: each delivery waits for a human
approval. Evidence of the model in use goes to `docs/RESULTS.md`.

## Third-party

See `NOTICE`. Dependencies are declared in `go.mod`.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md): what is built, how a delivery works, and what is not built
- [`docs/RESULTS.md`](docs/RESULTS.md): measured results, each with its command and commit
- [`docs/ML_CONTRACT.md`](docs/ML_CONTRACT.md): the Analyst's model contract, point by point
- [`docs/FREEZE.md`](docs/FREEZE.md): what is frozen, and what enforces it
- `AGENTS.md`: guide for implementing agents
- `CONTRIBUTING.md`: branches, commits, pull requests, authorship
- `docs/`: layout, testing, commit messages, decision records

## License

Apache-2.0. See `LICENSE`.
