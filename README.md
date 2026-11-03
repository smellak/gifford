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

### Binary release (Linux, no Go required)

For a fresh installation on Linux amd64 or arm64, with `curl`, CA certificates,
`tar`, `sha256sum` and `sudo` available, run this single command. It installs the
[`v0.1.0-rc.7` release candidate](https://github.com/smellak/gifford/releases/tag/v0.1.0-rc.7).

```sh
gifford_installer="$(curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' https://raw.githubusercontent.com/smellak/gifford/v0.1.0-rc.7/scripts/install.sh)" && printf '%s\n' "$gifford_installer" | sudo sh -s -- --version v0.1.0-rc.7 --bin-dir /usr/local/bin && gifford init && gifford doctor
```

This downloads the complete installer from the same tag as the binary, checks
the archive against that release's `checksums.txt`, and installs only the binary
with elevated permissions. `init` and `doctor` run as the calling user: they
create and check that user's configuration. No node, system service, model or
agent is started, and an existing binary or configuration is not overwritten.
`/usr/local/bin` must be in `PATH`. A successful `doctor` here checks the fresh
installation, not message delivery or an active node.

### Build from source (developers)

This alternative requires Go 1.27 or later already installed:

```sh
go install github.com/smellak/gifford/cmd/gifford@latest
export PATH="$PATH:$(go env GOPATH)/bin"   # go install puts gifford there
gifford init              # write config.yaml and create the state directories
gifford doctor            # check them
```

### Start a node

After either installation route:

```sh
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
- **Node.js launchers with `gifford run`**: when the command after `--` is a Node.js script that starts the harness as a child process instead of replacing itself, the harness runs as two processes. The process guard binds exactly one harness process, so it defers every automatic delivery, and `gifford run` warns when the command is a Node.js launcher script. Run the harness binary itself as the command. Measured with Codex 0.154.0 installed by npm on linux-x64: `codex` is such a launcher. Run its native binary with the arguments you would give `codex`, and set the two variables the launcher sets, `CODEX_MANAGED_BY_NPM=1` and `CODEX_MANAGED_PACKAGE_ROOT`, the package's real path:
  ```sh
  root="$(npm root -g)/@openai/codex"
  CODEX_MANAGED_BY_NPM=1 CODEX_MANAGED_PACKAGE_ROOT="$(realpath "$root")" \
    gifford run --agent codex-1 --harness codex -- \
    "$root/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/bin/codex"
  ```
  Set the variables before `gifford run`, never as `-- env …`: `gifford run` measures the harness version by running the command with `--version`, so the command must be the binary itself. The measured Codex 0.154.0 profile permits its persistent `codex-code-mode-host` helper when it is the harness's direct child and its executable is beside the harness binary. That helper alone does not block automatic delivery; additional tool processes still do. Keep `features.code_mode_host` enabled so Codex can use tools. This exception does not permit a persistent Node.js launcher under `gifford run`; an agent already running under that launcher uses the separate tmux registration below.
- **After restarting the node** (`gifford down` and `gifford up`), restart the harnesses too: their `gifford mcp` keeps the connection to the node that stopped, and every tool call answers "Hub unavailable" until the harness restarts.

### Agents already running in tmux

An agent that is already running in tmux can be registered without relaunching
it through `gifford run`. The hub reads `instances.yaml` from Gifford's config
directory: `$XDG_CONFIG_HOME/gifford/instances.yaml`, or
`$HOME/.config/gifford/instances.yaml` when `XDG_CONFIG_HOME` is unset. A
system node therefore uses `/etc/gifford/instances.yaml`. To use another file,
set a clean absolute path in `config.yaml`:

```yaml
tmux_instances:
  statements: /srv/gifford/instances.yaml
```

The statements file and its directory must be owned by root or by the user
running the hub, must not be writable by group or other users, and cannot be
symlinks. Root is refused as either user; the hub's uid and any configured
bridge uid are also refused as `agent_user`. This is the minimal one-agent
form for an npm-installed Codex that was started from a shell in tmux (the
example assumes `alice` has uid 1000):

```yaml
schema_version: 1
instances:
  - agent_id: codex-1
    agent_user: alice
    owner_user: alice
    socket: /tmp/tmux-1000/default
    pane_id: "%3"
    pane_comm: bash
    harness_comm: node
    harness_argv0: node
    harness_argv1: codex
    harness: codex
    harness_version: "0.154.0"
    generation: 1
```

The user and process fields must match tmux and `/proc`: `owner_user` owns
the tmux server, `agent_user` runs the CLI, `pane_comm` names the pane process,
and `harness_comm` names either that process or its only direct child with
that name. `harness_argv0` is the basename of argv[0]. For `node`, `bun` or
Python interpreters, `harness_argv1` is also required and is the basename of
argv[1]; omit it for a native CLI. Quote the pane id because an unquoted YAML
value cannot start with `%`. `generation` starts at 1. A missing file is
skipped, while an explicit `instances: []` revokes every active tmux instance
on the next reconciliation pass.

For the npm Codex shape above, Gifford registers the retained `node` launcher.
The work guard follows its native child only when the registered launcher is
still present at the same PID and start time, `harness_argv1` is `codex`, and
exactly one direct child has comm `codex`, a positive PID and start time, and
an absolute argv[0] whose basename is `codex`. The registered launcher must
also remain in the pane's foreground and must not be stopped. If any condition
cannot be proved, automatic delivery stays busy and the guard does not
establish a resting baseline. Once they are proved, the native child anchors
the work census, including the measured `codex-code-mode-host` exception
described above; any new tool process remains in the census and blocks
delivery.

Run `gifford doctor` after saving the file. It loads the same configured path
as the hub and reports what the registry holds for each current statement; it
opens the database read-only:

| Registry state | `gifford doctor` | Meaning |
|---|---|---|
| ACTIVE | `INSTANCE    OK   codex-1 on pane %3 since …` | The exact statement has an active registered instance. |
| PENDING | `INSTANCE    WAIT codex-1: no pass has reconciled it yet` | No reconciliation pass has recorded the statement yet. |
| REFUSED | `INSTANCE    FAIL codex-1 refused: <reason> since …` | Registration was rejected; the reason is shown and the hub retries it on later passes. |
| INVALIDATED | `INSTANCE    FAIL codex-1 invalidated: <reason> since …; raise its generation to register it again` | The instance was active but its binding stopped matching or was revoked. Raise only `generation` to register it again. |

For a system node, see [Agents already running in tmux](docs/system-node.md#agents-already-running-in-tmux)
and [The node socket](docs/system-node.md#the-node-socket) for `node.sock` and
its `SO_PEERCRED` identity rules.

## Basic flow

```sh
gifford panel             # watch agents, messages and decisions
gifford ctl --help        # control the node from scripts
```

If a message stays unattended, the operator receives a normal message,
filed and signed by the node's system sender through the usual
list, claim and acknowledge paths — not a side channel. The operator
reads it with Gifford's MCP tools: the bridge mirrors the source
inbox into Gifford, never back, so nothing reflects the notice to
the source.

## Tested with real agents

No simulations: every figure in [`docs/RESULTS.md`](docs/RESULTS.md)
names the command that produced it and the commit it ran on. The
live message path ran end to end, with a recorded read and reply,
on Codex, Muse Code and OpenCode; files cross nodes with matching
SHA-256 on both ends. Panels that share one terminal are serialized
by the cooperative pane-lock protocol
([`docs/pane-locks.md`](docs/pane-locks.md)).

## Native observer plugins

Observe-only plugins report content-free lifecycle events through
the native socket, so the node can see without reading: the
[OpenCode native-events plugin](adapters/opencode/README.md) and
the [Claude Code hooks](adapters/claude-code/README.md). Absent a
plugin, its harness keeps working; the observer simply sees less.

## Roadmap

These commands are not in v0.1-hackathon. They stay out of `--help`, and calling one prints "not implemented in v0.1-hackathon":
- `gifford relay` and `gifford install-systemd`;
- `gifford capture attach`, `export`, `ingest`, `reconcile`, `status`, `stop` and `supervise`;
- governed loops: `gifford ctl loop create`, `next` and `complete`;
- the rest of manual control:
  - `gifford ctl deliver-next`, `hold-message`, `release-hold`, `approve-message`, `reject-message` and `unlock`;
  - `gifford ctl inspect`, `replay`, `retry-artifact` and `revoke-node`;
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

On a machine with a shared node, an agent running as another user has no
runtime socket of its own. It reaches the node through the shared socket
with `--socket`, which works on all four subcommands and must be an
absolute path. Without `--socket`, everything works exactly as above;
a `--socket` that points nowhere fails instead of trying another socket.

```sh
gifford artifact --socket /run/gifford/node.sock send ./report.pdf --to reviewer
```

## Model

The analyst is optional and off after `gifford init`. When `state_analysis` is
configured, it runs on **Muse Glimmer** (Meta, open weights) behind the model
gateway; the reference server in `ml/serve/` runs it with vLLM on Modal.
Evidence of the model in use goes to `docs/RESULTS.md`.

Without `state_analysis`, each route behaves as its guards dictate. A pane
that is already running in tmux still delivers on its own when the
deterministic detector reports it free: the recorded Codex round trip
committed its notice, read, and reply without Glimmer. A proxied harness
instead holds every delivery for human approval, as in the two-node check
where a person approved each delivery. Native-entry agents are observed,
never injected: they read their inbox with the MCP tools.

For panes already running in tmux, the detector corroborates an idle
screen before delivering below the model threshold: in the recorded
run a free OpenCode agent with calibrated idle probability 0.557,
below the 0.639 threshold, received its notice because the detector
confirmed the agent was free. Under a threshold-only rule no notice
would have been sent. See [`docs/RESULTS.md`](docs/RESULTS.md).

## Third-party

See `NOTICE`. Dependencies are declared in `go.mod`.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md): what is built, how a delivery works, and what is not built
- [`docs/pane-locks.md`](docs/pane-locks.md): the cooperative pane-lock protocol for panels sharing one terminal
- [`docs/RESULTS.md`](docs/RESULTS.md): measured results, each with its command and commit
- [`docs/ML_CONTRACT.md`](docs/ML_CONTRACT.md): the Analyst's model contract, point by point
- [`docs/FREEZE.md`](docs/FREEZE.md): what is frozen, and what enforces it
- [`docs/system-node.md`](docs/system-node.md): one node per machine, run as a system service
- `AGENTS.md`: guide for implementing agents
- `CONTRIBUTING.md`: branches, commits, pull requests, authorship
- `docs/`: layout, testing, commit messages, decision records

## License

Apache-2.0. See `LICENSE`.
