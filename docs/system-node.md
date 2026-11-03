# One node per machine

[`gifford.service`](gifford.service) runs one Gifford node per machine as the
system user `gifford`, never as root.

## Layout

| Path | Owner and mode | Holds | Made by |
|---|---|---|---|
| `/etc/gifford` | `gifford`, `0700` | `config.yaml` (`0600`), `instances.yaml` and `policies/` | the administrator |
| `/var/lib/gifford` | `gifford`, `0700` | the database, the node keys and the artifacts | systemd, `StateDirectory=` |
| `/run/gifford` | `gifford`, `0755` | `node.sock` (`0666`) when `node_socket` is set, the shared `native.sock` (`0666`) next to it, and the runtime directory `gifford/` (`0700`): `hub.sock`, `daemon.sock` and `hub.lock` | systemd, `RuntimeDirectory=`; removed when the service stops |

The unit sets `XDG_CONFIG_HOME=/etc`, `XDG_DATA_HOME=/var/lib` and
`XDG_RUNTIME_DIR=/run/gifford`. Gifford resolves its config, data and runtime
directories from those variables, as it does for a node run by a user, so no
code path is specific to the service.

Keep anything else the node needs, such as per-pane lock files, out of
`/var/lib/gifford`. The node refuses a data directory that is not exactly
`0700`, and systemd resets the mode of its `StateDirectory=` at every start,
so a directory there cannot carry access for other users.

## Install

Build the commit you install with `task --force build`, and record
`sha256sum bin/gifford bin/gifford-tmux` next to the commit. The helper uses
the same `CGO_ENABLED=0 go build -trimpath` recipe here and in release archives,
so those two builds are byte-for-byte comparable. Then, as root, from that
checkout:

```sh
install -m 0755 bin/gifford /usr/local/bin/gifford
useradd --system --user-group --home-dir /var/lib/gifford --no-create-home \
  --shell /usr/sbin/nologin gifford
install -d -m 0700 -o gifford -g gifford /etc/gifford
# The node id takes lowercase letters, digits, '.', '_' and '-'.
(umask 077; printf 'schema_version: 1\nnode_id: %s\n' node-<name> > /etc/gifford/config.yaml)
chown gifford:gifford /etc/gifford/config.yaml
install -m 0644 docs/gifford.service /etc/systemd/system/gifford.service
systemctl daemon-reload
systemctl enable --now gifford.service
```

`journalctl -u gifford` then shows `gifford hub: ready: serving
/run/gifford/gifford/hub.sock and /run/gifford/gifford/daemon.sock`.

### Privileged helper manifest upgrades

When enabling the response-required helper slot, deploy the new
`gifford-tmux` binary before publishing a manifest that contains
`notice_response`. The new field is optional for the new helper, so that binary
continues to accept the previous manifest. The previous helper rejects the new
manifest because registry parsing rejects unknown fields.

After the binary is in place, publish the canonical manifest with
`notice_response`, update `tmux_attach.helper_manifest_sha256` to that manifest's
digest, and restart the node. The `bridge.response_required_notice_line` value
must be byte-for-byte identical to the manifest's `notice_response`; otherwise
the helper transport refuses that delivery before writing to the pane.

## The node socket

`node_socket` in `config.yaml` opens a socket that every local user can
connect to:

```yaml
node_socket:
  path: /run/gifford/node.sock
  bridge_uid: <uid>
```

The node opens its separate native-adapter entry beside the configured node
socket. With the configuration above it is `/run/gifford/native.sock`; it is
not multiplexed through `node.sock`. Both sockets have mode `0666`, but the
node classifies every connection from its kernel credentials. Without a
`node_socket` configuration, the native-adapter entry remains
`<runtime>/native.sock`.

The node classifies each connection once, when it accepts it, from the
kernel's SO_PEERCRED and never from anything the client sends:

- `bridge_uid` reaches only the bridge methods. It is a decimal uid, never a
  user name, and it must be neither 0 nor the node's own uid;
- root and the node's own uid are closed: administrators use the private
  control socket;
- any other uid must descend from the process of a live agent session that
  runs as that same uid. It reaches that agent's methods, and never
  registration;
- anything else is closed before the node reads a byte.

`bridge.notice_line` replaces the default bridge notice,
`: [GIFFORD] 1 pending message: check your inbox`.
`bridge.response_required_notice_line` likewise replaces
`: [GIFFORD] response required: check your inbox and reply or report a block`
for response reminders.

## Files between agents

An agent running as another user has no runtime socket of its own, so
`gifford artifact` reaches the node through the shared socket instead.
Pass its path with `--socket` on `send`, `list`, `receive` and `status`;
the path must be absolute. The agent's keys stay in the data directory of
the user who runs the command, and publishing file keys never moves the
agent's main endpoint.

```sh
gifford artifact --socket /run/gifford/node.sock send ./report.pdf --to reviewer
gifford artifact --socket /run/gifford/node.sock list
gifford artifact --socket /run/gifford/node.sock receive art-0123456789abcdef0123456789abcdef --out ./downloads
gifford artifact --socket /run/gifford/node.sock status art-0123456789abcdef0123456789abcdef
```

Without `--socket`, every subcommand keeps using the runtime `hub.sock`
exactly as before. A `--socket` that points nowhere fails instead of
falling back to another socket.

## Bridge start and stop order

The node only learns source reads through the bridge
(`bridge_mark_read`), so a bridge that stops and returns can leave a
notice out for a message that was already read at the source. The bridge
is itself a client of the node socket, so it can only sync while the node
runs. Around any node or bridge maintenance, keep this order:

1. Pause each agent with `gifford ctl pause <agent>` and check that its
   `auto_delivery` is now off. The pause is stored in the node's
   database, so the node comes back with the agent still paused.
2. Stop the node, then the bridge.
3. Do the maintenance, such as replacing the binary, and start the node.
4. Start the bridge and wait until it has synced with the source.
5. Resume each agent with `gifford ctl resume <agent>`.

While the agents are paused, no delivery goes out on its own, so nothing
reaches a pane from reads that may be stale. Once the bridge has synced,
the reads the node has heard are current, and renotices only fire for
messages that are still unread.

## Agents already running in tmux

The administrator states which agents already run in a tmux pane in
`/etc/gifford/instances.yaml`; `tmux_instances.statements` in `config.yaml`
names another file. The real file names this machine's users and sockets, so
it stays with the deployment's config. It must be a regular file that
`gifford` can read, and both it and its directory must belong to root or
`gifford` and be writable only by their owner.

```yaml
schema_version: 1
instances:
  - agent_id: codex-1
    agent_user: dev          # the user the CLI runs as
    owner_user: dev          # the user whose tmux server holds the pane
    socket: /tmp/tmux-1001/default
    pane_id: "%3"            # quoted: a YAML value cannot start with %
    pane_comm: bash          # the pane's process: the shell the CLI was started from
    harness_comm: node       # the CLI: the pane's process or its only direct child
    harness_argv0: node      # the basename of the CLI's argv[0]
    harness_argv1: codex     # node, bun and python only: the basename of argv[1]
    harness: codex
    harness_version: "0.154.0"
    generation: 1
  - agent_id: claude-1
    agent_user: dev
    owner_user: dev
    socket: /tmp/tmux-1001/default
    pane_id: "%4"
    pane_comm: bash
    harness_comm: claude
    harness_argv0: claude
    harness: claude-code
    harness_version: "2.1.280"
    generation: 1
```

The CLI may be the pane's own process, or the only direct child of it with
the stated name, such as a CLI started from a shell prompt in the pane. npm's
Codex is `node`, which runs the native binary as its own child. The CLI
itself is never a shell. A pane whose process is the CLI itself, as with
`tmux new -s x claude`, states `pane_comm` equal to `harness_comm`. When
`pane_comm` does not match the pane's process, registration is refused with
`pane_comm`. With a shell in the pane, the node checks the
instance again before every write and invalidates it once the CLI exits. It
types the notice, and later presses Enter, only while the registered CLI
holds the pane's foreground and is not stopped. tmux cannot make a check and
a keystroke atomic. If the CLI exits before it reads the notice, the typed
line stays in the shell without Enter, and the delivery is recorded as
uncertain. If the CLI exits or stops in the milliseconds between the last
check and Enter, Enter reaches the shell; the CLI has already read the
notice by then, so the shell's line holds only what someone types at that
same instant.

`harness_comm` alone cannot tell apart the CLIs that run on an interpreter:
every one is `node`. When `harness_argv0` is `node`, `bun`, `python`,
`python3` or `python3.N`, `harness_argv1` names the program, the basename of
argv[1], as `codex` above; for any other program it stays out. These two
basenames are all the node reads and keeps of a command line, where another
user's secrets may hide. An argv[1] that starts with `-` is an option, never
a program, so such a CLI cannot be stated.

The kernel cuts a process's comm to 15 bytes, and `harness_argv0` keeps the
whole name: a CLI whose binary is `muse-bin-1.4.13-R6996.1` is stated with
`harness_comm: muse-bin-1.4.13` and `harness_argv0: muse-bin-1.4.13-R6996.1`.

At every pass of its tmux reconcile loop, the node makes its registry follow
the file:

- it registers each statement that has no active instance, once tmux and
  `/proc` show the pane, the CLI and the users it states;
- it revokes the instance of a statement that is removed or changed;
- it does not register a statement again after its instance was
  invalidated, for example because the CLI restarted. To register it again,
  raise its `generation` and change nothing else.

A missing file leaves the registry as it is, and so does a file that does
not parse; `instances: []` revokes every instance. Root is always refused,
as `agent_user` and as `owner_user`.

A statement that cannot register leaves no instance, and the next pass tries
it again. Until it registers or is removed, the node keeps the reason in
`tmux_instance_refusals` as a code that names no user, path or process:

| Code | The node found |
|---|---|
| `invalid` | a malformed statement |
| `shell` | a shell stated as the CLI's process |
| `excluded` | an agent excluded from the registry |
| `unknown_user` | no such owner or agent user |
| `root_user` | root as the owner or the agent user |
| `reserved_uid` | the node's or the bridge's uid as the agent user |
| `socket_dir` | a socket directory that is missing, a symlink, not `0700` or not the owner's |
| `tmux_unreachable` | no answer from the tmux server on the socket |
| `pane_missing` | no such pane on the socket, or its process gone |
| `pane_comm` | another program in the pane |
| `harness_missing` | not exactly one CLI process in the pane |
| `harness_program` | a CLI whose `harness_argv0` or `harness_argv1` is not the stated one |
| `agent_uid` | the CLI running as another user |
| `already_active` | the agent or the pane already active |
| `internal` | a failure of the node's own checks |

`gifford doctor`, run as `gifford` like every command below, reports each
statement from the node's database, which it opens read-only:

- `OK` when the statement has an active instance;
- `WAIT` before the first pass;
- `FAIL` with its refusal code, or with the reason its instance was
  invalidated.

## Operate

Run every gifford command for this node as `gifford`, with the same three
variables:

```sh
sudo -u gifford env XDG_CONFIG_HOME=/etc XDG_DATA_HOME=/var/lib \
  XDG_RUNTIME_DIR=/run/gifford gifford status
```

To upgrade, install the new binary and run `systemctl restart gifford`.

The unit leaves out some hardening settings on purpose. Its comments say
which ones, and why.
