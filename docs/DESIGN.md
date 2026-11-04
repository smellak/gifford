# Design

Gifford is a communication network for interactive coding agents. It routes
the message body through MCP and uses a short terminal notice only when the
selected last-mile path can justify it. This page describes what is built on
`main`; what is not built is listed at the end, and measured results are in
[RESULTS.md](RESULTS.md).

## The problem

Claude Code, Codex, Muse Code, OpenCode and Antigravity are terminal programs
with a prompt. An automation that wants to tell one of them something usually
types into that prompt, for example with `tmux send-keys`. If a person is typing
in the same prompt, the two byte streams interleave, and the agent receives a
line that neither of them wrote. A timeout does not fix it: a pause of minutes
can belong to the same draft. In the fuzzer's baseline, where a synthetic
person and an injector share one terminal, about a third of the lines come out
corrupt ([RESULTS.md](RESULTS.md)).

Gifford's strongest path removes the race by topology. `gifford run` puts a
PTY proxy between the person's terminal and the harness. At that guarantee
level the proxy is the sole writer, parses every key the person sends and
refuses to write while a draft is open or while it cannot tell. Gifford can
also attach to an existing tmux pane, but that compatibility path cannot make
the same atomicity claim and exposes its residual race explicitly.

## Two official decision routes

Both routes begin with a stored message and end, at most, in one allowed
last-mile operation. Neither route bypasses identity, version, channel,
freshness, human-input or commit-time guards.

1. **Route A — deterministic PTY.** A version-pinned detector reports the
   harness, input box and activity state. Deterministic guards verify that the
   verdict is certain, fresh and stable, then the orchestrator may choose a
   delivery. No model call is required.
2. **Route B — Glimmer-assisted.** The Analyst interprets the projected screen,
   prioritises and classifies the state; a deterministic detector corroborates
   it; deterministic guards bound the available actions; and the orchestrator
   chooses between those actions. Glimmer may veto or defer a candidate. It
   never authorises a write, weakens a guard or makes an unmeasured harness
   deliverable.

If the model route is unavailable, its breaker can fall back to the base model
or the independent deterministic route according to the recorded policy. That
fallback is not evidence that the model approved the delivery.

## Last-mile guarantee levels

| Level | Delivery path | Guarantee |
|---|---|---|
| **A — managed PTY** | `gifford run` owns the child PTY; the Proxy performs prepare/commit. | The Proxy is the sole writer. A key or screen change between phases aborts the attempt. This is the only level to which “only the Proxy writes” applies. |
| **B — controlled tmux pane** | The node locks a registered pane, captures and rechecks it, then the privileged helper sends one fixed notice and Enter. | Locks, process checks, fresh captures and residual verification reduce risk, but the final check and `send-keys` are not atomic. A check-to-send race remains and is reported as such. |
| **C — observation only** | MCP, hooks/plugins/mods and captures can report state or leave a message pending. | No automatic terminal write is claimed. This is the fallback for unsupported or unmeasured delivery paths. |

Hooks, plugins and mods are observers, not delivery paths. Their event may
strengthen evidence, but a successful observer handshake does not upgrade the
last-mile guarantee level.

## Component responsibilities

The person always has priority and can switch the channel off. Each component
has one job:

- The channel control plane allows, holds or blocks each message and delivery.
- The proxy protects the person's draft and carries the one delivered line.
- The Analyst interprets, prioritises and classifies an agent's state, and only
  advises or vetoes.
- The orchestrator chooses among the actions the rules already allow; its
  executor is the only caller of delivery.
- The hub routes signed envelopes. The agent reads the body through MCP, never
  through its prompt.
- Every reply between agents belongs to a loop with a finite budget.

## Layers

| Layer | On `main` | Packages |
|---|---|---|
| Last mile | Three explicit levels: managed PTY, controlled tmux and observation only. Optional harness emitters under `adapters/` are observe-only; no native in-harness delivery is shipped. | `adapters`, `internal/proxy`, `internal/proxy/tmuxattach`, `internal/tmuxhelper` |
| Human keyboard | Managed PTY: sole writer plus the Human Draft Guard (`CLOSED`, `OPEN`, `PASTING`, `SUBMITTING`, `UNKNOWN`) and two-phase delivery. Controlled tmux: locks and rechecks with a documented residual race. | `internal/proxy/...`, `internal/proxy/tmuxattach`, `internal/tmuxhelper` |
| Perception | The Analyst settles the state from harness events or an exited process without a model, otherwise asks Muse Glimmer for `ANALYZE_AGENT_STATE` through the model gateway. It runs only if `config.yaml` has a `state_analysis` section, which `gifford init` does not write. | `internal/analyst`, `internal/gateway`, `internal/screenproj` |
| Learning | Recordings, labels, deterministic dataset building, evaluation, adapter gates and lifecycle control. Training ran outside the public runtime repository; adapter weights stay outside Git. | `internal/capture`, `internal/label`, `internal/dataset`, `internal/eval`, `internal/modelprofile`, `ml/serve` |
| Governance | Versioned controls in six scopes (global, node, agent, directional link, thread, loop); the strongest restriction wins; loop budgets and approvals. | `internal/channel` |
| Decision | Deterministic guards decide what is allowed. The deterministic detector route needs no model. On the assisted route, Glimmer classifies and may veto while a corroborating detector and the guards retain authority over the write boundary. | `internal/orchestrator`, `internal/triage`, `internal/detect` |
| Distribution | A local hub on a Unix socket; federation over HTTPS with TLS 1.3 and client certificates, enrolled with single-use tokens. | `internal/hub`, `internal/federation`, `internal/registry` |
| Documents | Artifacts encrypted end to end (HPKE with X25519, ChaCha20-Poly1305), resumable, kept by the hub until every recipient acknowledges. Local only. | `internal/artifact`, `internal/hub` |

A managed-PTY delivery to agent B works like this. A's harness calls `send_message`, and
`gifford mcp` signs the envelope with A's Ed25519 key. The hub binds each
connection to the agent whose `gifford run` session is an ancestor of the
caller (Linux peer credentials) and admits the message through the channel
control plane in the transaction that stores it. The orchestrator decides. A
person's approval (`gifford ctl approve <id> --deliver-now` or the panel)
replaces only the model's idle check, `p_idle_threshold`; every other guard
still applies. If B's automatic delivery is off (manual mode, or after
`gifford ctl pause`), the approval releases that one delivery, once, and
automatic delivery stays off. Approving the same request again from
`gifford ctl` fails.

B's proxy writes `: [GIFFORD] 1 pending message` (the default notice) only if its
profile is measured, the draft is `CLOSED` at the generation and version the
orchestrator saw, no key sequence is half read, the harness has printed nothing
for 300 ms and the screen shows an empty input line under the cursor. A human
key between prepare and commit aborts it. B then calls `read_message` and
`ack_message`.

For a controlled tmux pane, the node applies the same control and decision
boundary, then locks the registered pane, captures it, verifies the measured
detector/profile and process tree, and delegates only the fixed notice plus
Enter to `gifford-tmux`. It captures again around the operation and records the
outcome. Unlike the managed PTY path, another tmux client can still type after
the final check and before the helper sends the bytes; this path therefore
retains a residual check-to-send race.

## Processes

```text
 person's terminal                          person's terminal
        |                                           |
 +---------------------------+              +----------------+
 | gifford run --agent A     |<------+      | gifford panel  |
 | PTY proxy, one per agent  |       |      | gifford ctl    |
 +---------------------------+       |      +----------------+
   | child PTY (sole writer)         |              |
   v                                 |              |
 +---------------+                   |              |
 | harness A     |                   |              |
 +---------------+                   |              |
   | MCP over stdio                  |              |
   v                                 |              |
 +---------------+                   |              |
 | gifford mcp   |                   |              |
 +---------------+                   |              |
   | hub.sock                        | agent socket | daemon.sock
   v                                 | (node dials) v
 +-------------------------------------------------------------+
 | node process: gifford up, hub or daemon (one per node)      |
 |   local hub                 channel control plane           |
 |   orchestrator, executor    proxy registry, notifications   |
 |   event store gifford.db (SQLite, WAL), sole online writer  |
 |   model gateway             only with state_analysis        |
 |   federation server         only with federation configured |
 +-------------------------------------------------------------+
   | HTTPS, Bearer token                  | HTTPS, TLS 1.3,
   v                                      v client certificates
 Glimmer endpoint                       other nodes
 (ml/serve: vLLM behind a thin endpoint)
```

`gifford hub`, `gifford daemon` and `gifford up` start the same node process; a
second hub or daemon on the same directories refuses, and `gifford up` leaves a
running node alone. `gifford run --agent` registers with the hub over
`hub.sock` and listens on `<runtime>/agents/<agent>.sock`, which the node dials.

## Invariants the repository checks by script

`scripts/check_guards.sh` runs `git grep` over tracked files and fails on any
hit. CI runs it as `task lint:guards` in the `verify` job, a required check on
`main`. It checks exactly these patterns:

1. `send-keys` appears only in Markdown, `docs/`, the script itself,
   `test/fuzz/baseline/` and `test/conformance/`, and in `test/conformance/`
   only as `[]string{"send-keys", "-H", "-t", session.target}`, which types
   simulated human input in hexadecimal.
2. Every line of a non-Markdown file under `internal/` that mentions
   `pipe-pane` contains `-O`, and none has `-I` after `pipe-pane`.
3. Under `internal/proxy/`, `ptmx.Write` and `pty.Write(` appear only in
   `internal/proxy/writer.go`.
4. Under `internal/`, outside tests, the names `PreparePTYDelivery`,
   `DeliverNativeEvent` and `EmitFileReady` appear only in
   `internal/orchestrator/executor.go`, `internal/artifacts/ready.go` and
   `internal/channel/`; the agent-socket delivery operations
   (`AgentOpPrepareDelivery`, `AgentOpCommitDelivery`, `AgentOpCancelDelivery`,
   `AgentOpNativeDelivery`, or a conversion such as
   `AgentOp("prepare_pty_delivery")`) only in `executor.go`,
   `internal/protocol/` and `internal/agentsock/`.
5. Under `internal/`, SQL that inserts, replaces, updates or deletes rows of
   `channel_controls`, `communication_loops`, `channel_control_decisions`,
   `approval_requests` or `channel_epoch` appears only in `internal/channel/`,
   and nothing else outside tests assigns a `.ControlState` field.
6. No quoted `channel`, `agent_channel`, `link`, `loop` or `approval` method
   name, such as `"channel.set_global"`, appears in `internal/endpoint/`, so no
   MCP tool exposes a control method.
7. In `internal/proxy/session/`, outside `writer.go` and tests, no line names
   the middle PTY `midM` together with `Write` or `writeFull`, except where
   `midM` is handed to the session writer.

These are text patterns, not proofs, and on this commit some match nothing: no
code calls `tmux pipe-pane`, `internal/artifacts/ready.go` does not exist, and
none of the three names in check 4 is defined. The test
`internal/orchestrator/guard_test.go` plants five violations of check 4 in a
scratch repository and confirms the script fails on each. The rule that
nothing is delivered without an allowing `channel.Decision` is held by the type
instead: `Decision` has only an unexported field, only the channel engine
creates one, and the executor refuses to act on one that does not allow.

## What is not built

- Training is not performed by the public runtime repository, and adapter
  weights are never committed to it. The recorded external adapter and its
  runtime lifecycle are recorded in [ML_CONTRACT.md](ML_CONTRACT.md). A
  recorded operator override is not a passed six-criterion gate.
- Live observation of tmux panes: `gifford capture` offers only `import`.
- `gifford relay` and `gifford install-systemd`. Cross-node artifacts are
  implemented and covered by an automated test
  (`internal/federation` replicator with the `QUEUED_REMOTE`, `REPLICATING`
  and `STORED_REMOTE` states; `TestFederatedArtifactRoundTripOverMTLS` in
  `internal/hub`), but no external party has accepted them: the judge build
  does not claim them, and the federation server's artifact and revoke routes
  answer HTTP 501 until that acceptance.
- Message encryption: messages are signed, and their only encryption profile
  is `none`. Only artifacts are encrypted end to end.
- A way out of `UNKNOWN`: `gifford ctl unlock` is not implemented and nothing
  outside tests calls the guard's `Unlock`, so an agent whose guard reaches
  `UNKNOWN` takes no delivery until its `gifford run` restarts.
- Profiles beyond the exact versions in
  [COMPATIBILITY.md](COMPATIBILITY.md). Claude Code 2.1.316 has an observe-only
  mod but no measured PTY profile. Muse Code 1.4.14-R7309.1 has a measured
  profile since PR #520; its native plugin handshake stays pinned to 1.4.13
  and falls back to PTY evidence. An unmeasured profile refuses automatic
  terminal delivery.
- Creating, stepping or completing a governed loop from `gifford ctl`, and
  several manual controls (`deliver-next`, `hold-message`, `release-hold`,
  `inspect`, `replay` and others); the README lists them. Also
  `gifford ctl loop stop --cancel-turn`, and every form of
  `gifford ctl approve` except `--deliver-now` (`--once`, `--thread`,
  `--steps`, `--add-turns` and `--mode`).
- Other platforms: the hub identifies peers only on Linux, releases are built
  for Linux only, and CI only cross-compiles for macOS and Windows.
- Recorded results for the per-harness conformance battery. See
  [RESULTS.md](RESULTS.md).
