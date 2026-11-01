# Design

Gifford lets interactive coding agents send each other messages without
anything ever typing over the person at the keyboard. This page describes what
is built on `main`; what is not built is listed at the end, and measured
results are in [RESULTS.md](RESULTS.md).

## The problem

Claude Code, Codex, Muse Code, OpenCode and Antigravity are terminal programs
with a prompt. An automation that wants to tell one of them something usually
types into that prompt, for example with `tmux send-keys`. If a person is typing
in the same prompt, the two byte streams interleave, and the agent receives a
line that neither of them wrote. A timeout does not fix it: a pause of minutes
can belong to the same draft. In the fuzzer's baseline, where a synthetic
person and an injector share one terminal, about a third of the lines come out
corrupt ([RESULTS.md](RESULTS.md)).

Gifford removes the race by topology. `gifford run` puts a PTY proxy between
the person's terminal and the harness, and the proxy is the only process that
writes to the harness's terminal. It parses every key the person sends, so it
knows when a draft is open, and it refuses to write while one is open or while
it cannot tell.

## The central rule

Only the proxy writes to an agent's terminal, and it writes one short line only
when every check allows it. The person always has priority and can switch the
channel off. Around that rule each part has one job:

- The channel control plane allows, holds or blocks each message and delivery.
- The proxy protects the person's draft and carries the one delivered line.
- The Analyst reads an agent's state, and only advises.
- The orchestrator chooses among the actions the rules already allow; its
  executor is the only caller of delivery.
- The hub routes signed envelopes. The agent reads the body through MCP, never
  through its prompt.
- Every reply between agents belongs to a loop with a finite budget.

## Layers

| Layer | On `main` | Packages |
|---|---|---|
| Last mile | The generic PTY path works with any CLI. A native adapter may listen on `<runtime>/adapters/<agent>.sock`; its manifest is verified, any failure falls back to the generic path, and it never writes to the PTY. Optional observe-only harness emitters live under `adapters/`. | `adapters`, `internal/proxy`, `internal/adapters` |
| Human keyboard | One writer; the Human Draft Guard (`CLOSED`, `OPEN`, `PASTING`, `SUBMITTING`, `UNKNOWN`); delivery in two phases, prepare then commit. | `internal/proxy/...` |
| Perception | The Analyst settles the state from harness events or an exited process without a model, otherwise asks Muse Glimmer for `ANALYZE_AGENT_STATE` through the model gateway. It runs only if `config.yaml` has a `state_analysis` section, which `gifford init` does not write. | `internal/analyst`, `internal/gateway`, `internal/screenproj` |
| Learning | Recordings, labels, a dataset build with a readiness gate, evaluation and an adapter gate. No training code, no adapter. | `internal/capture`, `internal/label`, `internal/dataset`, `internal/eval` |
| Governance | Versioned controls in six scopes (global, node, agent, directional link, thread, loop); the strongest restriction wins; loop budgets and approvals. | `internal/channel` |
| Decision | Deterministic guards decide what is allowed. With the model configured, `TRIAGE_MESSAGE` classifies the message and `CHOOSE_ORCHESTRATION_ACTION` picks between injecting and deferring when both are allowed. Without it, every delivery waits for a person's approval. | `internal/orchestrator`, `internal/triage` |
| Distribution | A local hub on a Unix socket; federation over HTTPS with TLS 1.3 and client certificates, enrolled with single-use tokens. | `internal/hub`, `internal/federation`, `internal/registry` |
| Documents | Artifacts encrypted end to end (HPKE with X25519, ChaCha20-Poly1305), resumable, kept by the hub until every recipient acknowledges. Local only. | `internal/artifact`, `internal/hub` |

A delivery to agent B works like this. A's harness calls `send_message`, and
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

B's proxy writes `[GIFFORD] 1 pending message` (the default notice) only if its
profile is measured, the draft is `CLOSED` at the generation and version the
orchestrator saw, no key sequence is half read, the harness has printed nothing
for 300 ms and the screen shows an empty input line under the cursor. A human
key between prepare and commit aborts it. B then calls `read_message` and
`ack_message`.

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

- No adapter has been trained, and nothing here trains one: `ml/` holds only
  the serving endpoint, `gifford ctl model activate` and `rollback` are not
  implemented, and two criteria of the adapter gate are not implemented, so it
  cannot pass. See [RESULTS.md](RESULTS.md).
- Live observation of tmux panes: `gifford capture` offers only `import`.
- `gifford relay` and `gifford install-systemd`. Artifacts stay on one node:
  the hub refuses a recipient on another node, and the federation server
  answers its artifact and revoke routes with HTTP 501.
- Message encryption: messages are signed, and their only encryption profile
  is `none`. Only artifacts are encrypted end to end.
- A way out of `UNKNOWN`: `gifford ctl unlock` is not implemented and nothing
  outside tests calls the guard's `Unlock`, so an agent whose guard reaches
  `UNKNOWN` takes no delivery until its `gifford run` restarts.
- Profiles beyond five exact versions: Claude Code 2.1.280, Codex 0.154.0, Muse
  Code 1.4.13-R6996.1, OpenCode 1.18.29 and Antigravity 1.4.8 are measured. Any
  other version gets the generic profile, which is unmeasured, and an
  unmeasured profile refuses every delivery.
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
