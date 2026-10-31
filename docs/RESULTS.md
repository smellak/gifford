# Results

Each section names the command that produced its figures and the commit it ran on. Nothing in this file is an estimate.

## Keyboard-versus-injection fuzzer

```sh
gifford fuzz --scenarios 10000 --seed 31102026 --baseline
```

Each scenario races a synthetic person's keyboard (lines with backspace,
Ctrl-U, Ctrl-C, bracketed paste and Enter) against an injector that delivers
notices through a real session at the worst instants it can find. A scenario
through the proxy fails if the harness gets a line that is neither exactly a
notice nor exactly what the person meant, if a key never arrives as sent, if a
delivery starts with the draft open or unknown, or if a notice arrives other
than once when committed, or at all when not. `--baseline` then runs the same
10,000 scenarios without the proxy: the person and the injector write to the
same terminal.

| Through the proxy | `6fc1256` | `012f10e`, run 1 | `012f10e`, run 2 |
|---|---:|---:|---:|
| scenarios failed | 0 | 0 | 0 |
| mixed lines | 0 | 0 | 0 |
| lost keys | 0 | 0 | 0 |
| commits with the draft open or unknown | 0 | 0 of 26,158 | 0 of 26,440 |
| notices received other than once | not recorded | 0 | 0 |
| Enter pressed before the draft showed | not recorded | 0 | 0 |
| lines received | not recorded | 80,505 | 80,787 |
| notices delivered / requeued / total | not recorded | 26,158 / 3,962 / 30,120 | 26,440 / 3,680 / 30,120 |
| prepare attempts | not recorded | 95,816 | 94,963 |

| Without the proxy (baseline) | `6fc1256` | `012f10e`, run 1 | `012f10e`, run 2 |
|---|---:|---:|---:|
| scenarios that did not finish | not recorded | 0 | 0 |
| corrupt lines | 33.61 % | 27,715 of 81,552 (33.98 %) | 27,670 of 81,529 (33.94 %) |
| lost keys | 380,742 | 386,291 | 385,837 |
| notices received whole | not recorded | 13,956 of 30,120 | 13,971 of 30,120 |

- The `6fc1256` column is an earlier run of the same command at that commit of
  `main`; only the figures shown were kept from it.
- Both `012f10e` runs used a binary built at that commit with
  `go build ./cmd/gifford` (Go 1.27.3, linux/amd64). Each ran 16 scenarios at
  a time, exited with status 0, and took 1 min 36 s and 1 min 31 s
  respectively.
- The seed fixes the scripts, not the timing: the two runs on `012f10e` differ
  in every figure except the zero counts and the 30,120 notices.
- The fake harness changed between the two commits (after `3ef9861` it draws a
  line above and below its input line), so line-level figures from `6fc1256`
  and `012f10e` are not strictly comparable.
- The runs were not repeated on later commits. After `012f10e`, commit
  `a3b9e30` changed how `internal/proxy/session`, which every scenario runs,
  ends at process exit; these figures do not cover that change.

## Two nodes

The procedure is [federation-two-nodes.md](federation-two-nodes.md), and
`scripts/federation-two-node-check.sh` checks each side. It ran once, on
2026-10-31 between 18:45 and 18:55 UTC, on two machines. Both nodes ran the
same binary, built from `main` at `5a6eaf5`, and each had one OpenCode agent
under `gifford run`. Neither node had the model, so a person approved each
delivery.

| Direction | Delivery | Origin check | Destination check |
|---|---|---|---|
| A to B | one attempt, committed and acknowledged | 6 of 6 OK, and the live probe passed | 6 of 6 OK |
| B to A | one attempt, committed and acknowledged | 6 of 6 OK, with the script's queries run by hand, read-only | 6 of 6 OK |

The live probe (`--role origin -v`) passed `tls-1.3`, `tls-1.2-rejected`,
`no-client-certificate`, `unenrolled-client-certificate`,
`known-node-wrong-certificate`, `forged-origin`, `invalid-signature` and
`exact-replay`.

- **Not covered.** Delivery without approval: neither node had the model.
- **Found.** On that commit, answering a federated message with
  `reply_to_message` created nothing, neither a message nor an admission
  decision. The agent then sent its answer with `send_message`.

## Conformance battery S0–S5, per harness

**Not yet recorded.**

`task conformance` runs the suite (build tag `conformance`) against one
installed harness, chosen with `GIFFORD_CONFORMANCE_HARNESS`; a real harness
also needs `GIFFORD_CONFORMANCE_COMMAND` and `GIFFORD_CONFORMANCE_PROFILE`. The
suite writes a sanitised report in which each of S0–S5 is `PASS`, `FAIL` or
`NOT_EXECUTED`. The same report has rows for other checks too, among them
the control-plane scenarios `C1` to `C3`, some recorded as `PENDING`; this
section will take only S0–S5, each status as it came out.

- **S0**: the recipient connects with its measured profile at the exact
  version, the guard reaches `CLOSED`, and the process tree holds only the
  harness.
- **S1**: an approved delivery lands above an empty input line and reaches
  `DELIVERED`/`COMMITTED`.
- **S2**: with a human draft open, an approval does not deliver; the delivery
  is deferred.
- **S3**: an incomplete UTF-8 key from the person aborts the prepare and writes
  nothing; once the key is completed and cleared, a retry delivers exactly
  once.
- **S4**: a bracketed paste and a window resize take the guard to `UNKNOWN`,
  and an approval does not deliver.
- **S5**: the `gifford run` process is replaced; the old session ends
  `DISCONNECTED` with no process left, and the replacement, with a new session
  id, commits the message held in S4 exactly once.

## Dataset v0

Dataset v0 is recorded, labelled, split and sealed as [calibration-dataset.md](calibration-dataset.md) describes. Its recordings, windows, screens and mask stay on the recording machine; only the figures below leave it.

It was built on `main` at `8366793`:

```sh
gifford dataset build --out DIR --screen-lines 24 --mask FILE --every 8s \
  --align-config FILE --split-file FILE --scripted-input
gifford dataset inspect DIR/manifest.json
```

A rebuild from the same inputs gave the same manifest and windows, byte for byte.

### Seal

| | |
|---|---|
| `manifest.json` sha256 | `536636d506c58f67ee5c411b56941a5059b81049868db10b7a42b17781089f3f` |
| dataset digest | `sha256:28d65c555dc7e62dadd764f0ccc1e732ba9b33b0c3aab3493b342b042612f66a` |
| split file sha256 | `e468406b04b1020d1a7e2f191fa8b7ea9c84409876c7da6e006942014cd20b77` |
| align | margin 600 ms, config sha256 `8f9bfc8d15327dbafc3b5ea21a1624518c9fc8f5bae71a7a81ee1c569e83ddc1` |
| input | scripted: no human keyboard, every draft counted closed |

The seal is published before any tuning. No temperature, θ or few-shot example has been chosen from these windows.

### Counts

16 sessions, 8 per harness, give 277 windows: 142 in `validation`, the tuning half, and 135 in `test`, the sealed half. There is no `train` split. Every label comes from the driver's timeline (`script`).

| Label | Muse Code, validation / test | OpenCode, validation / test | Total |
|---|---:|---:|---:|
| A `IDLE_AT_PROMPT` | 22 / 25 | 22 / 23 | 92 |
| B `THINKING_STREAMING` | 1 / 2 | 0 / 0 | 3 |
| C `RUNNING_TOOL` | 6 / 4 | 3 / 3 | 16 |
| D `WAITING_APPROVAL` | 32 / 21 | 18 / 17 | 88 |
| E `ASKING_QUESTION` | 15 / 13 | 14 / 18 | 60 |
| F `ERROR_OR_EXITED` | 4 / 4 | 5 / 5 | 18 |
| G `UNKNOWN` | 0 / 0 | 0 / 0 | 0 |
| Total | 80 / 69 | 62 / 66 | 277 |

The build dropped 108 windows within the align margin, 30 with no label and 33 with only a weak label. There were no conflicts, and no session was skipped.

### Limits

- **Small n.** B has 3 windows, all from Muse Code. C has 16, 7 of them in `test`, and F has 18, 9 in `test`. Their metrics are reported with their n.
- **Readiness.** `gifford dataset inspect` reports 18 readiness faults. Eleven concern `train`, which v0 does not have. The other seven concern the evaluated halves:
  - `validation` holds 142 of 300 windows, and `test` 135 of 300;
  - in `test`, B has 2 of 20 windows, C 7 of 20 and F 9 of 20;
  - D and E come from 2 harnesses of the 3 asked for.

  None of them is presented as met.
- **Two harnesses.** v0 holds Muse Code and OpenCode only.
- **Scripted input.** Nobody typed into any session, so every draft counts as closed. v0 does not measure the Analyst while a person types.
- **Privacy review.** Before the seal, 23 windows were read on the recording machine to check the masking, and 9 of them were in `test`. They were read for that review only. They are not used to choose few-shot examples, features or thresholds. Automated counts over all 277 windows found no unmasked account, machine or domain name, home path, IPv4 address, email address or workspace name.
- **New child processes.** A replay does not observe the process tree, so `new_child_processes` is null in every window. Prompt v2 does not use it.

## Model evaluation

Not yet recorded.
