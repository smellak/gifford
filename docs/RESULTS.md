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
- **Not the live screen.** Live, the Analyst only gets a screen in which the measured profile finds the input box exactly once, on the cursor's row, with the input guard closed and the box empty; every other screen is withheld and not analysed. In the screens it does get, the box's editable rows read `[input]`, and its borders stay. v0 windows come from the replayed screen: they show the box as the harness drew it, and they include every labelled window, whether or not a live profile would find the box in it. The row cut and the name mask are the same in both, since `gifford dataset build` refuses a `--screen-lines` or `--mask` that differs from the Analyst's `ml.screen_lines` and `ml.mask`. The figures below measure the model on the v0 windows, not on the live form of the screen.
- **Privacy review.** Before the seal, 23 windows were read on the recording machine to check the masking, and 9 of them were in `test`. They were read for that review only. They are not used to choose few-shot examples, features or thresholds. Automated counts over all 277 windows found no unmasked account, machine or domain name, home path, IPv4 address, email address or workspace name.
- **New child processes.** A replay does not observe the process tree, so `new_child_processes` is null in every window. Prompt v2 does not use it.

## Model evaluation

The base model with prompt v1, on dataset v0. No adapter was trained, so every figure here is the base model's.

The predictions were made with `main` at `8366793`, against an endpoint deployed from `main` at `702e7cf`: `meta-models/Muse-Glimmer-30B` at revision `cec94fcfd6512f2cfe708138c32533b346ea4f69`, served by vLLM 0.28.0 in FP8 on one L40S GPU. All 277 windows got a valid answer.

```sh
gifford eval predict --profile base --manifest DIR/manifest.json --windows DIR/windows \
  --gateway FILE --out base.jsonl
gifford eval --profile base=base.jsonl --manifest DIR/manifest.json --out FILE
```

| | |
|---|---|
| predictions file | `sha256:7db6718406c9ae59fc123231c315ab90a4b7724cebb78d0757d5058ba8fdb4ce`, 277 lines |
| temperature T, fitted on `validation` | 1.135028 |
| θ on calibrated p(A), set on `validation` | 0.639435 |

### Sealed half: `test`, 135 windows

| Metric | Value |
|---|---:|
| accuracy | 0.6741 |
| macro-F1, over A to F, the labels with support | 0.5495 |
| false IDLE: a window that is not A, predicted A | 0.3908 (34/87) |
| false SAFE, model only: calibrated p(A) at least θ on a window that is not A | 0.0230 (2/87) |
| ECE, before / after calibration | 0.1188 / 0.1266 |
| Brier, before / after calibration | 0.4529 / 0.4598 |
| UNKNOWN | 0.0296 (4/135) |
| latency p50 / p95 | 1286 / 2148 ms |

| State | Support | Recall |
|---|---:|---:|
| A `IDLE_AT_PROMPT` | 48 | 0.9375 |
| B `THINKING_STREAMING` | 2 | 0.5000 |
| C `RUNNING_TOOL` | 7 | 0.4286 |
| D `WAITING_APPROVAL` | 38 | 0.6579 |
| E `ASKING_QUESTION` | 31 | 0.5484 |
| F `ERROR_OR_EXITED` | 9 | 0.0000 |
| G `UNKNOWN` | 0 | n/a |

Confusion matrix, rows the truth and columns the prediction:

| | A | B | C | D | E | F | G |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 45 | 0 | 0 | 0 | 3 | 0 | 0 |
| B | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| C | 4 | 0 | 3 | 0 | 0 | 0 | 0 |
| D | 8 | 0 | 3 | 25 | 0 | 0 | 2 |
| E | 14 | 0 | 0 | 0 | 17 | 0 | 0 |
| F | 7 | 0 | 0 | 0 | 0 | 0 | 2 |
| G | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

| Harness | n | accuracy | macro-F1 | recall D | recall E | false IDLE | false SAFE | ECE after | p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Muse Code 1.4.13-R6996.1 | 69 | 0.5942 | 0.4657 | 0.3810 | 0.4615 | 0.5000 (22/44) | 0.0227 (1/44) | 0.1021 | 2058 |
| OpenCode 1.18.29 | 66 | 0.7576 | 0.6430 | 1.0000 | 0.6111 | 0.2791 (12/43) | 0.0233 (1/43) | 0.2090 | 2163 |

### Tuning half: `validation`, 142 windows

| Metric | Value |
|---|---:|
| accuracy | 0.6620 |
| macro-F1 | 0.6500 |
| false IDLE | 0.3980 (39/98) |
| false SAFE, model only | 0.0000 (0/98) |
| ECE, before / after calibration | 0.1386 / 0.1379 |
| Brier, before / after calibration | 0.4647 / 0.4720 |
| UNKNOWN | 0.0423 (6/142) |
| latency p50 / p95 | 1274 / 1919 ms |

### What the figures say

- **θ does not hold on the sealed half.** θ was set on `validation` for a false SAFE of at most 0.5 %, and gives none in 98 windows there. On `test` it gives 2 in 87, which is 2.3 %. No count this small can show a rate of 0.5 %: 0 in 98 is still consistent with a rate of about 3 %.
- **Temperature scaling does not help.** On `test`, T = 1.135 raises ECE from 0.1188 to 0.1266 and Brier from 0.4529 to 0.4598. On `validation`, ECE barely moves and Brier rises.
- **The dominant error is predicting A.** On `test`, 34 of the 87 windows that are not A are predicted A: 14 of 31 E, 8 of 38 D, 7 of 9 F, 4 of 7 C and 1 of 2 B.
- **Muse Code D.** Recall of D on `test` is 0.38 on Muse Code and 1.00 on OpenCode. The 21 Muse Code D windows in `test` all come from one session, run without `--disable-sandbox`. The 32 in `validation` come from the two sessions run with it. Both variants show the same staged approval menu.
- **Latency.** p95 is 2148 ms on `test`, with predict running 4 calls at a time. That is above the Analyst's reference budget of 1.5 s p95, which the gate checks only for a profile it would activate.
- **The gate cannot pass with one profile.** `gifford eval` also runs the adapter gate. Criterion 0 fails, because the dataset's readiness gate failed with the 18 faults listed under Dataset v0. Criteria 1 to 5 are `PENDING`: 1, 3 and 4 need a second profile on `test`, and 2 and 5 are not implemented.

## Prompt candidate evaluation

Prompt v2 of `ANALYZE_AGENT_STATE` against prompt v1, both on the base model and on dataset v0: v1 is the baseline and v2 the candidate. The gate fails on criteria 3 and 4.

v2 reads the same input as v1 and answers with the same letters. It adds a features line to the user message, and to the system message the rules that read it, of which the first that holds applies: an approval menu is D; a working indicator is C with a tool running, and B otherwise; an error the agent does not get past is F; a question to the user is E; and with none of these and the output quiet, A. The features come from fixed patterns over each harness's interface text. The patterns and their tests went in as their own commit, `81e166a`, before any v2 prediction.

The predictions and the report were made with a binary built from `main` at `bd56a22`, with `vcs.modified=false`, against an endpoint deployed from the same commit: `meta-models/Muse-Glimmer-30B` at revision `cec94fcfd6512f2cfe708138c32533b346ea4f69`, served by vLLM 0.28.0 in FP8 on one L40S GPU, the same model and serving stack as under Model evaluation. The four runs went one after another, each with one call at a time, and every call got a valid answer.

```sh
gifford eval predict --profile base --prompt-version analyze_agent_state.v1 --concurrency 1 \
  --manifest DIR/manifest.json --windows DIR/windows --gateway FILE --out v1.jsonl
gifford eval predict --profile base --prompt-version analyze_agent_state.v2 --concurrency 1 \
  --manifest DIR/manifest.json --windows DIR/windows --gateway FILE --out v2.jsonl
gifford eval predict --profile base --prompt-version analyze_agent_state.v1 --concurrency 1 \
  --manifest internal/eval/testdata/safety/manifest.json \
  --windows internal/eval/testdata/safety/windows --gateway FILE --out safety-v1.jsonl
gifford eval predict --profile base --prompt-version analyze_agent_state.v2 --concurrency 1 \
  --manifest internal/eval/testdata/safety/manifest.json \
  --windows internal/eval/testdata/safety/windows --gateway FILE --out safety-v2.jsonl
gifford eval --profile base=v1.jsonl --candidate-prompt analyze_agent_state.v2=v2.jsonl \
  --safety-predictions baseline=safety-v1.jsonl --safety-predictions candidate=safety-v2.jsonl \
  --manifest DIR/manifest.json --safety-manifest internal/eval/testdata/safety/manifest.json \
  --out FILE
```

| Run | Windows | Start | End | Valid | Predictions file |
|---|---|---|---|---:|---|
| v1 | dataset v0 | 2026-10-31T23:58:15Z | 2026-11-01T00:05:52Z | 277/277 | `sha256:22f09df27f679d8f730ef576b0d53d3b08d6ccabd5c3d73918360b00a91d8902` |
| v2 | dataset v0 | 2026-11-01T00:06:22Z | 2026-11-01T00:12:55Z | 277/277 | `sha256:eaed0e160e91ee6566ebe6a56fd2a601d42985e5f52ef65af7751632d45912ef` |
| safety, v1 | safety suite | 2026-11-01T00:13:24Z | 2026-11-01T00:13:58Z | 24/24 | `sha256:c3981340e53757d2370af999b9237275d8cb0a1f72cfc1ce89c5db4014e19824` |
| safety, v2 | safety suite | 2026-11-01T00:14:08Z | 2026-11-01T00:14:41Z | 24/24 | `sha256:6be2fd9ca3aa55b78cb92424fbbbecba207485b7d102f2b57d7f0ae6e1a9dc61` |

| | Baseline, v1 | Candidate, v2 |
|---|---:|---:|
| temperature T, fitted on `validation` | 1.149759 | 1.125887 |
| θ on calibrated p(A), set on `validation` | 0.652115 | 0.708397 |

### Gate

| # | criterion | baseline | candidate | rule | verdict |
|---:|---|---|---|---|---|
| 0 | dataset preparation for training | N/A | N/A | no aplica: sin entrenamiento | N/A |
| 1 | macro-F1 and false IDLE improve, calibrated ECE no worse | macro-F1 0.5604; false IDLE 0.2989 (26/87); ECE 0.1744 | macro-F1 0.7587; false IDLE 0.1149 (10/87); ECE 0.1732 | macro-F1 rises; false IDLE falls (or both zero); ECE does not rise | PASS |
| 2 | false SAFE after guards does not rise | model-safe 0.0115 (1/87); blocked 1; residual 0.0000 (0/87); unavailable output 0, child/tool 87 | model-safe 0.0000 (0/87); blocked 0; residual 0.0000 (0/87); unavailable output 0, child/tool 87 | residual false SAFE does not rise; unavailable guards PASS | PASS |
| 3 | recall of D and of E not lower | D 0.7632; E 0.4839 | D 0.7368; E 0.8065 | recall D and E do not fall | FAIL |
| 4 | candidate latency p95 within budget | p95 2382.0 ms | p95 1892.0 ms; delta -490.0 ms | candidate p95 <= 1500.0 ms | FAIL |
| 5 | safety regression suite | failures 0/24; worst p(A) 0.2872; theta 0.6521 | failures 0/24; worst p(A) 0.3247; theta 0.7084 | candidate: all fixtures present and valid; p(A) < own theta; baseline reported | PASS |

The active profile stays the base model with prompt v1 and the calibration under Model evaluation, T = 1.135028 and θ = 0.639435. The baseline's T = 1.149759 and θ = 0.652115 were fitted in this run, on `validation`, so that v1 and v2 are compared like with like. They do not change the calibration in use.

### Sealed half: `test`, 135 windows

| Metric | v1 | v2 |
|---|---:|---:|
| accuracy | 0.6889 | 0.8148 |
| macro-F1, over A to F, the labels with support | 0.5604 | 0.7587 |
| false IDLE: a window that is not A, predicted A | 0.2989 (26/87) | 0.1149 (10/87) |
| false SAFE, model only: calibrated p(A) at least θ on a window that is not A | 0.0115 (1/87) | 0.0000 (0/87) |
| ECE, before / after calibration | 0.1249 / 0.1744 | 0.1501 / 0.1732 |
| UNKNOWN | 0.0741 (10/135) | 0.0296 (4/135) |
| latency p50 / p95 | 1607 / 2382 ms | 1366 / 1892 ms |

| State | Support | Recall, v1 | Recall, v2 |
|---|---:|---:|---:|
| A `IDLE_AT_PROMPT` | 48 | 0.9375 | 0.9375 |
| B `THINKING_STREAMING` | 2 | 0.5000 | 1.0000 |
| C `RUNNING_TOOL` | 7 | 0.4286 | 0.8571 |
| D `WAITING_APPROVAL` | 38 | 0.7632 | 0.7368 |
| E `ASKING_QUESTION` | 31 | 0.4839 | 0.8065 |
| F `ERROR_OR_EXITED` | 9 | 0.0000 | 0.4444 |
| G `UNKNOWN` | 0 | n/a | n/a |

### Tuning half: `validation`, 142 windows

| Metric | v1 | v2 |
|---|---:|---:|
| latency p50 / p95 | 1567 / 2293 ms | 1332 / 1895 ms |

### Output threshold, from `validation`

v2's `output` feature says `streaming` when the window had output bytes in its last 0.5 s, and `quiet` otherwise. The threshold was set from these counts on `validation`, before any v2 prediction and never from `test`: for each harness and label, the windows with output bytes in their last 0.5 s, 2 s and 10 s.

| Harness | Label | Windows | Last 0.5 s | Last 2 s | Last 10 s |
|---|---|---:|---:|---:|---:|
| Muse Code | A | 22 | 1 | 7 | 22 |
| Muse Code | B | 1 | 1 | 1 | 1 |
| Muse Code | C | 6 | 6 | 6 | 6 |
| Muse Code | D | 32 | 32 | 32 | 32 |
| Muse Code | E | 15 | 0 | 3 | 15 |
| Muse Code | F | 4 | 0 | 1 | 4 |
| OpenCode | A | 22 | 0 | 0 | 9 |
| OpenCode | C | 3 | 3 | 3 | 3 |
| OpenCode | D | 18 | 0 | 1 | 6 |
| OpenCode | E | 14 | 0 | 0 | 3 |
| OpenCode | F | 5 | 0 | 0 | 2 |

The rules give A only with the output quiet. At rest, Muse Code still writes: all 22 of its A windows had bytes in their last 10 s, and 7 in their last 2 s. With a 2 s threshold those 7 would not be quiet; with 0.5 s, 1 is not.

### Limits

- **Criterion 3 fails by one window.** On `test`, v2 gets 28 of the 38 D windows right and v1 gets 29. All 10 that v2 misses are Muse Code windows, and it reads them as C; on OpenCode, recall of D is 1.0000 with both prompts. On Muse Code it goes from 0.5714 to 0.5238. The 21 Muse Code D windows in `test` all come from one session.
- **A new v1 run does not give the figures under Model evaluation.** On the same 135 `test` windows, D is 0.7632 against 0.6579, E 0.4839 against 0.5484, false IDLE 26/87 against 34/87, macro-F1 0.5604 against 0.5495, and p95 2382 against 2148 ms. Three things changed between the two runs: the binary, from `main` at `8366793` to `bd56a22`; the endpoint's deployment, from `702e7cf` to `bd56a22`; and the concurrency, from 4 to 1. The cause has not been determined. The gate is applied as it was registered, and no run is repeated.
- **Both versions are over the latency budget.** With one call at a time, p95 is 2382 ms for v1 and 1892 ms for v2 on `test`, and 2293 and 1895 ms on `validation`. The budget is 1.5 s.
- **Temperature scaling makes calibration worse.** With both prompts, the T fitted on `validation` raises ECE and Brier, on `test` and on `validation`. On `test`, ECE goes from 0.1249 to 0.1744 and Brier from 0.4403 to 0.4482 for v1; for v2, from 0.1501 to 0.1732 and from 0.3026 to 0.3183. Criterion 1 compares ECE after calibration; before it, v2's is the higher.
- **All the false IDLE left is OpenCode's.** On `test`, v2 predicts A for 10 of the 43 OpenCode windows that are not A, 6 E and 4 F, and for none of the 44 on Muse Code. On `validation`, it does so for 11 of 40 on OpenCode and none of 58 on Muse Code.
- **Two thresholds for quiet output.** The guard replay counts output as quiet after 300 ms, the writer's shared threshold. v2's `output` feature counts it as quiet after 0.5 s, the threshold set above from `validation`.
- **previous_state is empty in v0.** Every v0 window has an empty previous_state, which the prompt shows as `none`. Live, the Analyst fills it in. This affects v1 and v2 alike.
- **The rule order comes from the labels' definition.** v2's rules follow the order in which the driver's code gives the labels. That is right, because that code is the definition of the labels. But it brings the features even closer to what labelled the windows, so v2 may do better on v0 than on other versions of the harnesses.
- **What the live Analyst sees.** As Not the live screen under Dataset v0 says, the live Analyst only gets a screen in which the profile finds the input box on the cursor's row, and these figures count every v0 window. A question above an empty prompt does reach it, so recall of E is given on its own. v2's system message describes `[input]`, the covered input box, which no v0 window shows, so this evaluation does not measure it.
