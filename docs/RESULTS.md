# Results

Each section names the command that produced its figures and the commit it ran on. Nothing in this file is an estimate.

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
- **Latency.** p95 is 2148 ms on `test`. The 1500 ms budget in the gate applies to an adapter, not to the base model.
- **The gate cannot pass with one profile.** `gifford eval` also runs the adapter gate. Criterion 0 fails, because the dataset's readiness gate failed with the 18 faults listed under Dataset v0. Criteria 1 to 5 are `PENDING`: 1, 3 and 4 need a second profile on `test`, and 2 and 5 are not implemented.
