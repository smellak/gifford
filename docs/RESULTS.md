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

Not yet recorded.
