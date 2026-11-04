---
license: apache-2.0
base_model: meta-models/Muse-Glimmer-30B
base_model_revision: cec94fcfd6512f2cfe708138c32533b346ea4f69
tags:
- peft
- lora
- text-classification
- coding-agents
- human-in-the-loop
pipeline_tag: text-classification
---

# glimmer-analyst-v2

`glimmer-analyst-v2` is a task-specific LoRA for the Gifford
`ANALYZE_AGENT_STATE` contract. It classifies a bounded, redacted terminal
projection as one of seven coding-agent states. It advises Gifford's
orchestrator; it cannot approve, write to a terminal or bypass a deterministic
guard.

## Immutable identity

| Field | Value |
|---|---|
| Adapter revision | `l56-s1-paso-2055` |
| Adapter SHA-256 | `dd28393cf156cd62290b31f24bea795c9d4eed38832c88a6330d0863a10a4c34` |
| Training run | `run-l56-s1-1102-01` |
| Dataset | `S1` |
| Dataset SHA-256 | `ea3f858e509f650518783e66171d7ef0aaf19804d107dd057c2b44dcf6080929` |
| Dataset manifest SHA-256 | `7007bf48c15f4481846a1ad92eb4672f3f44ec99bc0434fc37cdc122139d3ca4` |
| Base model | [`meta-models/Muse-Glimmer-30B`](https://huggingface.co/meta-models/Muse-Glimmer-30B) |
| Base revision | `cec94fcfd6512f2cfe708138c32533b346ea4f69` |
| Prompt | `analyze_agent_state.v1` |
| Training code revision | `31cf826` |

## Labels

| Letter | Contract state |
|:---:|---|
| A | `IDLE_AT_PROMPT` |
| B | `THINKING_STREAMING` |
| C | `RUNNING_TOOL` |
| D | `WAITING_APPROVAL` |
| E | `ASKING_QUESTION` |
| F | `ERROR_OR_EXITED` |
| G | `UNKNOWN` |

The input and output schemas, projection, redaction and calibration boundary
are frozen in [Gifford's ML contract](https://github.com/smellak/gifford/blob/main/docs/ML_CONTRACT.md).

## Training

LoRA training completed on one H100 for 2,055 steps, with gradient
accumulation 4, learning rate 0.0001, rank 16, alpha 32, evaluation every 500
steps, 208 adapted modules, seed 0 and no weighted sampling. Training began at
2026-11-02 15:48:23 UTC and ended at 16:59:58 UTC. The recorded dataset
readiness status was `underpowered`.

The normalised command and every public seal are recorded in
[RESULTS.md](https://github.com/smellak/gifford/blob/main/docs/RESULTS.md#lora-comparison-result). Adapter tensors were
produced and remain on the training/serving volume; they were never committed
to the Gifford repository.

## Evaluation

**89.3% on the comparison set of 711 windows; coverage: claude-code,
opencode, antigravity** (635 exact argmax matches). The predictions file has
SHA-256
`ba4ec202b3a02abfa0db7e89c97e2494ebff5ba998032283709f12aedc79d07f`.

This is a comparison/validation result, not a blind release gate, safety rate
or autonomous-delivery rate. The dataset was underpowered and the product's
six-criterion adapter gate did not pass. The runtime activation used the
bounded operator-override path and records `gate_status=override`. See the
[lifecycle table](https://github.com/smellak/gifford/blob/main/docs/ML_CONTRACT.md#adapter-identity-and-lifecycle).

## Intended use

- Classify projected screens from the measured harness/version domain to help
  Gifford decide whether to defer a pending notice.
- Compare a sealed adapter with the pinned base model under the same contract.

## Out-of-scope use

- Authorising a terminal write, granting an approval or replacing Gifford's
  deterministic detector and input guards.
- Treating a label as proof that a human is absent or a harness is safe.
- Screens outside the documented projection, unmeasured harness versions or
  general-purpose image/text classification.

## Limitations

- The 711-window comparison covers only Claude Code, OpenCode and Antigravity;
  it does not establish accuracy on Codex or Muse Code.
- No blind test split met the release-readiness requirement. Deployment must
  retain deterministic corroboration, breaker fallback and human controls.
- Training data reflects exact recorded harness versions and terminal layouts;
  UI updates can invalidate the evidence.
- The model can be confidently wrong. Agreement with Gifford's detector is not
  accuracy because the detector is not ground truth.

## License and redistribution

The adapter is published under Apache-2.0, matching the pinned base model's
[official license](https://huggingface.co/meta-models/Muse-Glimmer-30B/blob/cec94fcfd6512f2cfe708138c32533b346ea4f69/LICENSE).
Redistributors must preserve the Apache-2.0 license and notices and comply with
the base model's published usage policy. The base weights are not bundled with
this adapter. The training dataset is a separate, card-only artifact with
separate redistribution limits documented at
[`smellak/gifford-screens-s1`](https://huggingface.co/datasets/smellak/gifford-screens-s1)
and mirrored in [DATASET_CARD.md](DATASET_CARD.md).
