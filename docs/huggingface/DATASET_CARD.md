---
license: other
license_name: card-only-no-dataset-redistribution
tags:
- text-classification
- coding-agents
- terminal-state
task_categories:
- text-classification
language:
- en
---

# Gifford Analyst dataset S1

This card documents the sealed private dataset used to train
[`glimmer-analyst-v2`](https://huggingface.co/smellak/gifford-glimmer-analyst-v2).
It does **not** publish dataset rows, terminal windows, recordings or session
identifiers.

## Identity and seals

| Field | Value |
|---|---|
| Dataset version | `S1` |
| Dataset SHA-256 | `ea3f858e509f650518783e66171d7ef0aaf19804d107dd057c2b44dcf6080929` |
| Manifest SHA-256 | `7007bf48c15f4481846a1ad92eb4672f3f44ec99bc0434fc37cdc122139d3ca4` |
| Split sidecar SHA-256 | `49170a234c3ca6af6c0a8c6402ce7f97c062c7b1d666643cc7db8ee2b77f75eb` |
| Dataset builder SHA-256 | `2181ff01d096c8d0556b14639418f6015aabab9e99c382fc7474249f9793df08` |
| Builder code revision | `31cf826` |
| Contract | `ANALYZE_AGENT_STATE` |
| Window schema | 1 |
| Projection form | `live-input-v2` |
| Redaction policy | 2 |
| Human-input policy | `human-input-v1` |
| Activity policy | `effective-read-v2` |

S1 contains 9,386 windows: 8,218 train and 1,168 validation, with no blind
test split. Label counts are A 5,116; B 503; C 754; D 476; E 1,349; F 629;
G 559. The readiness gate therefore failed and the dataset is recorded as
`underpowered`.

## Coverage

The sealed manifest names these exact harness versions:

- Claude Code `2.1.280`;
- OpenCode `1.18.29`;
- Antigravity `1.4.8`;
- Muse Code `1.4.13-R6996.1` in validation only.

The published 711-window adapter comparison is a subset covering Claude Code,
OpenCode and Antigravity; its result and prediction hash are in
[RESULTS.md](https://github.com/smellak/gifford/blob/main/docs/RESULTS.md#lora-comparison-result).

## Window and label contract

Each row contains the model input (harness, activity figures, previous state
and a bounded screen projection), one A–G target, and provenance that never
enters the model prompt. Projection applies human-input exclusion, cleaning,
redaction, a domain mask and a fixed row/width cut in that order. The full
schema and label precedence are specified in
[ML_CONTRACT.md](https://github.com/smellak/gifford/blob/main/docs/ML_CONTRACT.md).

## Training linkage

Training run `run-l56-s1-1102-01` used LoRA for 2,055 steps, accumulation 4,
learning rate 0.0001, rank 16, alpha 32, evaluation every 500 steps, 208
modules, seed 0 and no weighted sampling. It produced adapter revision
`l56-s1-paso-2055`, SHA-256
`dd28393cf156cd62290b31f24bea795c9d4eed38832c88a6330d0863a10a4c34`.

## Privacy, license and redistribution limits

- Raw recordings, captures, unprojected screens, mask files, workspace names,
  timelines, native-event sidecars and private deployment configuration are
  never published.
- Projected windows and row-level provenance are not authorised for public
  redistribution. This Hugging Face artifact is a card only; it grants no
  dataset license and contains no dataset payload.
- The public build and evaluation code is Apache-2.0. That code license does
  not license private captures or projected windows.
- Any later proposal to publish rows requires a new privacy review, a data
  license decision and confirmation that every contributing harness's terms
  allow redistribution. Until then the only public evidence is aggregate
  counts, cryptographic seals and evaluation metrics.
