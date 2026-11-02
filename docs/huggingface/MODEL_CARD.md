---
license: apache-2.0
base_model: PENDING (bound at training time; see "Base model" below)
tags:
- text-classification
- lora
- ai-agent-observability
pipeline_tag: text-classification
---

# Analyst LoRA over Muse Glimmer — model card (draft)

> Draft for publication. Figures marked **PENDING** await the adapter gate
> report. Nothing here trains, serves, or publishes a model: it describes the
> adapter the Analyst would use, as specified in the public Gifford docs.

## What it does

The adapter serves the `ANALYZE_AGENT_STATE` contract: a projected terminal
screen plus a few activity figures in, one of seven state letters out. The
Analyst calls it to know what a coding agent is doing right now, so the
orchestrator can decide safely (for example, whether delivering a message
would talk over a permission menu or an open question).

The seven labels, exactly as `docs/ML_CONTRACT.md` defines them:

| Letter | State (contract name) | Meaning |
|---|---|---|
| A | `IDLE_AT_PROMPT` | The input line is empty and ready. Nothing runs or streams; no question or menu waits. |
| B | `THINKING_STREAMING` | The model is producing text or reasoning; no tool runs. |
| C | `RUNNING_TOOL` | An agent-started tool or command is running. |
| D | `WAITING_APPROVAL` | A permission or approval menu is on screen, waiting for a human choice. |
| E | `ASKING_QUESTION` | The agent's last turn asks the human a question and waits for the answer. |
| F | `ERROR_OR_EXITED` | The agent stopped on an error it cannot pass alone, or the harness exited. |
| G | `UNKNOWN` | The screen does not determine the state (splash screen, mid-redraw frame). |

The answer format is one letter read from the first answer token's
log-probabilities, then `|` and at most three short evidence phrases. When the
seven letters together hold less than half of that token's probability, the
answer is `UNKNOWN`. Details: point 3 of `docs/ML_CONTRACT.md`.

## Base model

PENDING: the exact base checkpoint and revision the adapter is trained
against are bound when training is sealed. For reference, the public
evaluation of the base model on dataset version zero used the checkpoint
named in `docs/RESULTS.md`; that report measures the base model only, not
this adapter.

## How it is served

As `ml/serve/README.md` describes: a vLLM server behind a thin endpoint,
reached through the Gifford gateway:

- The endpoint serves one set of base weights, plus the Analyst LoRA when
  the deployment configuration carries an adapter table (name, revision,
  module, path). The adapter runs in text-only mode.
- The gateway selects the `base` or the `analyst-lora` inference profile
  per call. A deployment without an adapter answers every `analyst-lora`
  request with an error; the base profile keeps working.
- Calls carry temperature zero, a small token budget, and log-probability
  output so the label is read from the first token.
- The endpoint never logs payloads, prompts, screens, or answers.
- A request naming an unknown prompt version, or malformed output from the
  model, is answered with a fixed error, never with a guessed label.

Weights, adapters, data, and secrets never enter the Gifford repository.
Deployment follows the runbook in `ml/serve/README.md`.

## Intended use

- Classifying the live state of a coding agent (letters A to G) to inform
  orchestration decisions, such as deferring a delivery while the human is
  deciding.
- Research on agent observability: comparing prompts and calibrations on
  the same frozen dataset.

## Out-of-scope uses

- Recognising a permission screen never approves it and is not evidence
  for any delivery guard. The model does not act; it only labels.
- The label is not an approval signal, not a safety clearance, and not a
  substitute for the Proxy's own human-priority guards.
- Not for screens outside the projection it was trained on (redacted,
  masked, cut to the configured row count), and not for harnesses whose
  version was never measured.

## Known limitations (from the public docs)

- Until the adapter gate report is published, every public figure
  measures the base model only.
- The calibration dataset's first version is scripted: a driver typed every
  key, nobody typed by hand, so it does not measure the Analyst while a
  person types (`docs/calibration-dataset.md`).
- In dataset windows the child-process list is always empty (unknown) and
  the previous-state field is empty; serving fills the latter from the
  stored session state (`docs/ML_CONTRACT.md`, point 2).
- That dataset version was sealed under redaction policy 1; the current
  policy is 2, so rebuilding it changes its digest
  (`docs/calibration-dataset.md`, "Limits").
- The serving stack wraps the vision tower for LoRA as well, but the
  adapter only loads in text-only mode (`ml/serve/README.md`).

## Evaluation

The adapter gate (`docs/ML_CONTRACT.md`, point 10) passes only if all six
criteria pass on blind test sessions. Each criterion is **PENDING** until
the gate report for this adapter is published:

- **Criterion 0 — dataset readiness:** the dataset manifest passed its
  readiness gate. **PENDING.**
- **Criterion 1 — quality vs base:** on blind test, macro-F1 up, false IDLE
  down, calibrated ECE no worse than the base model. **PENDING.**
- **Criterion 2 — false SAFE after guards does not rise.** **PENDING.**
- **Criterion 3 — recall of waiting states:** recall of D and of E not
  lower. **PENDING.**
- **Criterion 4 — latency:** adapter latency p95 within budget, 1.5 s by
  default. **PENDING.**
- **Criterion 5 — safety regression suite:** every fixture accounted for,
  no invalid or error result, calibrated probability of A below the sealed
  threshold. **PENDING.**

Base-model figures on the frozen dataset are published separately in
`docs/RESULTS.md` and are not adapter results.
