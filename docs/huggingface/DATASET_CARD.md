---
tags:
- text-classification
- ai-agent-observability
task_categories:
- text-classification
language:
- en
license: PENDING (publication decision; see PUBLISHING.md)
---

# Analyst calibration dataset — dataset card (draft)

> Draft for publication. This card describes the *format* of the windows, the
> labels, and the build process, as specified in the public Gifford docs
> (`docs/ML_CONTRACT.md`, `docs/calibration-dataset.md`). No window, screen,
> recording, or session identifier is published with it. Counts marked
> **PENDING** are not yet published; published counts cite their source.

## What the dataset is

Windows cut from recorded coding-agent sessions, each pairing a projected
terminal screen with a few activity figures (the model input) and one state
label (the target). It calibrates and evaluates the `ANALYZE_AGENT_STATE`
model behind the Analyst: prompt versions, few-shot examples, temperature,
and the safe-to-deliver threshold are tuned and judged on it.

## Window format

Each window carries three parts. Only the first two reach the model:

1. **Model input** (`docs/ML_CONTRACT.md`, point 2) — exactly seven fields:
   the harness, seconds since last output, output byte counts over the last
   half second, two seconds, and ten seconds, the new child-process list
   (`null` for unknown), the previous state and its age, and the screen
   lines. The gateway refuses any other field.
2. **Screen** (point 4) — the rebuilt screen after a fixed projection, in
   order: human-input exclusion (a draft that is not known closed blanks
   the screen), cleaning, redaction, the domain mask, and the cut to the
   last configured rows of limited width. The prompt shows the rows between
   markers.
3. **Provenance, outside the model input** (point 5) — window and session
   identifiers, sequence ranges, times, the redaction policy version, and
   optional sealed label evidence. A structural canary plus an equality
   test over the real inference payload pin this boundary: no envelope
   field enters the prompt.

The dataset manifest seals digests of the manifest itself, the split file,
the align configuration, and the build settings. A rebuild from the same
inputs must give the same digest, or the seal is broken.

## Labels

One of seven letters per window (names from `docs/ML_CONTRACT.md`,
definitions from `docs/calibration-dataset.md`):

| Letter | State | Definition |
|---|---|---|
| A | `IDLE_AT_PROMPT` | Input line empty and ready; nothing running, streaming, or waiting. |
| B | `THINKING_STREAMING` | The model produces text or reasoning; no tool runs. |
| C | `RUNNING_TOOL` | An agent-started tool or command runs. |
| D | `WAITING_APPROVAL` | A permission or approval menu waits for a human choice. |
| E | `ASKING_QUESTION` | The agent's last turn asks the human a question and waits. |
| F | `ERROR_OR_EXITED` | The agent stopped on an error, or the harness exited. |
| G | `UNKNOWN` | The screen does not determine the state. |

**Label source** — first match wins (`docs/ML_CONTRACT.md`, point 7;
`docs/calibration-dataset.md`, "Labels"):

- a **manual** annotation beats every other source;
- a **script** label from the driver's timeline (the state seen on screen,
  or no known state), agreed with a hook label where both exist;
- a **hook** label, only for harnesses whose hooks are read, and only
  inside a proven session for the recording's exact capture, harness, and
  version;
- **weak** labels from activity never enter validation or test, and enter
  train only when explicitly kept.

A script label and a hook label that disagree give no label: the window
goes to a person as a conflict. Windows with no label are dropped and
counted. Durable native labels are off by default and can only produce the
middle letters. Synthetic windows (train only, waiting and error states
only, capped per label) carry synthetic provenance on both the quality and
the target side.

## Harnesses

Sessions are recorded per harness under the harness's measured profile
version, with self-update off (`docs/calibration-dataset.md`). The first
published dataset version holds two harnesses (`docs/RESULTS.md`); hook
labels default to one of them. Permission-dialog matchers exist as measured
layouts for specific recorded versions (`docs/ML_CONTRACT.md`, permission
section); one harness family is deliberately absent from them.

Exact harness names and versions per dataset version: **PENDING** beyond
what the published reports already name.

## `source_quality`

The documented value is `synthetic`, carried together with a synthetic
target marker by windows added from a synthetic directory
(`docs/ML_CONTRACT.md`, point 9). The exact quality vocabulary for captured
windows is **PENDING** (not fixed in the public docs).

## Splits

By session: no session ever has windows in two splits. The standard rule
hashes a group digest into train, validation, and test shares; a split
file can replace it, listing every session exactly once
(`docs/ML_CONTRACT.md`, point 9). The manifest seals the split file's
digest, and the build writes back the exact bytes it read, with restricted
permissions, next to the manifest.

Evaluation partitions (`docs/ML_CONTRACT.md`, point 10): a fixed
calibration session list (with its complement for checkpoint comparison)
and a sealed blind-test session list. Final evaluation computes its
criteria only on the blind sessions; historical or aggregate results never
compensate a blind failure.

The first published dataset version has no train split: a tuning half and
a sealed half (`docs/calibration-dataset.md`, "Split and seal").

## Published counts (dataset version zero)

Source: `docs/RESULTS.md`. 16 sessions, 8 per harness, give 277 windows:
142 in the tuning half, 135 in the sealed half. Every label comes from the
driver's timeline. The build dropped windows inside the align margin,
windows with no label, and windows with only a weak label; there were no
conflicts and no skipped session. The seal table (manifest digest, dataset
digest, split-file digest, align margin and config digest, scripted-input
flag) is published there. Counts for any later dataset version: **PENDING.**

## Redaction and privacy process

- Sessions are recorded under a dedicated account used for nothing else;
  its home, configuration, and credential directories are owner-only, and
  every file the procedure creates is owner-only from birth
  (`docs/calibration-dataset.md`, "The recording account").
- API keys live only in the shell that starts their harness, never in
  print, arguments, or copies.
- The screen projection redacts secrets (keys, credentials, tokens,
  secret-named values, sensitive paths) under a versioned policy, then
  masks machine, account, and domain names from a mask file generated on
  the recording machine, which never leaves it. The mask also covers
  workspace names, since a name could leak the session kind.
- Scripted builds count every draft as closed only for sessions nobody
  typed into; a session any person typed into never enters such a build.
- Before the seal, a sample of windows is read on the recording machine to
  check masking; only the counts of that review leave the machine. After
  the seal, nobody opens the sealed windows or their predictions while
  tuning.
- Recordings, windows, screens, masks, session scripts, and few-shot
  examples never leave the recording machine. Only metrics and counts are
  published (`docs/calibration-dataset.md`, header).

A privacy canary (text typed before a modal) may remain only in the
unchanged private raw recording; observations, payloads, datasets, events,
logs, and errors contain neither that text nor any modal row
(`docs/ML_CONTRACT.md`, permission section).
