# ML contract

The Analyst's model contract is `ANALYZE_AGENT_STATE`: a projected terminal
screen and a few activity figures in, one of seven state letters out. Ten points
fix what a trained adapter depends on; changing any of them means rebuilding the
dataset and training again. This page states each point as `main` implements
it, with the code that does so.

Status on this commit:

- The freeze is not tagged: `git tag -l` lists no `ml-contract-v1`.
- No adapter exists ([RESULTS.md](RESULTS.md)). Only `ANALYZE_AGENT_STATE`
  could use one: the `analyst-lora` profile is refused for any other contract,
  so `TRIAGE_MESSAGE` and `CHOOSE_ORCHESTRATION_ACTION` always run on the base
  model (`internal/protocol/gateway.go`).
- The Analyst is optional. It runs only when `config.yaml` has a
  `state_analysis` section and `ml.screen_lines` and `ml.mask`; `gifford init`
  writes none of them.

## The ten points

1. **States A–G.** A `IDLE_AT_PROMPT`, B `THINKING_STREAMING`, C
   `RUNNING_TOOL`, D `WAITING_APPROVAL`, E `ASKING_QUESTION`, F
   `ERROR_OR_EXITED`, G `UNKNOWN`. `BACKGROUND_WORK` is set only from harness
   events and is never a model label. Code: `modelLabels` in
   `internal/analyst/prompt.go`, `prompts/analyze_agent_state.v1.system.txt`,
   `internal/dataset/window.go`.

2. **Input schema.** Seven fields: `harness`, `seconds_since_last_output`,
   `output_bytes` over the last 0.5 s, 2 s and 10 s, `new_child_processes`
   (`null` for unknown, `[]` for none, or at most 8 sanitised names),
   `previous_state` and `previous_state_age_seconds`, and `screen_lines`. The
   gateway refuses any other field, and the prompt template is
   `analyze_agent_state.v1`. The 0.5 s count is `last_500ms` on the wire and
   `last_0_5s` in a dataset window. Harness version and source quality are
   recorded with each window but are not model input. Two gaps remain on this
   commit, and the replay code states both: the dataset build leaves
   `previous_state` empty in every window (the prompt reads `none`), while the
   Analyst fills it from the last state stored for that agent's session under
   the `base` profile, and leaves it empty under `analyst-lora`; and
   neither path fills the child-process list, so `new_child_processes` is
   `null` and the prompt always reads `unknown`. Code:
   `internal/analyst/analyst.go`, `internal/gateway/sanitize.go`,
   `internal/dataset/replay/replay.go`,
   `internal/orchestrator/state_producer.go`.

3. **Output schema.** One letter, read from the log-probabilities of the first
   answer token, then `|` and at most 3 evidence phrases of 1–120 characters
   separated by `;`. The probabilities over the seven labels sum to 1. If the
   letters hold less than 0.5 of the probability at that token, the answer is
   `UNKNOWN`. The endpoint prefills the answer so that the letter comes first,
   and asks for temperature 0, at most 40 tokens and the top 20
   log-probabilities. Code: `ml/serve/glimmer_serve/endpoint.py`,
   `InferResult.Validate` in `internal/protocol/gateway.go`.

4. **Screen format.** `internal/vt` rebuilds the screen with
   `github.com/charmbracelet/x/vt` v0.1.0, one line of scrollback and at most
   250,000 cells. `screenproj.Project`, used by both the dataset build and the
   Analyst, then applies in this fixed order:
   1. human-input exclusion `human-input-v1`: unless the draft is known
      `CLOSED`, the whole screen is blanked, or refused when the cursor is
      unknown or hidden;
   2. cleaning: trailing blanks and controls that show nothing are removed; a
      row with invalid UTF-8, a cursor-moving control, ESC or a C1 control is
      refused;
   3. the redactor over the whole screen and its width (point 8);
   4. the domain mask, `domain-mask-v1:` followed by the list's fingerprint,
      which replaces each listed name with `[domain]`, or `none`;
   5. the cut: trailing empty rows dropped, then the last N rows, each at most
      512 runes.

   N is `ml.screen_lines`, an integer from 1 to 40 with no default; a window
   does not record it. The prompt shows the lines between `<<<` and `>>>`
   under `screen (last K lines):`. `gifford dataset build --scripted-input`
   is for recordings in which a script typed every key: it needs
   `--split-file` and a timeline for every session (point 7), counts every
   draft as closed, so step 1 blanks and refuses nothing, and the manifest
   records `scripted_input: true`. Code: `internal/vt/emulator.go`,
   `internal/screenproj/`, `internal/config/config.go`,
   `internal/cli/dataset/build.go`.

5. **Event fields in the prompt.** No field of the event envelope enters the
   prompt. At serving time the input comes from a state observation
   (`report_state_observation`, added in contracts-v1.3): its harness, activity
   counts and screen rows, which the proxy sends only after excluding the
   draft and otherwise withholds. Its native events and exit code feed the
   model-free route instead. In a dataset window, ids, sequence range and
   times stay in the provenance, outside the model input;
   `TestWindowHasNoContentFields` pins that boundary. Code:
   `internal/protocol/state_observation.go`, `internal/analyst/analyst.go`,
   `internal/dataset/window_test.go`.

6. **Alignment.** `gifford label align` and `gifford dataset build` drop every
   window within a margin of a state transition, whether it comes from a hook,
   from the session's activity or from a scripted session's timeline, before
   any label is chosen, a manual one included. The margin defaults to 300 ms
   (`margin_ms`). Other defaults: a state without events lasts at most 5
   minutes, and windows are matched at their anchor instant
   (`event_window: anchor`, marked provisional). All three are settings of the
   align configuration file, not constants. `gifford dataset build` records
   the margin and the file's SHA-256 in the manifest as `align` (`margin_ms`
   and `config_digest`, null when align ran with its defaults). The replay
   cuts a window every second of wall clock from the session start, each
   covering the interval before its anchor; `gifford dataset build --every`
   changes the spacing, and the code marks the default as provisional too.
   Code:
   `internal/label/alignconfig.go`, `internal/label/align.go`,
   `internal/dataset/replay/replay.go`, `internal/dataset/manifest.go`.

7. **Labels by source.** A manual annotation wins over every other label. A
   scripted session's timeline, `labels/timeline.jsonl` in the data
   directory, is written by the program that drives the harness: each line
   gives the state it saw on screen from that instant until the session's
   next line, or `NONE` when it does not know. Its script label wins over a
   hook label only when the two agree; when they differ the window gets no
   label and goes to a person as a conflict. Script and hook labels win over
   weak ones. Hook labels apply only to hook harnesses (Claude Code by
   default) and only inside a root session proved for the recording's exact
   capture, harness and version. The root-session sources and the
   approval (D) and question (E) wait sources are all off by default, each to
   be enabled for one exact harness version. Weak labels derived from activity
   never enter a dataset; imported ones (`labels/weak.jsonl`) enter only with
   `weak.keep: train` and only in train windows, so validation and test never
   take a weak label. Code: `internal/label/` (`align.go`, `timeline.go`),
   `internal/cli/dataset/build.go`.

8. **Redaction.** One redactor, `internal/redact`, at `PolicyVersion` `"2"`,
   which covers its rules, the placeholder (one U+2022 per masked cell, so
   widths are kept), the Unicode 17 width model and the wrap rules. It masks
   private keys, credentials in connection URLs, Authorization credentials,
   vendor API keys and tokens, JSON Web Tokens, values of secret-named keys and sensitive paths.
   The dataset build and the Analyst take it from the same
   `ML.ProjectionPolicies`, and the gateway runs it again before anything
   leaves the node. Every window and every call record carries
   `redaction_policy_version`. No production caller passes sensitive paths, so
   the path rule masks nothing today. Code: `internal/redact/new.go`,
   `internal/config/config.go`, `internal/cli/hub/state_analysis.go`.

9. **Splits and readiness gate.** `split-v1` takes SHA-256 over
   `gifford.dataset.split.v1`, a NUL byte and the group digest, and its first 8
   bytes modulo 100: train below 70, validation 70 to 84, test 85 and above.
   The replay pipeline uses the recording session as the group;
   `internal/dataset/WINDOW.md` still lists that rule as open.
   `gifford dataset build --split-file FILE` replaces split-v1: the file puts
   each session in `train`, `validation` or `test`, every session of the data
   directory must be listed, weak labels enter only train, as under split-v1,
   and the manifest records the file's SHA-256 as `split_file`. Readiness
   defaults, each overridable under `ml.readiness`:
   - at least 1,500 train, 300 validation and 300 test windows;
   - A, B, C and F: at least 100 in train and 20 in test;
   - D and E: at least 150 in train and 30 in test, from at least 3 harnesses
     in each;
   - G: at least 50 in train; no label under 3 % of train;
   - at least 3 harnesses with 200 train windows each, and every train harness
     present in test;
   - no synthetic A, B, C or G window; synthetic D, E or F at most 30 % of that
     label in train; nothing synthetic or weak in test; no group in two splits.

   Code: `internal/dataset/stages.go`, `internal/dataset/readiness.go`,
   `internal/dataset/manifest.go`, `internal/cli/dataset/splitfile.go`.

10. **Metrics and safety gate.** `gifford eval` fits a temperature on
    validation (between 0.05 and 20, minimising log-loss) and the SAFE
    threshold θ on calibrated p(A): the smallest θ that keeps the model-only
    false SAFE rate on validation at 0.5 % or less, or 0.97 when validation has
    no window outside A. For test and for validation it reports accuracy,
    macro-F1, recall per state, false IDLE, false SAFE, ECE and Brier score
    before and after calibration, the UNKNOWN rate and latency p50 and p95,
    then a shorter row per harness, harness version and source quality. The
    adapter gate passes only if all six criteria pass:
    0. the dataset manifest passed its readiness gate;
    1. on test, macro-F1 up, false IDLE down and calibrated ECE no worse than
       the base model;
    2. false SAFE after guards does not rise: over test windows whose true
       state is not A, replay the output-quiet guard and compare the residual
       false SAFE rate; the candidate rate must not exceed the base rate. The
       child/tool guard cannot be evaluated from a prediction and is recorded
       as unavailable. This criterion is `PENDING` only when either profile is
       missing or there is no test window outside A;
    3. recall of D and of E not lower;
    4. adapter latency p95 within budget, 1.5 s by default;
    5. safety regression suite: the candidate must account for every fixture
       in the fixed suite with matching facts, no invalid or error result, and
       calibrated p(A) below its own validation-fitted θ. The base result is
       reported but does not decide the verdict. This criterion is `PENDING`
       only when no safety fixtures were supplied.

    Code: `internal/eval/calibrate.go`, `internal/eval/metrics.go`,
    `internal/eval/gate.go`, `internal/eval/prompt_gate.go`.

## Importing pipeline evidence

`gifford ctl model register-pipeline EVIDENCE.json` is the local product path
from sealed inputs to an activatable registry row. The document names, with a
SHA-256 for each, the dataset manifest, training-run metadata, adapter
metadata, ordinary base and candidate predictions, the fixed safety manifest,
and both safety prediction files. Relative paths are resolved from the
document's directory before the local Control API receives them.

The importer verifies every file hash and cross-file identity, parses the
manifest and recomputes its readiness result, then calls `eval.Evaluate` over
the exact prediction bytes with the fixed bin count and latency budget. It
persists the linked dataset, training, adapter and evaluation rows. A computed
FAIL, PENDING, incomplete run or underpowered dataset remains durable for
audit but creates no registry row and cannot be activated. Only a computed
PASS goes through the same immutable registry writer used by `model.register`.
The latter also requires the canonical evaluation row to contain the imported
report, its digest and the evidence references; copied declarations without
that provenance are rejected. Code: `internal/modelprofile/import.go`,
`internal/modelprofile/service.go`, `internal/cli/ctl/model.go`.

## Permission-dialog candidate policy v2

`live-input-v2` is an additive candidate-data policy. It does not alter the
v1 screen or manifest seals, activate an adapter, reuse a v1 calibration, or
change delivery. In particular, recognizing a permission screen never
approves it and is not evidence for any Proxy prepare or commit guard.

The policy retains `empty_input_v1` and adds `permission_dialog_v1`. The new
form is fully canonical: the model sees exactly `[permission-dialog]`, with a
19 by 1 synthetic screen and synthetic cursor at (0, 0). No title, option,
reason, background row or physical cursor survives. The marker represents an
already-proved observation; it never supplies label D.

The matcher and its causal evidence are versioned as
`permission-dialog-v1` and `permission-dialog-evidence-v1`. They are disabled
by default. A source is enabled only by an exact harness and complete recorded
version line in config, for example:

```yaml
ml:
  permission_dialog_sources:
    - harness: opencode
      harness_version: <exact recorded 1.18.29 version line>
```

The closed matcher bank has measured layouts for Antigravity 1.4.8, Claude
Code 2.1.280 and OpenCode 1.18.29. Muse is deliberately absent. Unsupported
versions, duplicates and approximate version lines fail configuration. A
harness that reflects typed content in its modal remains disabled even if its
fixed legend otherwise matches.

Session records every outer-input arrival and its later content-free
classification, whether its parser remains pending, key generation, and a
post-guard boundary for every PTY read. Its closing totals prove that no
arrival or read is absent. Under the session mutex, the tracker requires a
complete non-modal predecessor, a genuinely CLOSED guard, fully aligned input,
no pending bytes or synchronized update, one exact signature, and no later
human-key generation. Human or unknown input, resize, a partial frame, a
changed signature or ambiguity invalidates the episode. Silence never means
zero keys. Replay reconstructs the same transport and parser alignment and
PTY-read boundaries over the unchanged raw bytes, validates the causal facts,
and withholds an anchor that falls inside an unsealed read. A capture without
the v2 evidence seal cannot be upgraded by replay or import.

D truth is separate from recognition. A retained permission form must fall in
either a real recipe timeline interval labeled D or a version-enabled native
approval wait in the same capture and native root run epoch. The latter is the
harness event normalized as `approval.requested`; the closed hook projection
links it to exactly one startup/root event and to the recording's exact
capture, harness and version. Repeated startup evidence is a new or ambiguous
epoch and yields no hook label. Manual, weak, screen-marker and matcher-derived
D are all rejected. OpenCode's `permission.asked` for the question tool is E,
not D.

`dataset build --live-form-v2` requires 1 Hz, an alignment configuration with
a 600 ms margin, and a split file. The private capture plan must declare its
duration before capture and must not be extended to manufacture more episodes.
The split-file review must keep every recording session and every native
lineage in one split; the public dataset has only opaque session group digests,
so it does not infer a private lineage map. The split-file and alignment-config
digests seal those public build inputs.

Permission windows deduplicate by `(recording run/epoch, tracker episode,
natural anchor, policy)`. The manifest reports natural permission anchors and
unique episodes separately. A replay or reimport of the same anchor cannot add
to either count; multiple natural anchors in one episode remain multiple
windows. This intentionally departs from PRD section 9.17's normalized-screen
hash: all canonical permission screens have identical normalized text, so
that rule would collapse unrelated real approvals.

The readiness defaults do not change: D still needs 150 train and 30 test
windows, at least three harnesses in each split, and every label still needs
three percent of the complete final train set. A blind v2 test capture happens
only after matcher, prompt and recipe freeze, includes new Antigravity
episodes, and excludes every inspected design capture from the final test
split. Historical captures are only candidates: missing evidence means
recapture. Base and candidate are evaluated on the identical v2 manifest and
inputs, with a v2-specific calibration and candidate artifact; neither the v1
calibration nor a v1 LoRA is treated as v2 evidence.

The privacy canary is text typed before a modal. It may remain only in the
unchanged private raw recording. The observation, inference payload, dataset,
new evidence events, logs and fixed errors contain neither that text nor any
physical modal row. The OpenCode acceptance probe must begin with a real
delivery-opened turn and reach CLOSED through the existing Draft Guard. If the
permission prompt does not appear under that recipe, the recipe contributes
no D episode; the Draft Guard is not relaxed.

## Where the contract is written down

The wire types are `infer_request`, `infer_result`, `gateway_profile` and
`glimmer_call_record` in `schemas/v1/`, frozen since `contracts-v1` (see
[FREEZE.md](FREEZE.md)), plus `model_status_result` for `gifford ctl model
status`. The prompt versions in `internal/analyst/prompts/` are
`ANALYZE_AGENT_STATE` v1 and v2, `CHOOSE_ORCHESTRATION_ACTION` v1 and v2, and
`TRIAGE_MESSAGE` v1. Their defaults are ANALYZE v1 (`PromptVersion`), CHOOSE v2
(`ChoosePromptVersion`) and TRIAGE v1 (`TriagePromptVersion`), selected in
`internal/analyst/prompt.go`. `ml/serve/README.md` describes how the endpoint
reads the label.
