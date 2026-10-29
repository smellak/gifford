# Window contract (S0)

`dataset.Window` is one labelled training example for the `ANALYZE_AGENT_STATE`
contract. This note fixes what a window holds and validates. How windows are cut
from captures is S1.

## What a window holds

| Part | Fields | Rules |
|---|---|---|
| Input | `contract`, `harness`, `seconds_since_last_output`, `output_bytes` (`last_0_5s`, `last_2s`, `last_10s`), `new_child_processes`, `previous_state`, `previous_state_age_seconds`, `screen` | `contract` is `ANALYZE_AGENT_STATE`. Counts are non-negative, and the nested windows never decrease. Bytes inside a trailing window mean the last output is inside it, and no bytes mean it is not. `previous_state` is any `protocol.AgentPhase`, or empty for a session's first window, which then has age 0. |
| Screen | `redacted`, `cols`, `rows`, `lines`, `digest` | `redacted` must be true. Lines are rendered text: any control or escape character means raw terminal output and is refused. There are at most `rows` lines. `digest` is SHA-256 over the lines joined by `\n` (`ScreenDigest`). |
| Target | `label` | One of `A`–`G`, bound by a closed table, not by protocol order: `A` `IDLE_AT_PROMPT`, `B` `THINKING_STREAMING`, `C` `RUNNING_TOOL`, `D` `WAITING_APPROVAL`, `E` `ASKING_QUESTION`, `F` `ERROR_OR_EXITED`, `G` `UNKNOWN`. `BACKGROUND_WORK` is never a target, because the native path decides it without the model. `previous_state` accepts every phase. |
| Provenance | `session_id`, `capture_session_id`, `source_quality`, `sequence_from`/`sequence_to`, `window_start`/`window_end`, `anchor_at`, `source_refs`, `source_digest` | `source_quality` is screen-derived: `pty_screen`, `bootstrap_pipe_pane`, `bootstrap_import` or `synthetic`. The sequence range is inclusive and ordered. `window_start < window_end`, and the anchor lies within them. The source is named by `blob://` references and a `sha256:` digest, never by its bytes. |
| Domain | `environment_domain` | `bank_generic` or `fleet_managed`. These are public, neutral names: any mapping from a real origin to a domain lives outside the repository. |
| Policies | `redaction_policy_version`, `domain_mask_policy_version` | Both are required, 1–32 characters, with no spaces or control characters. |
| Split | `split`, `group_digest` | `train`, `validation` or `test`, plus the `sha256:` key the split was assigned by. |

## What a window never holds

- **Raw capture bytes.** Only references and digests.
- **Human content.** There are no draft, keystroke, prompt, message or transcript
  fields. `TestWindowHasNoContentFields` pins the exact field set, so adding a
  field is a reviewed change to this contract.
- **Unredacted screens.** An unmarked screen fails validation. The flag records
  that the redactor ran; this package does not run it.

`ParseWindow` decodes strictly: no unknown fields, no trailing data, at most
1 MiB, then `Validate`. Errors name the field and the rule, never its content.

## Open until S1

- **`n`:** how many windows are drawn per session, and how they are spaced.
- **`event_window`:** the span between `window_start` and `window_end`. The
  contract only orders the times, and fixes no length.
- **Co-anchoring:** how `anchor_at` relates to the events in
  `sequence_from`…`sequence_to`, and to other windows of the same session.
- **The split rule:** what `group_digest` hashes (session, agent or capture), so
  that no group spans two splits.
- **No output yet:** a pane that never printed. `seconds_since_last_output`
  has no "never" value yet.

## Limits

- **Validation is shape and coherence only.** The screen digest is recomputed,
  but the other digests are not checked against any stored bytes.
- **No training here.** `ml/` trains offline.
- **No change to `protocol`, `schemas`, `vt` or `redact`.**
- **Fixtures are invented:** no real sessions, paths, hosts or secrets.
