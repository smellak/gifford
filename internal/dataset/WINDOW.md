# Window contract (S0)

`dataset.Window` is one labelled training example for the `ANALYZE_AGENT_STATE`
contract. This note fixes what a window holds and validates. How windows are cut
from captures is S1.

## What a window holds

| Part | Fields | Rules |
|---|---|---|
| `contract` | top level | Always `ANALYZE_AGENT_STATE`. It names the contract, so it is not an input feature. |
| `input`: the model boundary | `harness`, `seconds_since_last_output`, `output_bytes` (`last_0_5s`, `last_2s`, `last_10s`), `new_child_processes`, `previous_state`, `previous_state_age_seconds`, `screen_lines` | Exactly the features the model sees, and nothing else; a test pins this set. Counts are non-negative, and the nested windows never decrease. Bytes inside a trailing window mean the last output is inside it, and no bytes mean it is not. `new_child_processes` has three states in one required member: `null` means the process source was not observed (the prompt reads `unknown`), `[]` means it was observed and started no child (`none`), and a list names the children. The list is never inferred from another source. It holds at most 8 entries, sorted and without duplicates. Each entry is an executable's basename, optionally followed by one space and one verb from a closed set: `build`, `check`, `compile`, `exec`, `fmt`, `format`, `generate`, `install`, `lint`, `list`, `run`, `status`, `test`, `update`, `vet`. The basename is 1–32 ASCII characters from letters, digits and `._+-`, not starting with `-` or `.`. So an entry holds no full argv, path, URL, flag, environment, token, name or free text. The producer drops any other argument before building the list, and `Validate` refuses one that reaches a window. `CanonicalChildProcesses` only sorts and dedups; it sanitises nothing. `previous_state` is any `protocol.AgentPhase`, or empty for a session's first window, which then has age 0. `screen_lines` are the last `n` screen lines as rendered text: any control or escape character means raw terminal output and is refused. |
| `screen`: evidence, not features | `cols`, `rows`, `redaction`, `human_input_exclusion`, `digest` | Fails closed. `redaction` and `human_input_exclusion` must each be `applied`, with the `policy_version` the window's `policies` name. Both are needed, because the redactor alone does not remove what a human typed. There are at most `rows` lines. `digest` is `ScreenDigest`: SHA-256 over a canonical preimage. It is `gifford.dataset.screen.v1`, then cols, rows and the line count, then each line as its byte length and its bytes. Every integer is decimal text ending in a NUL. Splitting the same text into lines differently never gives the same preimage. |
| `target` | `label` | One of `A`–`G`, bound by a closed table, not by protocol order: `A` `IDLE_AT_PROMPT`, `B` `THINKING_STREAMING`, `C` `RUNNING_TOOL`, `D` `WAITING_APPROVAL`, `E` `ASKING_QUESTION`, `F` `ERROR_OR_EXITED`, `G` `UNKNOWN`. `BACKGROUND_WORK` is never a target, because the native path decides it without the model. |
| `provenance` | `session_id`, `capture_session_id`, `source_quality`, `sequence_from`/`sequence_to`, `window_start`/`window_end`, `anchor_at`, `source_refs`, `source_digest` | `source_quality` is screen-derived: `pty_screen`, `bootstrap_pipe_pane`, `bootstrap_import` or `synthetic`. The sequence range is inclusive and ordered. `window_start < window_end`, and the anchor lies within them. The source is named by `blob://` references and a `sha256:` digest, never by its bytes. |
| `environment_domain` | top level | `bank_generic` or `fleet_managed`. These are public, neutral names: any mapping from a real origin to a domain lives outside the repository. |
| `policies` | `redaction_policy_version`, `human_input_policy_version`, `domain_mask_policy_version` | All are required, 1–32 characters, with no spaces or control characters. |
| `split` | `split`, `group_digest` | `train`, `validation` or `test`, plus the `sha256:` key the split was assigned by. |

## What a window never holds

- **Raw capture bytes.** Only references and digests.
- **Human content.** There are no draft, keystroke, prompt, message or transcript
  fields, and the screen lines must carry the human-input exclusion marker.
  `TestWindowHasNoContentFields` pins the exact field set and the model
  boundary, so adding a field is a reviewed change to this contract.
- **Unprocessed screens.** A screen without both stage markers fails
  validation. The markers record that the stages ran; this package does not
  run them.

`ParseWindow` decodes strictly: no unknown fields, no trailing data, at most
1 MiB. Every member must be present at every depth, because a missing member
would decode as a zero, and zero is a valid value for many fields. No member may
be `null` either, except `new_child_processes`, where `null` is the deliberate
unknown state. So a first window states `"previous_state": ""`, and an empty
screen is `"screen_lines": []`.

Before decoding, it refuses a document that is not valid UTF-8 or holds a lone
`\u` surrogate escape. Decoding would replace both with U+FFFD, and `Validate`
could no longer see them. Then it runs `Validate`, which also refuses screen
lines and policy versions that are not valid UTF-8. Errors name the field and the rule, never content.
Decoder errors are replaced by a fixed reason, because they quote tokens and
field names taken from the document.

## Open until S1

- **`n`:** how many of the screen's last lines enter `screen_lines`. Today the
  contract only bounds them by `rows`.
- **Windows per session:** how many windows are drawn from one session, and how
  they are spaced.
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
- **No manifest here.** The dataset manifest of hashes, provenance, policies
  and counts is the next change.
- **No training here.** `ml/` trains offline.
- **No change to `protocol`, `schemas`, `vt` or `redact`.**
- **Fixtures are invented:** no real sessions, paths, hosts or secrets.
