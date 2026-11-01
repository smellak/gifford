# Window contract (S0)

`dataset.Window` is one labelled training example for the `ANALYZE_AGENT_STATE`
contract. This note fixes what a window holds and validates. How windows are cut
from captures is S1.

## What a window holds

| Part | Fields | Rules |
|---|---|---|
| `contract` | top level | Always `ANALYZE_AGENT_STATE`. It names the contract, so it is not an input feature. |
| `input`: the model boundary | `harness`, `seconds_since_last_output`, `output_bytes` (`last_0_5s`, `last_2s`, `last_10s`), `new_child_processes`, `previous_state`, `previous_state_age_seconds`, `screen_lines` | Exactly the features the model sees, and nothing else; a test pins this set. Counts are non-negative, and the nested windows never decrease. Bytes inside a trailing window mean the last output is inside it, and no bytes mean it is not. `new_child_processes` has three states in one required member: `null` means the process source was not observed (the prompt reads `unknown`), `[]` means it was observed and started no child (`none`), and a list names the children. The list is never inferred from another source. It holds at most 8 entries, sorted and without duplicates. Each entry is an executable's basename, optionally followed by one space and one verb from a closed set: `build`, `check`, `compile`, `exec`, `fmt`, `format`, `generate`, `install`, `lint`, `list`, `run`, `status`, `test`, `update`, `vet`. The basename is 1–32 ASCII characters from letters, digits and `._+-`, not starting with `-` or `.`. So an entry holds no full argv, path, URL, flag, environment, token, name or free text. The producer drops any other argument before building the list, and `Validate` refuses one that reaches a window. `CanonicalChildProcesses` only sorts and dedups; it sanitises nothing. `previous_state` is any `protocol.AgentPhase`, or empty for a session's first window, which then has age 0. `screen_lines` are the screen as `screenproj.Project` projects it, the Analyst's own model input: the last `n` rows as rendered text, trailing empty rows dropped, each of at most 512 runes. Any control or escape character means raw terminal output and is refused. |
| `screen`: evidence, not features | `cols`, `rows`, `redaction`, `human_input_exclusion`, `digest`; optional `observation_form`, `observation_policy_version`, `observation_episode_digest`, `observation_anchor_digest` | Fails closed. `redaction` and `human_input_exclusion` must each be `applied`, with the `policy_version` the window's `policies` name. Both are needed, because the redactor alone does not remove what a human typed. There are at most `rows` lines. `digest` is `ScreenDigest`: SHA-256 over a canonical preimage. It is `gifford.dataset.screen.v1`, then cols, rows and the line count, then each line as its byte length and its bytes. Every integer is decimal text ending in a NUL. Splitting the same text into lines differently never gives the same preimage. The optional observation members occur only under `live-input-v2`; the two digests and `permission-dialog-v1` policy occur only on its canonical permission form. They are evidence outside the model boundary. |
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

- **`n`:** how many of the screen's last lines enter `screen_lines`. It is
  config.yaml's `ml.screen_lines`, 1 to 40, with no default; the window does
  not record it, and the contract only bounds the lines by `rows`.
- **Windows per session:** how many windows are drawn from one session, and how
  they are spaced.
- **`event_window`:** the span between `window_start` and `window_end`. The
  contract only orders the times, and fixes no length.
- **Co-anchoring:** how `anchor_at` relates to the events in
  `sequence_from`…`sequence_to`, and to other windows of the same session.
- **The split rule:** what `group_digest` hashes (session, agent or capture), so
  that no group spans two splits.
- **No output yet:** a pane that never printed. `seconds_since_last_output`
  has no "never" value yet, so no such window is built: the replay marks a
  window before its session's first output `NoOutputYet`, the pipeline
  refuses it, and `gifford dataset build` leaves it out and counts it as
  `no_output_yet`. An output of no bytes is no first output.

## Manifest

`BuildManifest` describes a dataset of windows without their content. It
records:
- the schema versions, the contract and the single set of policy versions;
- counts by split, label and domain, each bucket stated even when zero;
- one entry per window: its id, split, label, domain, `window_digest` (the
  SHA-256 of its JSON encoding), `group_digest` and `source_digest`;
- `split_file`: the SHA-256 of the split file that assigned every entry
  `train`, `validation` or `test`, or null when split-v1 assigned the splits;
- `align`: how align labeled the windows, as its `margin_ms` and the
  `config_digest` of its config file, null for the defaults; `dataset build`
  always records it, and a null `align` says nothing;
- `scripted_input`: true when a script, not a human, typed every session's
  input, so `dataset build --scripted-input` counted every draft closed and
  `human-input-v1` blanked nothing; false otherwise;
- `screen_form`: `live-input-v1` when `dataset build --live-form` cut every
  screen through the empty-input gate, `live-input-v2` when
  `--live-form-v2` used the closed empty-input/permission-dialog policy, and
  absent otherwise (see "Live forms");
- for `live-input-v2`, each entry's observation form and permission policy,
  episode and anchor digests, plus `observation_counts` with permission
  anchors and unique episodes counted separately;
- a `dataset_digest` over all of it.

The digest uses the same canonical style as the screen digest:
`gifford.dataset.manifest.v1`, then each field ending in a NUL. The counts are
derived from the entries, so they are left out of the digest. A split file's
digest, then the align policy, then `scripted_input`, then `screen_form`,
come last, each only when there is one or it is true. Optional observation
entry fields and counts are also domain-separated and present only in v2. A
manifest without them therefore keeps the digest it had before they existed;
the v1 seal is unchanged.

It refuses:
- an empty dataset;
- a repeated window id;
- windows built under different policy versions;
- an invalid window;
- a group that appears in two splits, so no group leaks between train and test;
- in `live-input-v2`, an unformed entry, a D entry without the canonical
  permission form, a permission form that is not D from a real script or hook
  interval, a wrong observation policy, or a repeated permission anchor.

Entries are sorted by window id, so the manifest does not depend on the order of
its input. `Validate` re-derives the order, the one-split rule, the counts and
the digest. `ParseManifest` decodes as strictly as `ParseWindow` does, with a
64 MiB limit. Legacy manifests may leave out `screen_form` and every v2
observation member, so they read as they always did. A `live-input-v2`
manifest must carry all members its form requires; none may be `null`.

## Pipeline

`RunPipeline` turns recordings into an inventory: windows and their manifest.
It reads recordings only through `RecordingSource`. For each one, in order, it:
1. projects the screen with `screenproj.Project` under
   `PipelineDeps.Projection`, the policies the Analyst serves with: the human
   input excluded (`human-input-v1`: unless the draft is known closed, the
   whole screen is blanked), trailing blanks and controls that show nothing
   dropped, the whole screen redacted with its width, the domain names
   masked (`domain-mask-v1`) or none, then the last `n` rows kept, each of
   at most 512 runes;
2. assigns the split (`Splitter`), builds the window and validates it.
   `SplitTable` assigns a split file's splits by group; a group it does not
   list is a split error. `PipelineOptions.SplitFile` carries the file's
   digest into the manifest.

The recording's draft reaches the projection as the input guard names it:
`closed` as `CLOSED`, anything else as `UNKNOWN`. The screen evidence is the
recorded pane size. The policy versions are the ones the projection ran.

It fails closed. Any of these ends the run with no inventory:
- a missing source or splitter, or invalid projection policies, or
  `MaxWindows` out of range;
- a source or split error;
- a recording made before its session's first output (`NoOutputYet`), whose
  quiet time would have to be invented;
- a recording in live form whose screen the live session would withhold
  (`LiveWithheld`), since the live Analyst would get no input for it;
- any projection error: a draft not known closed with a cursor unknown,
  hidden or off the screen; a screen out of bounds; a row that is not valid
  UTF-8 or holds a control with an effect on the screen (backspace, tab, line
  feed, carriage return, escape, a C1 control), which cleaned could hide a
  secret; or a redactor failure;
- more recordings than `MaxWindows`;
- an invalid window;
- anything the manifest refuses;
- a cancelled context.

Errors name the stage and the recording index. They never wrap a source's,
split's or redactor's own error, because that error could quote the screen;
the projection's errors are fixed reasons. Before the pipeline, `gifford
dataset build` drops and counts a window made before its session's first
output, one withheld in live form, and one whose cursor is unknown or hidden
while the draft is not closed, so in a build none of these errors happens.

## Live forms

`gifford dataset build --live-form` cuts every window as the live Analyst
would get it, and the manifest records `screen_form: live-input-v1`. The
replay (`replay.Options.Live`) puts each window's screen through the session's
own gate, `session.LiveScreen`, under the profile `gifford run --agent` picks
for the recorded harness and version. A window keeps its screen only when:
- the input guard is closed, as the `input.draft_*` events report it, which
  report its every move; it is closed until the first, as a session's starts;
- the profile is measured, with an input line; a version outside its range
  falls back to a profile with no measurement, as the run's generic fallback
  does;
- the input line under the cursor reads empty;
- the frame is within the report's bounds and valid UTF-8;
- the input line and its measured bounds identify exactly one area, on the
  cursor's row.

A window that passes carries the screen with that area's rows replaced by
`[input]`, and its draft closed, so `human-input-v1` blanks nothing, as for
the live Analyst. Any other window carries no screen, and `LiveWithheld` names
the first rule it fails: `draft_not_closed`, `profile_unmeasured`,
`no_input_evidence`, `input_not_empty`, `frame_out_of_bounds` or
`no_single_input_area_at_cursor`. The build leaves such a window out after
labelling and counts it by harness, label and reason; the pipeline refuses
one that reaches it. Labels never read the screen, so a window keeps its id
and its label in either form. `--live-form` excludes `--scripted-input`,
since it takes the draft from the guard. The features are the ones any build
gives.

A live session also waits until every human chunk is written and folded and
no key sequence is held in part. A v1 recording keeps no trace of those
beyond the guard's moves, so the v1 replay reads the guard alone. V2 adds the
content-free causal seal described below.

### Permission-dialog form (`live-input-v2`)

`gifford dataset build --live-form-v2` retains the v1 empty-input form as
`empty_input_v1` and adds `permission_dialog_v1`. The latter has no physical
terminal row: its whole model input is the single row `[permission-dialog]`,
with synthetic geometry 19 columns by 1 row and a synthetic cursor at (0, 0).
The marker is a representation, never label truth and never permission to
deliver, approve or deny anything.

The feature is off unless `ml.permission_dialog_sources` names the recording's
exact harness and complete recorded version line. The matcher bank supports
only the measured Antigravity 1.4.8, Claude Code 2.1.280 and OpenCode 1.18.29
layouts. Muse has no source. An enabled new capture records
`permission-dialog-evidence-v1`: every outer-input arrival and classification,
every post-guard PTY-read boundary, key generation and closing totals. The
payloads hold counts, fixed enums and digests, never terminal or typed text.
Old captures and imports have no such seal and cannot produce a permission
form.

Both live Session and replay feed the same tracker only at complete PTY-read
boundaries. A valid episode requires a complete non-modal frame first, a real
CLOSED guard, no crossed or unresolved input, no pending bytes or synchronized
update, one exact signature, and an unchanged human-key generation. A human or
unknown key, resize, partial frame, ambiguity or signature change invalidates
the episode. Silence never proves zero keys. Replay additionally accounts for
every arrival and preserves both raw-chunk and PTY-read boundaries; a window
inside an unsealed read is withheld.

A permission form enters a dataset only with D truth from a real scripted
interval or the exact native approval wait linked to this capture's single
root run epoch. Manual, weak and marker-derived D are rejected. The hook path
is version-gated and starts at the observed approval wait; an approval request
alone is not backdated into D. Repeated root startup evidence is ambiguous and
produces no hook interval.

The v2 build fixes sampling at 1 Hz and the alignment margin at 600 ms. Its
episode digest covers `(recording run/epoch, tracker episode, policy)`; the
anchor digest adds the natural anchor. Replaying or reimporting the same tuple
does not add a window, while different natural anchors of one episode remain
different windows. The manifest reports both anchor and episode counts. This
deliberately differs from the normalized-screen-hash deduplication proposed in
PRD section 9.17: every canonical permission screen has the same normalized
text, so that rule would collapse unrelated real episodes.

## Limits

- **Validation is shape and coherence only.** The screen digest is recomputed,
  but the other digests are not checked against any stored bytes.
- **The manifest names windows, it does not check them against bytes.** See
  below.
- **No training here.** `ml/` trains offline.
- **No change to `protocol`, `schemas`, `vt` or `redact`.**
- **Fixtures are invented:** no real sessions, paths, hosts or secrets.
