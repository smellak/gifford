# Calibrating the base model: dataset v0 and its evaluation

This is how the `ANALYZE_AGENT_STATE` dataset v0 is recorded, labelled, split and sealed, and how the base model is evaluated on it. Nothing here trains a model. The dataset measures the base model and tunes four levers around it:

- a bounded output;
- prompt v2 with deterministic features;
- dynamic few-shot examples;
- post-hoc calibration.

Recordings, windows, screens, masks, session scripts and few-shot examples never leave the recording machine. They never enter this repository, a message or a pull request. Only metrics and counts are published, in `docs/RESULTS.md`.

## The recording account

The sessions are recorded under a dedicated account, used for nothing else.

- **Permissions.** Its home, its configuration and the directories that hold credentials are mode 0700. Every file the procedure creates is 0600 from birth (`umask 077` in every shell).
- **Keys.** Each harness's API key is loaded only in the shell that starts that harness, with `set -a; . <env file>; set +a`. It is never printed, never passed in argv and never copied into another file.
- **Login errors.** A harness that reports a token or login error stops the session. Nobody re-authenticates from the procedure.
- **Versions.** The harnesses are pinned to the measured profile versions, and their self-update is off for the whole dataset.
- **MCP.** Gifford's MCP server is configured in each harness as `/absolute/path/to/gifford mcp`.

## Scripted sessions

Each session is one `gifford run --agent <id> --harness <harness> -- <harness command>` in a fresh copy of the same workspace: a small git project with one failing test.

A driver script works in a second tmux pane. It types into the harness pane and reads that pane's rendered screen with `capture-pane`, never the raw output. The pane the driver types into is never the one anybody watches.

### Kinds of session

A session is one of three kinds. Its kind, its exact harness command and its workspace configuration are recorded with it.

| Kind | Steps, in order |
|---|---|
| `demo` | A, held 30 s · B: a request for a long explanation, with no tools · A, 20 s · C: a request to run `sleep 20; echo done` with the shell tool, which runs without approval · A, 20 s · E: a request to ask one clarifying question about which function to fix, and to wait; held · the answer, which starts the fix · A, 20 s |
| `ask` | A, 30 s · the same `sleep 20` request under an approval mode that asks: D, held, then approved once; C while it runs · A, 20 s · E, held · the answer: each edit that needs approval gives D again · A, 20 s |
| `error` | A, 30 s · a prompt to a harness whose model or provider cannot answer: F, held 40 s · the harness exits |

- **Holds.** D and E are held for 30 to 60 s. A menu after the first in the same step is held 8 s. Each session records its holds.
- **Fixed texts.** Each step's request is a fixed text, the same in every session, so the screens differ only by what the harness does.
- **Muse Code.**
  - `demo` and `error` run with `--trust-workspace --disable-approval --disable-sandbox`. Where the harness's shell sandbox cannot start, as on a machine that restricts unprivileged user namespaces, a shell command with no approval and no `--disable-sandbox` never runs.
  - `ask` runs with `--trust-workspace --approval-mode untrusted --approval-judge off`, and either with or without `--disable-sandbox`. Both give D, with the same menu:
    - it approves the command line in stages, one per command in it, each side of a pipe included: `sleep 20; echo done` takes two;
    - each stage is allowed once, or always in the workspace, or the whole command is aborted;
    - without `--disable-sandbox`, the command first fails inside the sandbox, which cannot start on the recording machine, and the agent runs it again outside it.

    Each session records whether it ran with the flag.
  - `error` names a model that does not exist.
- **OpenCode, the Build agent.**
  - `demo` runs with no `permission` block. `ask` sets `bash` and `edit` to `ask` in the workspace's `opencode.json`. `error` points the provider at an address that refuses connections.
  - The label bridge is installed in an isolated config dir with `gifford label bridge opencode <dir>`, and the session runs under `env OPENCODE_CONFIG_DIR=<dir> gifford run …`, so its hooks are recorded too.

### The driver's timeline

The driver writes one line for each transition it sees, to the timeline that `gifford label align` reads (`labels/timeline.jsonl`):

```json
{"session":"<recording session id>","at":"2026-11-01T10:00:00.000Z","label":"WAITING_APPROVAL"}
```

`label` is an agent phase, or `NONE`, for no known state. A state holds from its line to the session's next line.

- **When a state starts.** The driver writes a state only once it has seen it on screen: the approval menu, the running command, the first streamed text, the question.
- **What it never writes.** It never writes a state it has not seen.
- **Before each of its own actions**, such as typing a prompt, approving a menu or answering, it writes `NONE` first, before the screen changes.
- **When an observed state ends.** The end of a state the driver did not cause, such as the end of a stream or of a command, is written at the first poll that no longer shows it.
- **No A after a question.** An idle hold is not written as A when the agent's last message ends in a question: that is E, and the driver did not ask for it.
- **No A after an error.** After F, the session ends with no A: the error stays on screen, and the same screen would carry two labels.

**The margin.** The driver polls the screen every 0.5 s while it waits, and every 0.25 s while a menu or a command is on screen. A `capture-pane` takes a few milliseconds. A state therefore ends at most about 0.51 s before its line, so align runs with `margin_ms: 600`, no less than one poll plus one capture: no window of a state's first instants takes the label of the state before. The manifest records the margin and the sha256 of the align config as `align`, under the dataset digest, and `RESULTS.md` repeats both.

## Labels

The letters are those of prompt v1:

| Letter | State | Definition |
|---|---|---|
| A | `IDLE_AT_PROMPT` | The input line is empty and ready. Nothing is running or streaming. No question or menu waits for an answer. |
| B | `THINKING_STREAMING` | The model is producing text or reasoning, and no tool is running. |
| C | `RUNNING_TOOL` | A tool or command started by the agent is running. |
| D | `WAITING_APPROVAL` | A permission or approval menu is on screen and waits for a human choice. |
| E | `ASKING_QUESTION` | The agent's last turn asks the human a question, and it waits for the answer. This holds whether it uses a question widget or ends its message with the question. A finished answer that merely contains a question mark is A. |
| F | `ERROR_OR_EXITED` | The agent stopped on an error it cannot get past by itself, or the harness has exited. |
| G | `UNKNOWN` | The screen does not determine the state, such as a splash screen or a frame in the middle of a redraw. |

D and E matter most: delivering a message over a permission menu or an open question is the expensive mistake. Each needs about 20 windows or more across the two harnesses.

**Where a window's label comes from.** `gifford label align` decides it, in this order:

1. **A manual annotation**, from `gifford label annotate`, wins over every other source.
2. **The driver's timeline** gives the script label: the state that holds over the whole window, margin included. A window within the margin of a timeline line is discarded and counted.
3. **A hook label**, for a harness whose hooks align reads. A script label and a hook label that disagree give no label and a `CONFLICT`. Neither wins: `annotate --doubtful` asks a human. A script label that agrees with a hook label is chosen, as `script`.
4. **A weak label**, from output activity, never makes a script label doubtful, and never enters the validation or test split.

A window with no label is dropped from the dataset and counted, as is a `CONFLICT` nobody annotated. In the dataset, a script label is a real label, like a hook or manual one.

## Windows

`gifford dataset build --out DIR --screen-lines N --mask FILE --every D --align-config FILE --split-file FILE --scripted-input` replays every session through the projection the Analyst uses:

- the human's input is excluded, and in v0 there is none: see `--scripted-input` below;
- secrets are redacted;
- names are masked;
- the last N rows are kept.

N and the mask are those of the Analyst's `ml.screen_lines` and `ml.mask`.

- **The mask file** lists, one per line, every machine, account and domain name that can appear on screen. It is generated on the recording machine from the system's own names, so nobody types them, and it is private to that machine. It also lists the name of each session's workspace, taken from the sessions' records: the name carries the session's kind, and would leak the label.
- **`--every`** is chosen so that the dataset holds 200 to 300 windows.

**No human keyboard: `--scripted-input`.** Dataset v0 is scripted: the driver types every key, and nobody types by hand.

- **Why the flag.** `gifford run`'s input guard cannot tell the driver's keys from a human's, so it keeps a human draft open. On a measured profile it closes a draft only when the screen shows an empty input line under the cursor, quiet after the harness reacts. That never happens while the harness streams, runs a tool or shows a menu. So `human-input-v1` would blank those windows, or drop them when a menu hides the cursor.
- **What it does.** With `--scripted-input`, every draft counts as closed, because no human keyboard typed one. Every window then goes through the same projection, without the blanking: redacted, masked and cut to N rows.
- **Only sessions nobody typed into.** The flag holds only for sessions in which nobody typed by hand. A session that a person typed into, even once, never enters a build with the flag.
- **What it refuses.** It needs `--split-file`. Every session in the data dir must have its timeline in `labels/timeline.jsonl`; a session without one fails the build, which writes nothing.
- **Recorded.** The manifest records `scripted_input`, under the dataset digest. `RESULTS.md` repeats it: v0 is scripted, and its drafts are counted closed because there is no human keyboard.

`gifford dataset inspect DIR/manifest.json` prints the digest and the counts. Counts are reported by label, by harness and by split.

## Split and seal

The split is by session, so that no session has windows in both halves:

- **The tuning half** is the `validation` split. It is used to choose few-shot examples, to fit the temperature and to set θ.
- **The sealed half** is the `test` split. It is used only to evaluate, once each candidate profile is fixed.

**The split file.** Sessions are assigned before any label is looked at, in a file of one JSON object per line:

```json
{"session":"<recording session id>","split":"validation"}
```

- **How sessions are assigned.** The sessions are sorted by harness, then kind, then id, and given alternately to the two halves, starting with `validation`. One alternation runs through every harness and kind, so the halves stay even, and each holds sessions of both harnesses and every kind.
- **Fixed once.** The file is written once and is not changed afterwards.
- **What `--split-file` refuses.** It fails the build on a session of the data dir missing from the file, on `train`, and on a session listed twice. Nothing falls into a half by default.
- **The manifest** keeps the file's sha256 as `split_file`, under the dataset digest.

**Sealing.**

- **Before the seal**, a sample of the B, C and D windows is read on the recording machine. Paths, accounts and machine names must come out masked or redacted, as in the other windows. Only the counts of that review leave the machine.
- Once the manifest is built, its sha256 is published, before any tuning starts.
- Nobody opens the sealed windows, or their predictions, while tuning.
- A later rebuild must give the same digest. If it does not, the seal is broken, and the evaluation says so.

## Evaluation

1. **Predict.** `gifford eval predict --profile base --manifest DIR/manifest.json --windows DIR/windows --gateway <gateway.yaml> --out base.jsonl` predicts both halves with the deployed base model.
2. **Score.** `gifford eval --profile base=base.jsonl --manifest DIR/manifest.json --out RESULTS.md` fits the temperature and θ on the tuning half and reports on the sealed half. It reports n with every metric:
   - the confusion matrix;
   - macro-F1, overall and per harness;
   - the recall of D and E;
   - false IDLE;
   - false SAFE: the calibrated p(A) is at least θ while the truth is not A;
   - ECE and Brier, before and after calibration;
   - latency p50 and p95.

   θ is the single threshold for "safe to deliver". It is set on the tuning half so that false SAFE is at most 0.5 %. There are no thresholds per label: this n cannot support them.

   `RESULTS.md` also gives the n of D by harness and by session kind. Its Dataset table repeats, from the manifest, the dataset digest, the split file, the align margin and config, and the input: scripted, every draft counted closed.
3. **A candidate profile** combines prompt v2, few-shot examples and calibration. It is evaluated the same way and compared with v1 on the sealed half. It replaces v1 only if, on the sealed half:
   - macro-F1 and false IDLE are better than v1's;
   - the recall of D and E is no worse;
   - false SAFE after the guards is no worse;
   - latency stays within budget;
   - the safety regression suite is green.

   Otherwise v1 stays, and `RESULTS.md` says so.

**Not in prompt v2: new child processes.** A replayed recording does not observe the process tree, so every window's `new_child_processes` is null, which reads as unknown. Prompt v2 does not use the feature, and `RESULTS.md` says so.

**Traceability.**

- **Versions.** Every call records its `prompt_version` and calibration version next to the base checkpoint and its revision.
- **A DEFER decision** leads back to its `ANALYZE_AGENT_STATE` call through `orchestrator_decisions.state_id`, then `state_snapshots.glimmer_call_id`, then `glimmer_calls`.
- **A decision's own `glimmer_call_id`** is that of `CHOOSE_ORCHESTRATION_ACTION`. It is empty when CHOOSE is not called.

## Limits

Dataset v0 was built and sealed with redaction policy 1. The redactor is policy 2
from the Unicode 17 width-table change, so rebuilding v0 with a later binary
produces a different manifest digest.
