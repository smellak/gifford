# Cooperative pane locks

Short operator guide to the per-panel lock protocol: what it coordinates,
how to configure it, and what each durable record means. This is a
description of on-disk and on-wire shapes, not a setup script; the node
performs every write below.

## Why a cooperative lock

Several automatic writers may target the same terminal panel: notifiers
that send a keystroke template, watchers that clear a prompt, and the node
itself. The protocol serializes them with one advisory file lock (flock)
per panel plus one global index lock, so two writers never interleave a
send, a capture, and an Enter on the same pane.

Cooperative means exactly that: the lock binds the file descriptor that
holds it, it does not isolate processes of the same user, and it does not
intercept the human keyboard. The checksum on every record detects
corruption; it does not authenticate the author. Authority (who may
notify, hand over, reconcile, or repair) lives in the protected register,
never in the writable state fields. A terminal `HECHO` state only says the
keystrokes were applied and seen on screen; it never says the agent read
anything or replied.

## Configuration: the `tmux_locks` block

The block is all-or-nothing: protocol identities, paths, and timing come
from configuration, never from node identity or compiled defaults. Every
key below is required unless noted.

- `peer_owner`: the owner name of the panel servers this node serves.
  Canonical owner token, and never the node owner itself.
- `peer_writers`: the deployment-specific writer names allowed to journal
  attempts. Nonempty, strictly sorted, canonical tokens, and none of the
  node-reserved writers (`provision`, `transfer`, `reconcile`, `repair`,
  and the node owner name).
- `host`: the logical host this node speaks for. Lowercase letters,
  digits, and dashes, 1 to 63 characters.
- `mode_path`: clean absolute path of the mode file that selects the
  legacy or required protocol mode.
- `admin_uids_path`: clean absolute path of the file listing the user ids
  authorized to request administrative operations.
- `operation_schema`: canonical token naming the operation digest schema
  the journal stamps into each attempt.
- `initial_take_drain`: how long a fresh panel stays out of automatic
  takes after its first durable index publication. A positive duration,
  mandatory, with no default: the operator states the drain explicitly.
- `writer_log_socket`: the common writer log socket. Empty means the
  common log is off and writers keep their local behavior; set means
  every writer reports its lifecycle to the mediator on that socket
  (see "The common writer log" below).

Any missing key, empty value, or malformed value fails startup with an
invalid-block error; there is no partial configuration.

## Provisioning a panel

Only the node provisions, and only for an administrator: a caller on the
Control API socket whose verified user id is listed in `admin_uids_path`,
which the node reads again on every operation. The commit order is:

1. Authorize first. Peer identity (process, user, group, verified flag)
   comes from the socket credentials, plus the non-wire actor derived
   from them for telemetry. A denial touches nothing on disk.
2. Create the owner directory, the socket directory, and the three panel
   files (empty lock, state, protected register) with exclusive create:
   a concurrent provision of the same panel loses the race and then
   revalidates what is already installed. Every write is followed by a
   file sync and every new directory by a parent-directory sync, so a
   crash never leaves a half-written record behind.
3. Publish the panel to the owner index and the owner id to the enrolled
   set while still holding the global index lock. Publication replaces
   each index whole (temporary file, sync, atomic rename, directory
   sync) and only moves the sequence forward; entries already listed
   keep their place, so repeating a provision converges instead of
   duplicating.
4. Record one publication row with the panel key
   (`owner/socket-sha/pane.lock`) and the timestamp taken right after
   the owner-index directory sync. The caller persists the row in its
   own transaction.

A repeat provision of the same owner and binding is a no-op that
revalidates everything; any mismatch with the installed files (different
owner, binding, or generation skew) fails closed and leaves the files
untouched.

With `tmux_locks`, the node never writes to a panel it does not own. A
delivery to a panel whose owner id is not in the enrolled set, or that no
owner index lists, is deferred as `not-incorporated`; a delivery to a
panel that another owner holds is deferred as `foreign-owner`. Neither
sends a keystroke. A missing file on an incorporated panel is never read
as "not incorporated": the panel is reported invalid and the write does
not happen.

Takes honor the drain: `take` on a provisioned panel is refused until
the configured duration has elapsed since that panel's first durable
index publication, and a panel with no publication row cannot be taken
at all. Only an administrator takes a panel, through the Control API; the
node never takes one by itself.

## Doctor warnings

`doctor` reports two silent misconfigurations as warnings, never as
failures: `tmux_locks` enabled without `writer_log_socket` (the
common writer journal stays off), and, in live mode, a model
selection stuck at `DEGRADED`, showing the recorded reason. A warning
keeps the exit code at zero; only a failure changes it.

## The operator command line (`gifford ctl pane`)

`gifford ctl pane provision|take|return|reconcile|repair|status`
drives the same administrative operations through the Control API.
Every subcommand takes one flag per field of the pane target and
infers none of them: `--owner-uid`, `--socket`, `--pane-id`,
`--host`, `--boot-id`, `--pane-pid`, and `--pane-start`.
`reconcile` additionally takes `--expected-attempt` (the attempt
identity the operator inspected, or `-`), and `status` takes only
the target and only reads: it prints the register and state receipt
(owner, owner generation, epoch, pending transfer, phase, state
generation, and transaction) without touching the lock.

Each mutating subcommand prints exactly one JSON line on stdout with
`op`, `result` (`changed`, `noop`, or `error`), `txid`,
`owner_before` and `owner_after`, `owner_gen`, and `phase`; on
failure the line carries `result: error` with the API `code` and its
fixed message, never the node's internal error text. It reads the lock receipt before and after the call so
the operator sees what changed. The exit code is 0 only when the
result is `changed` or `noop`.

The node serves these operations only when the config has a
`tmux_locks` block; without one, each answers that the method is not
implemented.

## The attempt journal (`.state`)

Each attempt is journaled in the panel's state record while the caller
holds the pane lock. Every update raises the generation by one and
reaches the disk before the caller may take its next physical step:

- Begin: the record is read once, then the attempt is made durable as
  `EN_CURSO` with a fresh attempt id, the operation digest, an empty
  result, the writer identity, no child, and step zero. An attempt
  already in flight or left uncertain, a record of another pane or
  process, or a generation without room for the attempt refuses the
  begin, and no effect may start.
- Steps: each sub-step of a multi-step effect and each started child
  process is journaled before the corresponding process starts. A child
  recorded as zero with a step of one or more means unknown, never "no
  child".
- Finish: the terminal phase is journaled once, `HECHO` when the effect
  was observed exactly, `INCIERTO` otherwise. Finishing never retries
  and never resets the attempt id.

The result vocabulary is closed: `-` while running, `CONFIRMADO`,
`SIN_EFECTO`, or `REPARADO` afterwards. A failed durable write before
the first effect forbids the effect; after it, the attempt is uncertain.

## Reconcile and repair

Reconcile clears an uncertain or in-flight attempt using evidence from a
fresh observation taken under the pane lock: whether the binding still
matches, whether the effect is visible, whether the composer is empty,
whether the writers and children have exited, and whether the drain wait
has elapsed. The screen itself never crosses into the decision or the
telemetry. Depending on the evidence, reconcile confirms the attempt or
closes it without effect. Reconcile is conditional on the exact attempt
an operator inspected: it takes the target plus the expected attempt
identity, and it does nothing when the record moved on.

Repair handles corrupt or unreadable records, where no trustworthy
attempt identity may exist, so it takes only the target. It advances
the epoch, resets the generation, and parks the record as
`INCIERTO`/`REPARADO`; it never returns a record to idle, so the
inhibition holds until a reconcile with evidence. A repaired record
keeps no attempt, so that reconcile takes the attempt `-`: it returns
the record to idle without effect once the binding matches, the
composer is empty, no writer or child remains and the drain wait has
elapsed. An earlier attempt identity never clears it.

Both operations answer with a common idempotent acknowledgement
carrying only whether anything changed. Their parameters carry the
complete live identity of the pane (owner, socket, pane id, host,
boot id, process id and start time): callers cannot select a pane by
its reusable id alone, and the actor always comes from the socket peer
credentials, never from the parameters. Every administrative operation
is logged under the single event name with closed result vocabularies
(denied, changed, unchanged, failed, confirmed, no effect, repaired),
and the log carries evidence verdicts and digests only, never record
or screen bytes.

## The common writer log

`tmux_locks.writer_log_socket` names the single socket where writers
report. When it is empty the common log stays off and writers keep
their local behavior; the client deliberately has no default path.
See the configuration section above for the key semantics.

A writer reports a closed lifecycle per attempt while it retains the
pane lock. It opens one interval, marks the start and the end of
every keystroke with an outcome of `ok`, `failed`, or `uncertain`,
and releases the interval while still holding the lock. An
uncertain outcome is never retried: after the opening boundary is
acknowledged the physical write may already have happened, so a lost
acknowledgement stays uncertain instead of running the effect twice.
Only fixed metadata crosses the socket, never keystroke contents,
screen bytes, or content lengths.
