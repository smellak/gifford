# ADR 0001 — VT emulator

- **Status:** proposed (opened 2026-10-28; private validation pending)
- **Owner:** A-PROXY

## Context

The Proxy and capture need to reconstruct the screen of five different TUIs from
the raw byte stream, take snapshots and diffs, compute a hash that is stable
against clocks and spinners, and extract the input line.

## Options

1. `github.com/hinshun/vt10x`
2. the `vt` package of `github.com/charmbracelet/x`

## Evaluation

The mapper compared both candidates on 2026-10-29 using synthetic byte streams
only. The repository's wrapper tests use the same kind of synthetic data.

| # | Observation | `vt10x` | `charmbracelet/x/vt` v0.1.0 |
|---|---|---|---|
| 1 | Version | 2022 pseudo-version, no tags | v0.1.0, tagged 2026-10-12 |
| 2 | Licence | MIT | MIT |
| 3 | Dependency impact | one module | sixteen modules; MIT except `golang.org/x/exp` (BSD-3-Clause) |
| 4 | Initial 28-fixture corpus | 27 rows pass; `wide-cursor` fails | 27 rows pass; `csi-s-u` fails |
| 5 | Wide grapheme then column move | wrong width | correct |
| 6 | `CSI s` / `CSI u` | correct | restore missing |
| 7 | Wide grapheme at the last column | written into one cell | dropped instead of wrapped |
| 8 | Terminal replies | discarded by default | internal pipe blocks unless drained |
| 9 | Close with a reply drain | not applicable | writer-close, drain-wait, emulator-close is race-free |
| 10 | 16 MiB unterminated OSC | bounded; 3.0 s under race | bounded; 1.7 s under race |
| 11 | Synthetic 8 MiB throughput | 14.3 MiB/s | 15.0 MiB/s |
| 12 | Approximate heap per cell at 1000×1000 | 33 B | 234 B |
| 13 | Scrollback | none | must be reduced from 10,000 lines to one |
| 14 | Resize 8×3 → 4×2 → 8×3 | cuts rows, no reflow | same |
| 15 | Ambiguous-width sample | width 1 | width 1 |
| 16 | Complete combining, VS16, ZWJ and modifier clusters | one cell per code point | correct clusters |
| 17 | Those clusters split between writes | invariant | extension lost |
| 18 | Corpus under byte, random and every-offset chunking | split UTF-8 differs | split clusters differ |
| 18a | Wrapper holding only the trailing printable cluster | invariant | invariant; final controls apply immediately |
| 19 | Cursor visibility | exposed | exposed |

Through the selected wrapper, all 29 corpus fixtures pass, including cursor and
screen-mode assertions. The complete synthetic suite, chunk invariance and the
race build pass. Wrapper throughput for the 8 MiB synthetic stream is recorded
as 6.07 MB/s on linux/amd64 QEMU with `GOMAXPROCS=2`, 4 KiB writes and one
benchmark iteration.

The operational validation runner, streams and screens stay outside this
repository. Only its verdicts belong here:

| TUI | Recognisable | Idle hash stable for 30 s | Harness | Date |
|---|---|---|---|---|
| Claude Code | pending | pending | pending | pending |
| Codex | pending | pending | pending | pending |
| Muse Code | pending | pending | pending | pending |
| OpenCode | pending | pending | pending | pending |
| Antigravity (optional, non-blocking) | pending | pending | pending | pending |

## Decision

Use `github.com/charmbracelet/x/vt` v0.1.0 with a narrow wrapper. It has the
stronger wide-character and grapheme behaviour and a current tagged release.
The decision becomes accepted only after Claude Code, Codex, Muse Code and
OpenCode pass the private validation. Antigravity is optional and never blocks
acceptance.

## Consequences

- The wrapper drains and discards replies, closes the reply writer before
  waiting for that drain, limits scrollback to one line, and caps the screen at
  250,000 cells.
- It supplies empty `CSI u`, wraps a wide grapheme before the last column, and
  holds the trailing printable grapheme cluster for chunk-invariant parsing.
  `Pending` reports that held data; pending bytes never confirm an empty input
  line. The stream owner decides when to `Flush`.
- The selected direct modules are `github.com/charmbracelet/x/vt` v0.1.0 and
  `github.com/rivo/uniseg` v0.4.7, both MIT licensed. Their transitive modules
  are MIT or BSD-3-Clause licensed.
- No synthetic fidelity gap is accepted. Operational gaps require an explicit
  decision before this ADR can become accepted.
- The screen hash deliberately replaces all ASCII digits and a closed spinner
  set. This can equate semantically different counters, such as failed-test
  counts. Any normalisation change must use a new prefix after measuring that
  risk in the later data pipeline.
- If a required private TUI fails and the wrapper cannot address it, revisit
  this decision. The fallback is `vt10x` with explicit width and grapheme
  handling, under a new specification revision.
