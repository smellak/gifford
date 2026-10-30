# Measuring keyboard profiles with keycap

`gifford keycap` measures the bytes produced by the real terminal path and the
keyboard modes negotiated by a harness. Run it from the repository root: a
successful result replaces `profiles/<harness>.yaml`.

Install and authenticate the harness first, and make sure its `--version`
command returns a dotted version. A missing version makes the measurement
incomplete and leaves the profile unchanged.

## Required path for the profile bank

The checked-in bank runs agents inside tmux, so measure through the same path:

```text
terminal -> ssh -> tmux -> keycap -> harness
```

Allocate the SSH terminal and disable the SSH escape character:

```sh
ssh -t -e none <bank-host>
tmux attach-session -t <bank-session>
```

`-e none` matters for the multiline paste in step 15. With SSH's default
escape character, `~` at the beginning of a line is consumed as an SSH escape;
`~.` closes the connection. Do not measure the bank through a plain
`ssh -t` connection.

Before measuring, record the terminal and tmux context alongside the result:

```sh
printf 'TERM=%s\n' "$TERM"
tmux -V
tmux show-options -s extended-keys
tmux show-options -s extended-keys-format
tmux show-options -s escape-time
```

If an older tmux does not know an option, record that fact. These values are
part of the measurement, not incidental machine configuration.

## Commands

Inside that tmux pane, run the command for the profile being measured:

```sh
gifford keycap --harness claude-code -- claude
gifford keycap --harness codex -- codex
gifford keycap --harness muse-code -- muse
gifford keycap --harness opencode -- opencode
gifford keycap --harness antigravity -- agy
```

The official Muse Code executable is `muse`. The Antigravity CLI executable is
`agy`.

The `generic` profile has no vendor command. Replace
`your-interactive-command` with the real TUI executable and any fixed
arguments:

```sh
gifford keycap --harness generic -- your-interactive-command
```

The words after `--` are both the interactive command and the basis of the
off-terminal version probe: keycap appends `--version` to them before starting
the guided session.

## Preflight: prove that cancel empties a draft

In a profile, `cancel` means the single key that leaves an unsent draft empty.
It does not mean “interrupt the current agent action.” Draft Guard closes a
draft when it sees this key, so measuring an interrupt key that leaves text in
the composer would make delivery unsafe.

Before keycap, launch the same harness version normally through the same
terminal, SSH and tmux chain. With no agent action running:

1. Type visible text into the composer.
2. Press `Ctrl+C` exactly once.
3. Confirm that all composer text disappears and the harness remains open.

Perform this check with text present. In Codex and OpenCode, `Ctrl+C` on an
empty composer exits immediately. The expected draft-clearing key for all five
harnesses is `Ctrl+C`:

- Claude Code clears input on the first press when nothing is running.
- Codex clears a non-empty draft; its public shortcut reference only documents
  the empty-composer exit behavior, so the live preflight is mandatory.
- Muse Code clears input when the composer contains text.
- OpenCode binds `input_clear` to `Ctrl+C`.
- Antigravity documentation currently disagrees about whether `Ctrl+C` clears
  or exits. `Esc Esc` is two keys and keycap accepts one unit for this step, so
  use `Ctrl+C` only if the live preflight succeeds.

If no single key empties the text without closing that harness, do not measure
or commit its profile. Report the harness version and the failed preflight.

## Keys to press in the bank tmux

Use these actions for the four profile-producing steps:

| Harness | Step 1: submit | Step 2: newline | Step 3: empty draft | Step 4: clear line |
|---|---|---|---|---|
| Claude Code | Enter | Ctrl+J | Ctrl+C | Ctrl+U |
| Codex | Enter | Ctrl+J | Ctrl+C | Ctrl+U |
| Muse Code | Enter | Alt+Enter | Ctrl+C | Ctrl+U |
| OpenCode | Enter | Ctrl+J | Ctrl+C | Ctrl+U |
| Antigravity | Enter | Ctrl+J | Ctrl+C | Ctrl+U |

Muse Code 1.4.3 made `Alt+Enter` insert a newline consistently inside tmux.
For the other four harnesses, `Ctrl+J` is the tmux-safe newline action. Do not
use `Shift+Enter` with the bank's default `extended-keys off`: tmux delivers it
as `\r`, indistinguishable from Enter.

`Ctrl+U` is the expected clear-line action. It is not documented by Muse Code
or Antigravity, so note a failed check, but this field is lower risk: a wrong
`clear_line` only leaves Draft Guard dirty; it does not close the guard.

With the bank's `extended-keys off`, the outer terminal's Kitty keyboard
protocol does not reach keycap; only standard key encodings reach the pane.
In this path, Home and End arrive as `\e[1~` and `\e[4~`.

## Guided tour

At startup, the operator sees the selected profile and command, a reminder
that measured keys do not reach the harness, and the first prompt. Press one
requested key or chord only after each prompt:

| Step | Exact prompt | Operator action |
|---:|---|---|
| 1 | `Pulsa la tecla que ENVIA (Enter).` | Press Enter. |
| 2 | `Pulsa la tecla de SALTO DE LINEA dentro del borrador.` | Press the matrix key: Ctrl+J, except Alt+Enter for Muse Code. |
| 3 | `Pulsa la tecla que DEJA VACIO UN BORRADOR CON TEXTO.` | Press Ctrl+C once. The normal-session preflight already proved its meaning. |
| 4 | `Pulsa la tecla que BORRA LA LINEA.` | Press Ctrl+U. |
| 5 | `Pulsa ESCAPE.` | Press Escape. |
| 6 | `Pulsa CONTROL-C.` | Press Ctrl-C. This is a measurement step, not an abort. |
| 7 | `Pulsa RETROCESO.` | Press Backspace. |
| 8 | `Pulsa FLECHA ARRIBA.` | Press Up Arrow. |
| 9 | `Pulsa FLECHA ABAJO.` | Press Down Arrow. |
| 10 | `Pulsa FLECHA IZQUIERDA.` | Press Left Arrow. |
| 11 | `Pulsa FLECHA DERECHA.` | Press Right Arrow. |
| 12 | `Pulsa INICIO (Home).` | Press Home. |
| 13 | `Pulsa FIN (End).` | Press End. |
| 14 | `Pulsa TABULADOR.` | Press Tab. |
| 15 | `Pega un texto de varias lineas.` | Paste text containing at least one newline. |

After each accepted key, keycap prints `registrado: <step>`. For the final
paste it reports the byte count and whether bracketed-paste markers were
present. The measured keys are swallowed; harness output remains visible but
is not recorded.

If a key is not plausible for the requested step, keycap says that the step
must be repeated and prints the same prompt again. Wait for that prompt and
press only the requested key. Do not type ahead: bytes arriving together can
be interpreted as successive steps.

## Result and replacement rules

A successful tour prints the modes observed from the harness, then:

```text
keycap: perfil escrito en profiles/<harness>.yaml (measured: true).
```

The file contains:

- the harness id and detected version;
- the measured submit, insert-newline, clear-line, and cancel sequences;
- bracketed-paste and extended-keyboard mode results; and
- the existing `input_line` expression, preserved because keycap does not
  measure it. A new profile starts with `^(.*)$`.

The catch-all `input_line` is only a local placeholder. A profile with
`measured: true` must not enter `main` until its real harness prompt pattern is
validated against captured screens. At minimum, the expression must capture
the draft in exactly one group and must not match an empty line or a line made
only of spaces. For example, `^\s*>\s?(.*)$` passes that minimum; `^(.*)$`
does not. Real prompt validation belongs to the later validated-profile bank,
not to keycap.

Keycap renders and validates the complete candidate before touching the target.
It writes a temporary file in `profiles/`, sets mode `0644`, closes it, and
renames it over the target. A successful repeat therefore replaces the old
profile atomically. An interrupted, incomplete, or invalid tour leaves the old
profile byte-for-byte intact; if no profile existed, it leaves no profile or
temporary file behind.

Profiles are embedded into the Gifford binary at compile time, one profile per
harness and not one per node. After measuring and validating `input_line`,
commit the profile and build a new binary from that commit. An already-running
or previously compiled binary continues to use the old embedded profile.

To repeat a measurement, rerun the same command. The current profile remains
usable until the replacement tour finishes successfully.

## Aborting safely

Ctrl-C typed in the measured terminal is captured as tour step 6 and does not
abort keycap. From a second terminal, identify the keycap process and send it
`SIGTERM`:

```sh
pgrep -af 'gifford keycap'
kill -TERM <pid>
```

Wait for the original command to report an incomplete measurement and restore
terminal modes. The existing profile remains unchanged. Avoid `SIGKILL` unless
the terminal is disposable, because it prevents terminal cleanup.

## Which terminal path to measure

The result depends on the complete terminal path. Keycap reads the bytes that
reach its outer terminal and observes the escape sequences the harness sends
back. Terminal emulators, SSH TTY allocation, `$TERM`, tmux versions, and tmux
keyboard settings can change those bytes or negotiated modes.

| Path | Validity |
|---|---|
| Terminal → `ssh -t -e none` → bank tmux → keycap | Required for the checked-in profile bank because it reproduces the path used by bank agents. |
| Direct local terminal, outside SSH/tmux | Valid only for direct executions using that terminal configuration. It is not the bank profile. |
| SSH without tmux | Valid only for that terminal, SSH configuration, remote `$TERM`, and direct remote execution. |
| A different tmux version or configuration | Valid only for that exact tmux path. Record it separately; do not replace the bank profile blindly. |

## Command references

- [Claude Code interactive mode](https://code.claude.com/docs/en/interactive-mode)
- [Codex CLI command and shortcut reference](https://learn.chatgpt.com/docs/cli/reference)
- [Muse Code interactive mode](https://dev.meta.ai/docs/muse-code/interactive)
- [Muse Code changelog](https://dev.meta.ai/docs/muse-code/changelog)
- [OpenCode keybinds](https://opencode.ai/docs/keybinds/)
- [Google Antigravity CLI reference](https://antigravity.google/docs/cli/reference/)
- [Google Antigravity CLI settings and tips](https://antigravity.google/docs/cli/using)
- [OpenSSH `ssh(1)`](https://man.openbsd.org/OpenBSD-current/man1/ssh.1)
- [tmux `tmux(1)`](https://man.openbsd.org/OpenBSD-current/man1/tmux.1)
