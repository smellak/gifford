# Measuring keyboard profiles with keycap

`gifford keycap` measures the bytes produced by an interactive terminal and
the keyboard modes negotiated by a harness. Run it from the repository root:
the successful result replaces `profiles/<harness>.yaml`.

Use an interactive Unix terminal. Install and authenticate the harness first,
and make sure its `--version` command returns a dotted version. A missing
version makes the measurement incomplete and leaves the profile unchanged.

## Commands

Run the command for the profile being measured:

```sh
gifford keycap --harness claude-code -- claude
gifford keycap --harness codex -- codex
gifford keycap --harness muse-code -- muse
gifford keycap --harness opencode -- opencode
gifford keycap --harness antigravity -- agy
```

The `generic` profile has no vendor command. Replace
`your-interactive-command` with the real TUI executable and any fixed
arguments:

```sh
gifford keycap --harness generic -- your-interactive-command
```

The words after `--` are both the interactive command and the basis of the
off-terminal version probe: keycap appends `--version` to them before starting
the guided session.

## Guided tour

At startup, the operator sees the selected profile and command, a reminder
that measured keys do not reach the harness, and the first prompt. Press one
requested key or chord only after each prompt:

| Step | Exact prompt | Operator action |
|---:|---|---|
| 1 | `Pulsa la tecla que ENVIA (Enter).` | Press the key normally used to submit a prompt, usually Enter. |
| 2 | `Pulsa la tecla de SALTO DE LINEA dentro del borrador.` | Press the harness binding that inserts a newline without submitting. |
| 3 | `Pulsa la tecla que CANCELA.` | Press the harness binding that cancels the current action. |
| 4 | `Pulsa la tecla que BORRA LA LINEA.` | Press the harness binding that clears the current input line. |
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
| 15 | `Pega un texto de varias lineas.` | Use the terminal's paste operation to paste text containing at least one newline. |

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

Keycap renders and validates the complete candidate before touching the target.
It writes a temporary file in `profiles/`, sets mode `0644`, closes it, and
renames it over the target. A successful repeat therefore replaces the old
profile atomically. An interrupted, incomplete, or invalid tour leaves the old
profile byte-for-byte intact; if no profile existed, it leaves no profile or
temporary file behind.

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
keyboard settings can change those bytes or the negotiated modes.

| Path | Validity |
|---|---|
| Direct local terminal, outside tmux | Recommended for the checked-in profile bank and the bench. It measures the shortest, canonical path. |
| SSH with a TTY | Valid only for that local terminal, SSH configuration, remote `$TERM`, and harness combination. Use it when SSH is the actual deployment path, not as a universal profile. |
| Inside tmux | Valid only for that tmux version and configuration. tmux can change `$TERM`, extended-key encoding, and paste negotiation. Use it when the harness will run through the same tmux path. |

For a canonical profile, run keycap directly in the terminal that will be used
for the bench, without SSH or tmux. For an environment-specific deployment,
measure through the exact SSH/tmux path used at runtime and treat the result as
path-specific.

## Command references

- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code/cli-usage)
- [Codex terminal command](https://developers.openai.com/learn/developers-codex-plugin)
- [Muse Code](https://dev.meta.ai/docs/muse-code)
- [OpenCode CLI](https://docs.opencode.ai/docs/cli/)
- [Google Antigravity CLI](https://antigravity.google/docs/getting-started?tab=cli)
