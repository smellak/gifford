# Commit messages

Conventional Commits 1.0.0, scope = package name (or `repo`, `ci`, `docs`, `ml`).

```text
feat(proxy): confirm draft submission from the reconstructed screen

Treat the Enter key as a submit only when the profile input line is
empty afterwards; otherwise the key inserted a newline and the draft
stays open.

Refs #42
Co-authored-by: Muse Code <muse-code@example.invalid>
```

- Subject: imperative, lowercase, no trailing period, ≤ 72 characters.
- Body: optional, wrapped at 76 columns, explains why.
- Types: `feat fix refactor test docs chore ci build perf revert`.
- `!` or `BREAKING CHANGE:` when `internal/protocol` or `schemas/` change.
- One coherent change per commit: not one per keystroke, not one per day.
- Never `fixup!`, `wip` or "misc fixes".

More examples:

- `fix(capture): refuse to attach when the pane already has a pipe`
- `test(orchestrator): cover deliverable=false guard`
- `docs(adr): record VT emulator choice`
- `chore(deps): bump modernc.org/sqlite`
