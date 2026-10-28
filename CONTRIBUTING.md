# Contributing to Gifford

Read `AGENTS.md` first. This file is the git side of the rules.

## Branches

`<role>/<subphase>-<slug>`, one branch per subphase, deleted after the merge:
`proxy/2.4-two-phase-commit`, `ml/5.6-dataset-build`. No long-lived branches.

## Commits

[Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)
with a mandatory scope equal to the package name (or `repo`, `ci`, `docs`, `ml`).
Allowed types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`,
`build`, `perf`, `revert`. Use `!` or `BREAKING CHANGE:` when a contract in
`internal/protocol` or `schemas/` changes.

- Imperative, lowercase subject, no trailing period, at most 72 characters.
- Optional body wrapped at 76 columns that explains *why*.
- Footer: `Refs #<issue>` and a `Co-authored-by:` trailer for every agent that
  wrote the change.
- No `fixup!`, no `wip`, no "misc fixes". One coherent change per commit, not
  one commit per keystroke and not one commit per day.

See `docs/commit-messages.md` for examples.

## Pull requests

- One PR per subphase, small (aim for 400 net lines or less; fuzzer corpora and
  golden files do not count).
- Fill the whole template: what and why, **how you tested it with observed
  evidence**, contracts and invariants, definition of done, authorship.
- A PR that touches a package it does not own is closed without review.
- You never merge your own PR. PRs are integrated one at a time, in dependency
  order, once the required checks are green and the maintainer has explicitly
  approved that PR (or a block of PRs named by their commits). Push and open PRs
  whenever they are ready; there is no merge schedule.

## Merging

`main` is protected: no direct pushes, PR and green CI required. Squash and
rebase merges are disabled and history is never rewritten: the per-commit
history, with each agent's co-authorship, is the dated evidence of the work.

## AI usage

Most of this project is written by AI coding agents. That is fine and expected,
but it must be visible:

- every commit written by an agent carries its `Co-authored-by` trailer;
- every PR declares who wrote it (human / agent under close direction / agent
  independently) and which agent and model;
- the author of a PR is responsible for having run what it claims to have run.

## Code of conduct

Be kind, be specific, review the code and not the person.
