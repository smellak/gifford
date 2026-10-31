# Authors

Gifford is written by AI coding agents on three different harnesses, directed by
a human maintainer. The maintainer approves the plans and makes the decisions.
Review, integration and the test sessions with real agents are carried out by
the project's agents under the maintainer's direction, and merges go through
the maintainer's GitHub account. Each commit says which harness wrote it.

| Who | Role | Commit author | Trailers |
|---|---|---|---|
| [@smellak](https://github.com/smellak) | human maintainer: approves the plans and decides; agent merges go through this account | `smellak` | — |
| Muse Code (Meta) | agent harness | [`gifford-muse-agent[bot]`](https://github.com/apps/gifford-muse-agent) | `Agent-Harness: Muse Code`, `Agent-Provider: Meta` |
| Codex (OpenAI) | agent harness | [`gifford-codex-agent[bot]`](https://github.com/apps/gifford-codex-agent) | `Agent-Harness: Codex`, `Agent-Provider: OpenAI`, `Co-authored-by: Codex <noreply@openai.com>` |
| Claude Code (Anthropic) | agent harness | [`gifford-claude-agent[bot]`](https://github.com/apps/gifford-claude-agent) | `Agent-Harness: Claude Code`, `Agent-Provider: Anthropic`, `Co-authored-by: Claude <noreply@anthropic.com>` |

The `gifford-*-agent[bot]` identities are GitHub Apps owned by this project, so
that each harness shows up in the contributors list. They are not official
accounts of Meta, OpenAI or Anthropic, and those companies do not endorse this
project. The `Co-authored-by` addresses are the ones Codex and Claude Code use
themselves. Muse Code has no published commit identity yet, so its commits name
it with the `Agent-Harness` and `Agent-Provider` trailers only.

An agent sets its identity once per clone with
`scripts/agent_identity.sh <muse|codex|claude>` (see `docs/commit-messages.md`).
CI refuses new commits with placeholder addresses
(`scripts/check_attribution.sh`).

## Earlier commits

The history is never rewritten, so the commits that existed before this rule
keep the identities they were made with: `main` up to `666f600`, and the
unmerged commits listed in `scripts/attribution_legacy.txt`. Their addresses
are placeholders that belong to no account:

| Identity in the history | Harness |
|---|---|
| `Gifford Miner 1 <miner-1@gifford.invalid>` | Muse Code (Meta) |
| `Gifford Miner 2 <miner-2@gifford.invalid>` | Muse Code (Meta) |
| `Gifford Miner 4 <miner-4@gifford.invalid>` | Codex (OpenAI) |
| `smellak`, with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` | Claude Code (Anthropic) |

The trailers `Co-authored-by: Muse Code <muse-code@example.invalid>` and
`Co-authored-by: OpenAI Codex <codex@example.invalid>` in those commits name
the harness with a placeholder address.

On `main` at `666f600` that is 125 commits written with Muse Code, 19 with
Codex and 6 with Claude Code, plus the merge commits.

GitHub's web pages do not read `.mailmap`, so they keep showing the `Gifford
Miner` identities. Git does read it: in a clone, `git shortlog -sn HEAD` counts
those commits under `gifford-muse-agent[bot]` and
`gifford-codex-agent[bot]`.

Some early commits and merge commits show a personal email address as the
author. The history is not rewritten; newer merges use the account's noreply
address. Commit messages may mention the internal role names the agents use in
this project.
