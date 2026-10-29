#!/usr/bin/env bash
# Sets this clone's commit identity to the agent's harness and installs the commit-msg hook.
# Usage: scripts/agent_identity.sh <muse|codex|claude>. The identities are listed in AUTHORS.md.
set -euo pipefail
case ${1:-} in
  muse) app=gifford-muse-agent id=349855422 harness='Muse Code' provider=Meta coauthor='' ;;
  codex) app=gifford-codex-agent id=349855993 harness=Codex provider=OpenAI coauthor='Codex <noreply@openai.com>' ;;
  claude) app=gifford-claude-agent id=349856518 harness='Claude Code' provider=Anthropic coauthor='Claude <noreply@anthropic.com>' ;;
  *) echo "usage: scripts/agent_identity.sh <muse|codex|claude>" >&2; exit 2 ;;
esac
git config --local user.name "$app[bot]"
git config --local user.email "$id+$app[bot]@users.noreply.github.com"
git config --local gifford.harness "$harness"
git config --local gifford.provider "$provider"
git config --local gifford.coauthor "$coauthor"
# A stub, not a link: each worktree runs its own copy, and a branch without the
# script says so instead of skipping the hook in silence.
marker='# gifford agent identity hook'
hook=$(git rev-parse --git-path hooks)/commit-msg
if [[ -e $hook ]] && ! grep -qF "$marker" "$hook"; then
  echo "$hook already exists; call scripts/commit-msg.sh from it by hand" >&2
  exit 1
fi
mkdir -p "$(dirname "$hook")"
cat >"$hook" <<EOF
#!/bin/sh
$marker
script="\$(git rev-parse --show-toplevel)/scripts/commit-msg.sh"
if [ -x "\$script" ]; then exec "\$script" "\$@"; fi
echo "commit-msg: \$script is not on this branch; merge main to get it" >&2
EOF
chmod +x "$hook"
echo "commits from this clone: $app[bot] ($harness, $provider)"
