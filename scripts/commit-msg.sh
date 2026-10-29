#!/usr/bin/env bash
# Local commit-msg hook: adds this clone's harness trailers and refuses placeholder addresses.
# Installed by scripts/agent_identity.sh (see AUTHORS.md).
set -euo pipefail
msg=$1
harness=$(git config --get gifford.harness || true)
provider=$(git config --get gifford.provider || true)
coauthor=$(git config --get gifford.coauthor || true)
trailers=()
if [[ -n $harness ]]; then trailers+=(--trailer "Agent-Harness: $harness"); fi
if [[ -n $provider ]]; then trailers+=(--trailer "Agent-Provider: $provider"); fi
if [[ -n $coauthor && $(git interpret-trailers --parse "$msg") != *"<${coauthor#*<}"* ]]; then
  trailers+=(--trailer "Co-authored-by: $coauthor")
fi
# Merge messages stay as git writes them: appending there glues the trailers to the subject.
if ((${#trailers[@]})) && ! git rev-parse -q --verify MERGE_HEAD >/dev/null; then
  git interpret-trailers --in-place --if-exists addIfDifferent "${trailers[@]}" "$msg"
fi
placeholder='\.invalid>|@example\.'
if [[ $(git var GIT_AUTHOR_IDENT) =~ $placeholder ]]; then
  echo "commit-msg: the author address is a placeholder; run scripts/agent_identity.sh" >&2
  exit 1
fi
while IFS= read -r line; do
  if [[ ${line,,} == co-authored-by:* && $line =~ $placeholder ]]; then
    echo "commit-msg: placeholder address in '$line'" >&2
    exit 1
  fi
done < <(git interpret-trailers --parse "$msg")
