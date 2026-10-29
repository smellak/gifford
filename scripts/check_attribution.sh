#!/usr/bin/env bash
# New commits use real addresses (AUTHORS.md). Exit 1 on any hit, or when the history needed to decide is missing.
# Usage: scripts/check_attribution.sh <base> <head>
set -euo pipefail
# main up to this commit, and the unmerged commits listed in attribution_legacy.txt,
# keep the identities they were made with. Every other commit in base..head is checked.
boundary=666f60014e03ff0b9f199302e1c005e37763f21b
legacy=$(dirname "$0")/attribution_legacy.txt
base=${1:-}
head=${2:-}
die() { echo "check_attribution: $1" >&2; exit 1; }
for rev in "$boundary" "$base" "$head"; do
  if [[ -z $rev || $rev =~ ^0+$ ]]; then die "missing base or head revision"; fi
  git cat-file -e "$rev^{commit}" 2>/dev/null || die "$rev is not in this checkout; fetch the full history"
done
[[ -r $legacy ]] || die "$legacy is missing"
placeholder() { [[ $1 == *.invalid || $1 == *'.invalid>' || $1 == *@example.* ]]; }
commits=$(git log --format='%H%x09%ae%x09%ce' "$head" "^$base" "^$boundary")
fail=0
while IFS=$'\t' read -r sha author committer; do
  if [[ -z $sha ]] || grep -qxF "$sha" "$legacy"; then continue; fi
  if placeholder "$author"; then echo "$sha: placeholder author address $author"; fail=1; fi
  if [[ $committer != "$author" ]] && placeholder "$committer"; then
    echo "$sha: placeholder committer address $committer"
    fail=1
  fi
  coauthors=$(git log -1 --format='%(trailers:key=Co-authored-by,valueonly,separator=%x0A)' "$sha")
  while IFS= read -r coauthor; do
    if [[ -n $coauthor ]] && placeholder "$coauthor"; then echo "$sha: placeholder co-author $coauthor"; fail=1; fi
  done <<<"$coauthors"
done <<<"$commits"
if ((fail)); then echo "set the agent identity with scripts/agent_identity.sh (see AUTHORS.md)"; fi
exit "$fail"
