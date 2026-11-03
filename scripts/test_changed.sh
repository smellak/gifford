#!/usr/bin/env bash
# Runs the tests of the Go packages a change touches: every package that has
# a changed file, found by walking up from each changed path to the nearest
# directory with Go files. The OpenCode plugin tests run when its files change.
# Usage: scripts/test_changed.sh <base> <head>
set -euo pipefail

base=${1:?base revision}
head=${2:?head revision}
if ! git cat-file -e "$base^{commit}" 2>/dev/null || ! git cat-file -e "$head^{commit}" 2>/dev/null; then
	echo "missing base or head revision" >&2
	exit 1
fi
from=$(git merge-base "$base" "$head")

declare -A packages=()
opencode=0
while IFS= read -r path; do
	[[ -z $path ]] && continue
	[[ $path == adapters/opencode/* ]] && opencode=1
	dir=$(dirname "$path")
	while [[ $dir != "." && $dir != "/" ]]; do
		if compgen -G "$dir/*.go" >/dev/null; then
			packages["./$dir"]=1
			break
		fi
		dir=$(dirname "$dir")
	done
done < <(git diff --name-only "$from" "$head")

if ((opencode)); then
	node --test adapters/opencode/test/*.test.js
fi
if ((${#packages[@]} == 0)); then
	echo "No Go package changed."
	exit 0
fi
mapfile -t list < <(printf '%s\n' "${!packages[@]}" | sort)
echo "Testing ${#list[@]} changed package(s):"
printf '  %s\n' "${list[@]}"
go test -timeout 20m "${list[@]}"
