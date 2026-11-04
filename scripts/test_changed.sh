#!/usr/bin/env bash
# Runs the tests of the Go packages a change touches: every package that has
# a changed file, found by walking up from each changed path to the nearest
# directory with Go files, and skips a package with no files under the default
# build tags. The OpenCode plugin tests run when its files change.
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
mapfile -t candidates < <(printf '%s\n' "${!packages[@]}" | sort)
# A package whose files all carry build tags (conformance, probes) has
# nothing to test under the default tags; its own job runs it.
list=()
for pkg in "${candidates[@]}"; do
	if [[ -n $(go list -e -f '{{if or .GoFiles .TestGoFiles .XTestGoFiles}}ok{{end}}' "$pkg") ]]; then
		list+=("$pkg")
	else
		echo "Skipping $pkg: no files build under the default tags."
	fi
done
if ((${#list[@]} == 0)); then
	echo "No Go package changed under the default tags."
	exit 0
fi
echo "Testing ${#list[@]} changed package(s):"
printf '  %s\n' "${list[@]}"
go test -timeout 20m "${list[@]}"
