#!/bin/sh
# Keep the self-contained bootstrap's unit identical to the reviewed source.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
extracted=$(mktemp)
cleanup() {
    rm -f -- "$extracted"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

awk '
    /^    cat <<.EOF.$/ { emit = 1; next }
    emit && /^EOF$/ { exit }
    emit { print }
' "$repo_root/scripts/install.sh" > "$extracted"

if ! cmp -s "$repo_root/docs/gifford.service" "$extracted"; then
    printf '%s\n' 'embedded gifford.service differs from docs/gifford.service' >&2
    diff -u "$repo_root/docs/gifford.service" "$extracted" >&2 || true
    exit 1
fi
