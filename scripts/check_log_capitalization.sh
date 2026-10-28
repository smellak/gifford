#!/usr/bin/env bash
# Log messages start with a capital letter (REPO-RULES R16). Exit 1 on any hit.
set -euo pipefail
pattern='slog\.(Debug|Info|Warn|Error)(Context)?\((ctx, )?"[a-z]|\.(Debug|Info|Warn|Error)(Context)?\((ctx, )?"[a-z]'
if git grep -n -E "$pattern" -- '*.go' ':!**/*_test.go' >/dev/null; then
  git grep -n -E "$pattern" -- '*.go' ':!**/*_test.go'
  echo "log message must start with a capital letter"
  exit 1
fi
