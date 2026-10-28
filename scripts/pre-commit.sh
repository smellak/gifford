#!/usr/bin/env bash
# Optional local pre-commit hook: scan staged changes for secrets.
# Install with: ln -s ../../scripts/pre-commit.sh .git/hooks/pre-commit
set -euo pipefail
if ! command -v gitleaks >/dev/null; then
  echo "gitleaks not found; install it before committing" >&2
  exit 1
fi
gitleaks git --staged --config .gitleaks.toml --redact --no-banner
