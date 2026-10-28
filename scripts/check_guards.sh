#!/usr/bin/env bash
# Project invariants that no linter covers. Exit 1 on any hit.
set -euo pipefail
fail=0
hit() { echo "guard violated: $1"; fail=1; }

# 1. tmux send-keys is never a transport (design decision 6).
if git grep -n -E 'send-keys' -- ':!scripts/check_guards.sh' ':!docs/**' ':!*.md' ':!test/fuzz/baseline/**' >/dev/null; then
  git grep -n -E 'send-keys' -- ':!scripts/check_guards.sh' ':!docs/**' ':!*.md' ':!test/fuzz/baseline/**'; hit "send-keys outside the fuzzer baseline"
fi
# 2. pipe-pane only with -O, never -I (design decision 46).
if git grep -n -E 'pipe-pane' -- 'internal/**' ':!*.md' | grep -v -- '-O' >/dev/null; then
  git grep -n -E 'pipe-pane' -- 'internal/**' ':!*.md' | grep -v -- '-O'; hit "pipe-pane without -O"
fi
if git grep -n -E 'pipe-pane.*-I' -- 'internal/**' ':!*.md' >/dev/null; then hit "pipe-pane with -I"; fi
# 3. Only internal/proxy/writer.go may write to the child PTY.
if git grep -n -E 'ptmx\.Write|pty\.Write\(' -- 'internal/proxy/**' | grep -v 'internal/proxy/writer.go' >/dev/null; then
  hit "PTY write outside internal/proxy/writer.go"
fi
# 4. Delivery calls only from the executors that take a channel.Decision (R46, design L.20.2).
if git grep -n -E 'PreparePTYDelivery|DeliverNativeEvent|EmitFileReady' -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/artifacts/ready.go' ':!internal/channel/**' ':!**/*_test.go' >/dev/null; then
  git grep -n -E 'PreparePTYDelivery|DeliverNativeEvent|EmitFileReady' -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/artifacts/ready.go' ':!internal/channel/**' ':!**/*_test.go'
  hit "delivery call outside the typed executors"
fi
# 5. Only internal/channel writes the control tables and control_state (R47).
if git grep -n -i -E '(insert[[:space:]]+into|update|delete[[:space:]]+from)[[:space:]]+(channel_controls|communication_loops|channel_control_decisions|approval_requests)' -- 'internal/**' ':!internal/channel/**' ':!migrations/**' >/dev/null; then
  git grep -n -i -E '(insert[[:space:]]+into|update|delete[[:space:]]+from)[[:space:]]+(channel_controls|communication_loops|channel_control_decisions|approval_requests)' -- 'internal/**' ':!internal/channel/**'
  hit "control table written outside internal/channel"
fi
if git grep -n -E '\.ControlState[[:space:]]*=([^=]|$)' -- 'internal/**' ':!internal/channel/**' ':!**/*_test.go' >/dev/null; then
  hit "control_state assigned outside internal/channel"
fi
# 6. No MCP tool that changes a control (R48, design §22.11).
if git grep -n -E '"(channel|agent_channel|link|loop|approval)\.[a-z_]+"' -- 'internal/endpoint/**' >/dev/null; then
  git grep -n -E '"(channel|agent_channel|link|loop|approval)\.[a-z_]+"' -- 'internal/endpoint/**'
  hit "control method exposed as an MCP tool"
fi
exit $fail
