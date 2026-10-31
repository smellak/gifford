#!/usr/bin/env bash
# Project invariants that no linter covers. Exit 1 on any hit.
set -euo pipefail
fail=0
hit() { echo "guard violated: $1"; fail=1; }

# 1. tmux send-keys is never a transport (design decision 6). The conformance
# suite may use its hexadecimal form only to simulate human terminal input.
if git grep -n -E 'send-keys' -- ':!scripts/check_guards.sh' ':!docs/**' ':!*.md' ':!test/fuzz/baseline/**' ':!test/conformance/**' >/dev/null; then
  git grep -n -E 'send-keys' -- ':!scripts/check_guards.sh' ':!docs/**' ':!*.md' ':!test/fuzz/baseline/**' ':!test/conformance/**'; hit "send-keys outside the fuzzer baseline or conformance human input"
fi
if git grep -n -E 'send-keys' -- 'test/conformance/**' | grep -v -F '[]string{"send-keys", "-H", "-t", session.target}' >/dev/null; then
  git grep -n -E 'send-keys' -- 'test/conformance/**' | grep -v -F '[]string{"send-keys", "-H", "-t", session.target}'
  hit "conformance send-keys without exact hexadecimal human input"
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
# 4. Delivery calls live only in executors that carry typed channel authority.
#    Keep both the original API-family guard and the agent-socket operation
#    guard: neither a renamed helper nor a literal AgentOp conversion may
#    bypass the executor (R46, design L.20.2).
delivery_calls='PreparePTYDelivery|DeliverNativeEvent|EmitFileReady'
if git grep -n -E "$delivery_calls" -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/artifacts/ready.go' ':!internal/channel/**' ':!**/*_test.go' >/dev/null; then
  git grep -n -E "$delivery_calls" -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/artifacts/ready.go' ':!internal/channel/**' ':!**/*_test.go'
  hit "delivery call outside the typed executors"
fi
delivery_ops='AgentOp(PrepareDelivery|CommitDelivery|CancelDelivery|NativeDelivery)|AgentOp[[:space:]]*\([[:space:]]*["`](prepare_pty_delivery|commit_pty_delivery|cancel_delivery|deliver_native_event)["`]'
if git grep -n -E "$delivery_ops" -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/protocol/**' ':!internal/agentsock/**' ':!**/*_test.go' >/dev/null; then
  git grep -n -E "$delivery_ops" -- 'internal/**' ':!internal/orchestrator/executor.go' ':!internal/protocol/**' ':!internal/agentsock/**' ':!**/*_test.go'
  hit "agent delivery operation outside internal/orchestrator/executor.go"
fi
# 5. Only internal/channel writes the control tables and control_state (R47).
control_write='(insert([[:space:]]+or[[:space:]]+(rollback|abort|fail|ignore|replace))?[[:space:]]+into|replace[[:space:]]+into|update([[:space:]]+or[[:space:]]+(rollback|abort|fail|ignore|replace))?|delete[[:space:]]+from)[[:space:]]+(channel_controls|communication_loops|channel_control_decisions|approval_requests|channel_epoch)'
if git grep -n -i -E "$control_write" -- 'internal/**' ':!internal/channel/**' ':!migrations/**' >/dev/null; then
  git grep -n -i -E "$control_write" -- 'internal/**' ':!internal/channel/**'
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
# 7. Only the session writer may write to the middle PTY. Creation,
#    handoff and close stay in session.go; every Write goes through
#    internal/proxy/session/writer.go.
if git grep -n -E '\bmidM\b' -- 'internal/proxy/session' ':!internal/proxy/session/writer.go' ':!**/*_test.go' | grep -E 'Write|writeFull' | grep -v -E 'dst: midM' >/dev/null; then
  git grep -n -E '\bmidM\b' -- 'internal/proxy/session' ':!internal/proxy/session/writer.go' ':!**/*_test.go' | grep -E 'Write|writeFull' | grep -v -E 'dst: midM'
  hit "middle PTY write outside internal/proxy/session/writer.go"
fi
exit $fail
