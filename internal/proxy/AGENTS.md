# internal/proxy — rules for agents

- Single writer: only `writer.go` writes to the child PTY. Everything else sends
  requests to the writer through a channel.
- Two-phase commit: `prepare` then `commit`. Any human keystroke between them aborts
  the delivery and leaves the human draft untouched.
- No timeout ever closes a human draft.
- Time comes from internal/clock, never `time.Now` (the fuzzer drives it).
- Nothing happens outside the main `select` loop: no goroutine mutates proxy state.
- The Proxy does not know the channel control plane exists. Control versions travel
  in `prepare` as an opaque `map[string]int64` and come back in the ACK.
- Never `tmux send-keys`.
