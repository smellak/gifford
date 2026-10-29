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
- Start goroutines only through `(*session).spawn` (WaitGroup and panic recovery); `Run` returns after all of them.
- Restore the outer terminal on every path out of `Run`. A panic in a goroutine is recovered, the session torn down, then raised again.
- Close a descriptor only after every goroutine reading or writing it has returned: cancel first. Never set `O_NONBLOCK` on the outer terminal: the parent shell shares its file description. `ptmx` turns non-blocking once, after its last `Fd()`; then never call `ptmx.Fd()` or a creack/pty helper on it: `Fd()` makes it blocking again, and a blocked write cannot be canceled.
