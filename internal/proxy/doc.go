// Package proxy is PTY Proxy: it runs a harness on a child PTY, forwards bytes
// untouched, parses human input, keeps the Human Draft Guard and delivers
// injected text with a two-phase commit (PRD §9.1, §9.2).
//
// Run starts the harness on a child PTY sized like the outer terminal, puts
// the outer terminal in raw mode and copies bytes both ways untouched until
// the harness exits. SIGWINCH resizes the child PTY; the other handled
// signals go to the harness's process group.
//
// Invariants: Only writer.go writes to the child PTY. Bytes cross in both
// directions unchanged and in order. The outer terminal is restored on every
// path out of Run, panics included. Every goroutine starts through spawn and
// has returned before Run does; only the loop changes session state. A
// descriptor is closed only after every goroutine using it has returned;
// only the proxy's own end of the child PTY is non-blocking. A human
// keystroke between prepare and commit aborts the delivery. No timeout ever
// closes a human draft. Time comes from internal/clock.
//
// What it does not do: It does not decide what to deliver and does not know
// the channel control plane exists (design §13.5). It does not yet parse
// input, keep the draft guard, inject, capture, persist or talk to the
// daemon; later subphases add them on this path.
package proxy
