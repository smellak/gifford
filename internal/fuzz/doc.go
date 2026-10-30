// Package fuzz is the keyboard-versus-injection race fuzzer behind
// gifford fuzz: thousands of scenarios race a synthetic human's keyboard
// against automatic deliveries and check that the human is never typed
// over.
//
// A scenario runs the fake harness, a line editor that records every
// line it gets, under a real session on its own terminal. The human
// types a script drawn from the batch seed: lines with backspace,
// Ctrl-U, Ctrl-C, bracketed paste, and arrows and Alt keys the harness
// ignores, sent with Enter, at changing rhythms; a long pause passes on
// the session's clock, never in real time. The human writes some key
// sequences in two pieces, as a slow link cuts them: a paste start
// marker, an escape sequence, a lone escape before its key, a rune;
// notices arrive between the pieces too. An injector with no Analyst to stop it delivers notices through
// the session API, prepare then commit or cancel, at the worst instants
// the script offers, and sends back to its queue, within a budget,
// every notice the session refuses. The oracle then checks the
// harness's record against what the human meant to send and the
// notices, and the session's events for deliveries that started with
// the draft open. The baseline runs the same scenarios with no Proxy:
// the human and the injector write to the same terminal.
//
// Invariants: Scripts depend only on the seed and the index. Every
// scenario is bounded by its timeout, so a hang fails with its seed and
// index and never holds the batch. A failed scenario keeps its files;
// nothing else is left on disk.
//
// What it does not do: It does not change the Proxy, the protocol or
// the emulator; it only uses them. It never drives a real harness.
package fuzz
