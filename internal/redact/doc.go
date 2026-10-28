// Package redact is the local redactor: the Model Gateway calls it before
// anything leaves the node and the dataset tools before any build or
// export, through one API (RedactScreen for a reconstructed terminal
// screen, RedactText for free text).
//
// It masks private keys, credentials in connection URLs, Authorization
// credentials, vendor API keys and tokens, JSON Web Tokens, values of
// secret-named keys (.env lines and their config and header forms) and the
// paths a node marks as sensitive. Each masked cell becomes one U+2022.
//
// Invariants:
//   - Output has the rows (lines) of the input, each with the same CellWidth
//     under the v1 width model; masked runes become U+2022 and every other
//     byte is copied as it is.
//   - A masked span touching a cluster-forming rune (ZWJ, variation
//     selector, flag, skin tone, tag, keycap) fails with ErrUnsupportedInput.
//   - Every result carries PolicyVersion and counts per kind, never content.
//   - A failure is an error with a zero result, never an unredacted or
//     partly redacted screen. Errors carry indexes, never input text.
//   - Same input and Options, same output: no clock, randomness, I/O or
//     global state. A Redactor is immutable and safe for concurrent use.
//
// What it does not do: It does not decide what leaves the node (the
// Gateway does, only after redaction), read configuration, log or store
// anything. Detection is by fixed rules: no model, no entropy scoring.
package redact
