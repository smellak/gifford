// Package redacttest holds the shared seeded fixture for the redactor:
// synthetic secret values assembled at run time, the layouts they seed,
// and the loader proving every secret is substituted.
//
// Invariants: no committed file, tests and testdata included, holds a
// secret-shaped literal; values come from Value at run time and are
// deterministic for a fixed seed; template rows hold {{kind:name}} slots,
// never filled secrets; fixture content is invented, with hosts under
// example.invalid.
//
// What it does not do: production code never imports this package; it
// exists for tests of internal/redact and its two consumers only.
package redacttest
