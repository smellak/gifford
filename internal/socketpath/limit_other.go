//go:build !unix

package socketpath

// MaxBytes keeps socket diagnostics available to cross-platform commands.
// Unix listeners themselves are platform-gated elsewhere in the repository.
const MaxBytes = 107
