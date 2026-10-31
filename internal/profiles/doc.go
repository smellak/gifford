// Package profiles loads harness profiles: keyboard sequences measured with
// keycap plus the input-line reader and version-bound editable-area bounds.
// The YAML sources live in /profiles and are embedded.
//
// Invariants: Outside a profile's version range the generic profile is used.
// An invalid profile fails closed (ErrInvalidProfile); an unknown harness
// fails closed too (ErrUnknownHarness).
//
// What it does not do: It does not measure keys; that is the keycap tool's
// job.
package profiles
