// Package profiles is loading of harness profiles (keyboard sequences measured
// with keycap, version_range, input_line); the YAML sources live in /profiles
// and are embedded.
//
// Invariants: Outside a profile's version range the generic profile is used.
//
// What it does not do: It does not measure keys; that is the keycap tool's
// job.
package profiles
