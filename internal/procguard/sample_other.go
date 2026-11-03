//go:build !linux

package procguard

import "errors"

// SampleProcess fails closed off Linux, where /proc identity is unavailable.
func SampleProcess(int) (Entry, error) { return Entry{}, errors.ErrUnsupported }

// SampleTree fails closed off Linux, where /proc identity is unavailable.
func SampleTree(int) ([]Entry, error) { return nil, errors.ErrUnsupported }
