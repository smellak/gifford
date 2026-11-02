//go:build !linux

package tmuxinstance

import "errors"

// unsupportedSystem refuses everything: tmux instances need Linux /proc.
type unsupportedSystem struct{}

func newSystem() system { return unsupportedSystem{} }

func (unsupportedSystem) bootID() (string, error)    { return "", errors.ErrUnsupported }
func (unsupportedSystem) machineID() (string, error) { return "", errors.ErrUnsupported }

func (unsupportedSystem) process(int64) (procInfo, error) {
	return procInfo{}, errors.ErrUnsupported
}

func (unsupportedSystem) children(int64) ([]int64, error) { return nil, errors.ErrUnsupported }

func (unsupportedSystem) socketDir(string) (dirInfo, error) {
	return dirInfo{}, errors.ErrUnsupported
}

func (unsupportedSystem) lookupUser(string) (uint32, error) { return 0, errors.ErrUnsupported }
