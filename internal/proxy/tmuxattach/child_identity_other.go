//go:build !linux

package tmuxattach

import "errors"

func observeChildProcess(int) (PaneChild, error) {
	return PaneChild{}, errors.New("observe child process: unsupported platform")
}
