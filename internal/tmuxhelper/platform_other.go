//go:build !linux

package tmuxhelper

import "context"

type unsupportedPlatform struct{}

func newPlatform() unsupportedPlatform { return unsupportedPlatform{} }

func prepareProcess() error { return errorsUnsupported }

func (unsupportedPlatform) EffectiveUID() uint32 { return 0 }

func (unsupportedPlatform) LoadManifest() ([]byte, error) {
	return nil, errorsUnsupported
}

func (unsupportedPlatform) ValidateSocket(uint32, string) error { return errorsUnsupported }

func (unsupportedPlatform) RunTmux(context.Context, string, []string, int) ([]byte, ExitCode) {
	return nil, ExitEnvironment
}

func (unsupportedPlatform) ProcessStart(int64) (uint64, error) { return 0, errorsUnsupported }

func (unsupportedPlatform) BootID() (string, error) { return "", errorsUnsupported }

var errorsUnsupported = &unsupportedError{}

type unsupportedError struct{}

func (*unsupportedError) Error() string { return "gifford-tmux is unsupported on this platform" }
