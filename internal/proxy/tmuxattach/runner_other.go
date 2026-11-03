//go:build !linux

package tmuxattach

import "context"

type unsupportedCommandRunner struct{}

func newCommandRunner() commandRunner { return unsupportedCommandRunner{} }

func (unsupportedCommandRunner) Run(context.Context, Server, ...string) ([]byte, error) {
	return nil, ErrCommand
}

func (unsupportedCommandRunner) Start(context.Context, Server, ...string) (runningEffect, error) {
	return nil, ErrCommand
}
