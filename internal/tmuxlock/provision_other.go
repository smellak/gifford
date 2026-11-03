//go:build !linux

package tmuxlock

import "errors"

// provisionPlatform is linux-only: panel files carry POSIX ACLs and lock
// semantics the other targets do not provide.
func provisionPlatform(spec ProvisionSpec, parts lockPathParts, _ validatedRecordPolicy, content panelContent) (ProvisionResult, error) {
	return ProvisionResult{}, errors.New("pane provision requires linux")
}
