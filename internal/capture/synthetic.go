package capture

import "context"

// Runner is the tmux surface capture drives, exported for the
// synthetic integration suite in test/capture: the suite dials its
// own private server and passes the value through the Deps structs.
// Production code keeps using it internally; nothing else implements
// it.
type Runner = tmuxRunner

// Dial returns the real runner over one tmux socket.
func Dial(socket string) Runner {
	return newRunner(socket)
}

// TestHookBeforeOpen pauses attach and reattach between the claim
// facts and the pipe open. Test-only: the synthetic suite sets it
// around one step and clears it after; unit tests never set it.
var TestHookBeforeOpen func(ctx context.Context)
