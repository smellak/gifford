package tmuxattach

import "context"

// PaneStep is the durable protocol boundary reached by one notice attempt.
type PaneStep uint8

// Pane delivery steps. Begin persists step zero before any external effect;
// each Step persists its value before the corresponding external process.
const (
	PaneStepNoticeStart      PaneStep = 1
	PaneStepNoticeCheckStart PaneStep = 2
	PaneStepEnterStart       PaneStep = 3
	PaneStepResultCheckStart PaneStep = 4
)

// PaneChild is the process identity observed for a state-changing command.
// Zero means the current executor cannot expose a stable child identity; a
// lifecycle implementation leaves the durable child fields at zero.
type PaneChild struct {
	PID   int64
	Start int64
}

// PaneLifecycle is the single extension point inside the pane flock. Check is
// the cooperative ownership guard. Begin, Step, Child, and Finish are the
// durable attempt journal. Implementations must return only after their update
// is durable; any error prevents the next physical effect.
type PaneLifecycle interface {
	Check(context.Context, Target, PaneObservation) error
	Begin(context.Context, Target) error
	Step(context.Context, Target, PaneStep) error
	Child(context.Context, Target, PaneStep, PaneChild) error
	Finish(context.Context, Target, Result) error
}

type noopPaneLifecycle struct{}

func (noopPaneLifecycle) Check(context.Context, Target, PaneObservation) error { return nil }
func (noopPaneLifecycle) Begin(context.Context, Target) error                  { return nil }
func (noopPaneLifecycle) Step(context.Context, Target, PaneStep) error         { return nil }
func (noopPaneLifecycle) Child(context.Context, Target, PaneStep, PaneChild) error {
	return nil
}
func (noopPaneLifecycle) Finish(context.Context, Target, Result) error { return nil }
