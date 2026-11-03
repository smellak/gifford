// Package decide resolves the no-Glimmer delivery decision for the first
// vertical slice: controls first, then guards, then detector state.
// It is pure: callers authorize and revalidate the controls, sample the
// guards and classify the pane, and this package only orders the verdict.
package decide
