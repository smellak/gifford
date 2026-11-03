package renotify

import (
	"context"
	"io"
	"time"
)

// Turner runs one turn of the backup loop at an injected now and reports
// when to look again. *Runner satisfies it; tests substitute a fake.
type Turner interface {
	RunOnce(ctx context.Context, now time.Time, items []Item) (time.Time, error)
}

// LoadFunc supplies the pending items for one turn. Facts arrive loaded;
// the driver never probes the pane.
type LoadFunc func(ctx context.Context) ([]Item, error)

// Clock is the time the driver waits on. It matches the node clocks.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// maxInterval caps how long the driver sleeps past a far wake so newly
// queued messages are picked up without waiting out a long schedule.
const maxInterval = time.Minute

// Driver ticks the backup loop: load, turn, sleep until the earlier of
// the reported wake and the cap. It owns no pane and no schedule.
type Driver struct {
	Turner   Turner
	Loader   LoadFunc
	Clock    Clock
	ErrW     io.Writer
	lastWake time.Time
}

// Run runs turns until ctx ends. One failing turn is reported and the
// driver waits for the next tick; stopping the context ends the loop.
func (d *Driver) Run(ctx context.Context) {
	for ctx.Err() == nil {
		d.turn(ctx)
		d.sleep(ctx)
	}
}

func (d *Driver) turn(ctx context.Context) {
	if d.Turner == nil || d.Loader == nil || d.Clock == nil {
		return
	}
	items, err := d.Loader(ctx)
	if err != nil {
		if ctx.Err() == nil && d.ErrW != nil {
			_, _ = io.WriteString(d.ErrW, "renotify loop: load: "+err.Error()+"\n")
		}
		return
	}
	next, err := d.Turner.RunOnce(ctx, d.Clock.Now(), items)
	if err != nil {
		if ctx.Err() == nil && d.ErrW != nil {
			_, _ = io.WriteString(d.ErrW, "renotify loop: turn: "+err.Error()+"\n")
		}
		return
	}
	d.lastWake = next
}

func (d *Driver) sleep(ctx context.Context) {
	wait := maxInterval
	if d.lastWake.After(d.Clock.Now()) && d.lastWake.Sub(d.Clock.Now()) < wait {
		wait = d.lastWake.Sub(d.Clock.Now())
	}
	select {
	case <-ctx.Done():
	case <-d.Clock.After(wait):
	}
}
