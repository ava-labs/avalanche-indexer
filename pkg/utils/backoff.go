package utils

import (
	"context"
	"time"
)

// Backoff produces exponentially increasing delays, starting at Initial and
// doubling up to Max. It is not safe for concurrent use.
type Backoff struct {
	initial time.Duration
	max     time.Duration
	current time.Duration
}

// NewBackoff returns a Backoff that starts at initial and doubles, capped at max.
func NewBackoff(initial, max time.Duration) *Backoff {
	return &Backoff{initial: initial, max: max, current: initial}
}

// Next returns the current delay, then advances by doubling, capped at Max.
func (b *Backoff) Next() time.Duration {
	d := b.current
	next := b.current * 2
	if next > b.max {
		next = b.max
	}
	b.current = next
	return d
}

// Reset returns the backoff to its initial delay.
func (b *Backoff) Reset() {
	b.current = b.initial
}

// Sleep blocks for d or until ctx is cancelled, returning ctx.Err() on
// cancellation and nil once the full duration elapses. The timer is always
// stopped, so it never leaks when ctx is cancelled early.
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
