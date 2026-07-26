package engine

import (
	"context"
	"sync"
	"time"
)

type operationDeadline struct {
	mu      sync.Mutex
	value   time.Time
	changed chan struct{}
}

func newOperationDeadline() operationDeadline {
	return operationDeadline{changed: make(chan struct{})}
}

func (deadline *operationDeadline) set(value time.Time) {
	deadline.mu.Lock()
	close(deadline.changed)
	deadline.value = value
	deadline.changed = make(chan struct{})
	deadline.mu.Unlock()
}

func (deadline *operationDeadline) operationContext() (
	context.Context,
	<-chan struct{},
	context.CancelFunc,
) {
	deadline.mu.Lock()
	value := deadline.value
	changed := deadline.changed
	deadline.mu.Unlock()

	base := context.Background()
	cancelDeadline := func() {}
	if !value.IsZero() {
		base, cancelDeadline = context.WithDeadline(base, value)
	}
	ctx, cancelChange := context.WithCancel(base)
	go func() {
		select {
		case <-changed:
			cancelChange()
		case <-ctx.Done():
		}
	}()
	return ctx, changed, func() {
		cancelChange()
		cancelDeadline()
	}
}

func deadlineChanged(changed <-chan struct{}) bool {
	select {
	case <-changed:
		return true
	default:
		return false
	}
}
