package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EasyTier/easytier-go-host/internal/coreabi"
	"github.com/EasyTier/easytier-go-host/internal/reactor"
)

type commandKind uint8

const (
	commandStart commandKind = iota + 1
	commandStop
	commandSendPacket
)

type command struct {
	kind     commandKind
	packet   []byte
	response chan error
}

type guestCore interface {
	Start(context.Context) error
	Stop(context.Context) error
	Drive(context.Context) (coreabi.State, error)
	NotifyCompletions(context.Context) error
	NextDeadline(context.Context) (int64, error)
	SendPacket(context.Context, []byte) error
	Drop(context.Context) error
}

type Instance struct {
	host   *Host
	ctx    context.Context
	cancel context.CancelFunc

	core       guestCore
	dataPlane  dataPlaneCore
	reactor    *reactor.Reactor
	packetSink uint64

	commands          chan command
	dataPlaneCommands chan dataPlaneCommand
	pendingOperations map[coreabi.OperationID]*pendingOperation
	completions       chan struct{}
	closeRequested    chan struct{}
	closeOnce         sync.Once
	closing           atomic.Bool
	done              chan struct{}
	running           chan struct{}
	runningOnce       sync.Once
	stopped           chan struct{}
	stoppedOnce       sync.Once
	state             atomic.Int32

	errMu       sync.Mutex
	terminalErr error
}

func (instance *Instance) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("start EasyTier instance with nil context")
	}
	if err := instance.execute(ctx, command{kind: commandStart}); err != nil {
		return err
	}
	select {
	case <-instance.running:
		return nil
	case <-instance.done:
		return instance.finishedError("start EasyTier instance")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (instance *Instance) Stop(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("stop EasyTier instance with nil context")
	}
	if err := instance.execute(ctx, command{kind: commandStop}); err != nil {
		return err
	}
	select {
	case <-instance.stopped:
		return nil
	case <-instance.done:
		return instance.finishedError("stop EasyTier instance")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (instance *Instance) SendPacket(ctx context.Context, packet []byte) error {
	if ctx == nil {
		return fmt.Errorf("send EasyTier packet with nil context")
	}
	if len(packet) == 0 {
		return fmt.Errorf("send empty packet")
	}
	return instance.execute(ctx, command{
		kind:   commandSendPacket,
		packet: append([]byte(nil), packet...),
	})
}

func (instance *Instance) ReceivePacket(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("receive EasyTier packet with nil context")
	}
	select {
	case <-instance.done:
		return nil, instance.finishedError("receive EasyTier packet")
	default:
	}
	return instance.reactor.ReceivePacket(ctx, instance.packetSink)
}

func (instance *Instance) Wait(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("wait for EasyTier instance with nil context")
	}
	select {
	case <-instance.stopped:
		return nil
	case <-instance.done:
		return instance.finishedError("wait for EasyTier instance")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (instance *Instance) State() coreabi.State {
	return coreabi.State(instance.state.Load())
}

func (instance *Instance) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("close EasyTier instance with nil context")
	}
	instance.closeOnce.Do(func() {
		instance.closing.Store(true)
		close(instance.closeRequested)
	})
	select {
	case <-instance.done:
		return instance.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (instance *Instance) execute(ctx context.Context, request command) error {
	if ctx == nil {
		return fmt.Errorf("execute EasyTier command with nil context")
	}
	request.response = make(chan error, 1)
	if instance.closing.Load() {
		return fmt.Errorf("execute EasyTier command on closing instance")
	}
	select {
	case instance.commands <- request:
	case <-instance.done:
		return instance.finishedError("execute EasyTier command")
	case <-instance.closeRequested:
		return fmt.Errorf("execute EasyTier command on closing instance")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.response:
		return err
	case <-instance.done:
		return instance.finishedError("execute EasyTier command")
	case <-instance.closeRequested:
		return fmt.Errorf("execute EasyTier command on closing instance")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (instance *Instance) run() {
	runErr := instance.driveLoop()
	instance.failPendingOperations(net.ErrClosed)
	cleanupErr := instance.shutdown()
	instance.errMu.Lock()
	instance.terminalErr = errors.Join(runErr, cleanupErr)
	instance.errMu.Unlock()
	close(instance.done)
}

func (instance *Instance) driveLoop() error {
	deadline := int64(math.MaxInt64)
	for {
		timer, timerChannel := deadlineTimer(deadline)
		select {
		case request := <-instance.commands:
			stopTimer(timer)
			if request.kind == commandSendPacket {
				next, err := instance.handlePacketBatch(request, deadline)
				if err != nil {
					return err
				}
				deadline = next
				continue
			}
			commandErr := instance.handleCommand(request)
			if commandErr != nil {
				request.response <- commandErr
				continue
			}
			next, driveErr := instance.drive(false)
			request.response <- driveErr
			if driveErr != nil {
				return driveErr
			}
			deadline = next
		case request := <-instance.dataPlaneCommands:
			stopTimer(timer)
			response := instance.handleDataPlaneCommand(request)
			if response.err != nil {
				request.response <- response
				continue
			}
			next, driveErr := instance.drive(false)
			if driveErr != nil {
				response.err = driveErr
			}
			request.response <- response
			if driveErr != nil {
				return driveErr
			}
			deadline = next
		case <-instance.completions:
			stopTimer(timer)
			next, err := instance.drive(true)
			if err != nil {
				return err
			}
			deadline = next
		case <-timerChannel:
			next, err := instance.drive(false)
			if err != nil {
				return err
			}
			deadline = next
		case <-instance.closeRequested:
			stopTimer(timer)
			return nil
		case <-instance.ctx.Done():
			stopTimer(timer)
			return instance.ctx.Err()
		}
	}
}

func (instance *Instance) handlePacketBatch(
	first command,
	deadline int64,
) (int64, error) {
	var batch [maximumPacketIngressBatch]command
	batch[0] = first
	count := 1
	var following *command

drain:
	for count < len(batch) {
		select {
		case request := <-instance.commands:
			if request.kind != commandSendPacket {
				following = &request
				break drain
			}
			batch[count] = request
			count++
		default:
			break drain
		}
	}

	successful := false
	var sendErrors [maximumPacketIngressBatch]error
	for index := range count {
		err := instance.handleCommand(batch[index])
		sendErrors[index] = err
		successful = successful || err == nil
	}
	var driveErr error
	if successful {
		deadline, driveErr = instance.drive(false)
	}
	for index := range count {
		if sendErrors[index] != nil {
			batch[index].response <- sendErrors[index]
		} else {
			batch[index].response <- driveErr
		}
	}
	if driveErr != nil {
		return deadline, driveErr
	}

	if following == nil {
		return deadline, nil
	}
	commandErr := instance.handleCommand(*following)
	if commandErr != nil {
		following.response <- commandErr
		return deadline, nil
	}
	deadline, driveErr = instance.drive(false)
	following.response <- driveErr
	return deadline, driveErr
}

func (instance *Instance) handleCommand(request command) error {
	instance.host.guestMu.Lock()
	defer instance.host.guestMu.Unlock()
	switch request.kind {
	case commandStart:
		return instance.core.Start(instance.ctx)
	case commandStop:
		return instance.core.Stop(instance.ctx)
	case commandSendPacket:
		return instance.core.SendPacket(instance.ctx, request.packet)
	default:
		return fmt.Errorf("unknown EasyTier command %d", request.kind)
	}
}

func (instance *Instance) drive(notify bool) (int64, error) {
	instance.host.guestMu.Lock()
	defer instance.host.guestMu.Unlock()
	if notify {
		if err := instance.core.NotifyCompletions(instance.ctx); err != nil {
			return 0, err
		}
	}
	state, err := instance.core.Drive(instance.ctx)
	if err != nil {
		return 0, err
	}
	instance.state.Store(int32(state))
	if state == coreabi.StateRunning {
		instance.runningOnce.Do(func() { close(instance.running) })
	}
	if state == coreabi.StateStopped {
		instance.stoppedOnce.Do(func() { close(instance.stopped) })
	}
	moreDataPlaneCompletions, err := instance.drainDataPlaneCompletions()
	if err != nil {
		return 0, err
	}
	deadline, err := instance.core.NextDeadline(instance.ctx)
	if err != nil {
		return 0, err
	}
	if moreDataPlaneCompletions {
		return 0, nil
	}
	return deadline, nil
}

func (instance *Instance) shutdown() error {
	cleanupContext, cancel := context.WithTimeout(
		context.WithoutCancel(instance.ctx),
		5*time.Second,
	)
	defer cancel()
	instance.host.guestMu.Lock()
	dropErr := instance.core.Drop(cleanupContext)
	instance.host.guestMu.Unlock()
	instance.reactor.UnregisterPacketSink(instance.packetSink)
	instance.cancel()
	instance.host.removeInstance(instance)
	return dropErr
}

func (instance *Instance) finishedError(operation string) error {
	if err := instance.terminalError(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s: instance is closed", operation)
}

func (instance *Instance) terminalError() error {
	instance.errMu.Lock()
	defer instance.errMu.Unlock()
	return instance.terminalErr
}

func deadlineTimer(deadline int64) (*time.Timer, <-chan time.Time) {
	if deadline == math.MaxInt64 {
		return nil, nil
	}
	duration := time.Duration(deadline) * time.Millisecond
	if deadline > int64(math.MaxInt64/time.Millisecond) {
		duration = time.Duration(math.MaxInt64)
	}
	timer := time.NewTimer(duration)
	return timer, timer.C
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
