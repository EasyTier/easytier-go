package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EasyTier/easytier-go-host/internal/coreabi"
)

const maxDataPlaneTransfer = 1024 * 1024

type streamConn struct {
	instance *Instance
	resource coreabi.ResourceID
	local    netip.AddrPort
	peer     netip.AddrPort

	readMu  sync.Mutex
	writeMu sync.Mutex
	read    operationDeadline
	write   operationDeadline

	closed    atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newStreamConn(
	instance *Instance,
	result coreabi.OperationResult,
) *streamConn {
	return &streamConn{
		instance:  instance,
		resource:  result.Resource,
		local:     result.Local,
		peer:      result.Peer,
		read:      newOperationDeadline(),
		write:     newOperationDeadline(),
		closeDone: make(chan struct{}),
	}
}

func (conn *streamConn) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	conn.readMu.Lock()
	defer conn.readMu.Unlock()
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}
	maximum := len(buffer)
	if maximum > maxDataPlaneTransfer {
		maximum = maxDataPlaneTransfer
	}
	for {
		ctx, changed, cancel := conn.read.operationContext()
		timeout, err := contextTimeoutMillis(ctx)
		if err != nil {
			cancel()
			return 0, normalizeDeadlineError(err)
		}
		result, err := conn.instance.performOperation(
			ctx,
			coreabi.OperationTCPRead,
			func(
				callCtx context.Context,
				core dataPlaneCore,
			) (coreabi.OperationID, error) {
				return core.SubmitTCPRead(
					callCtx,
					conn.resource,
					uint32(maximum),
					timeout,
				)
			},
		)
		cancel()
		if deadlineChanged(changed) && errors.Is(err, context.Canceled) &&
			!conn.closed.Load() {
			continue
		}
		if conn.closed.Load() && errors.Is(err, context.Canceled) {
			return 0, net.ErrClosed
		}
		if err != nil {
			return 0, normalizeDeadlineError(err)
		}
		n := copy(buffer, result.Data)
		if n == 0 && result.EOF {
			return 0, io.EOF
		}
		return n, nil
	}
}

func (conn *streamConn) Write(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}
	written := 0
	for written < len(buffer) {
		end := written + maxDataPlaneTransfer
		if end > len(buffer) {
			end = len(buffer)
		}
		ctx, changed, cancel := conn.write.operationContext()
		timeout, err := contextTimeoutMillis(ctx)
		if err != nil {
			cancel()
			return written, normalizeDeadlineError(err)
		}
		chunk := buffer[written:end]
		result, err := conn.instance.performOperation(
			ctx,
			coreabi.OperationTCPWrite,
			func(
				callCtx context.Context,
				core dataPlaneCore,
			) (coreabi.OperationID, error) {
				return core.SubmitTCPWrite(
					callCtx,
					conn.resource,
					chunk,
					timeout,
				)
			},
		)
		cancel()
		if deadlineChanged(changed) && errors.Is(err, context.Canceled) &&
			!conn.closed.Load() {
			continue
		}
		if conn.closed.Load() && errors.Is(err, context.Canceled) {
			return written, net.ErrClosed
		}
		if err != nil {
			return written, normalizeDeadlineError(err)
		}
		if result.Length <= 0 || result.Length > len(chunk) {
			return written, io.ErrShortWrite
		}
		written += result.Length
	}
	return written, nil
}

func (conn *streamConn) Close() error {
	conn.closeOnce.Do(func() {
		conn.closed.Store(true)
		conn.read.set(time.Time{})
		conn.write.set(time.Time{})
		conn.closeErr = conn.instance.closeDataPlaneResource(conn.resource)
		close(conn.closeDone)
	})
	<-conn.closeDone
	return conn.closeErr
}

func (conn *streamConn) LocalAddr() net.Addr {
	return net.TCPAddrFromAddrPort(conn.local)
}

func (conn *streamConn) RemoteAddr() net.Addr {
	return net.TCPAddrFromAddrPort(conn.peer)
}

func (conn *streamConn) SetDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	conn.read.set(deadline)
	conn.write.set(deadline)
	return nil
}

func (conn *streamConn) SetReadDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	conn.read.set(deadline)
	return nil
}

func (conn *streamConn) SetWriteDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	conn.write.set(deadline)
	return nil
}

func normalizeDeadlineError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return os.ErrDeadlineExceeded
	}
	return err
}

func (instance *Instance) Dial(
	ctx context.Context,
	peer netip.AddrPort,
) (net.Conn, error) {
	timeout, err := contextTimeoutMillis(ctx)
	if err != nil {
		return nil, normalizeDeadlineError(err)
	}
	result, err := instance.performOperation(
		ctx,
		coreabi.OperationTCPConnect,
		func(
			callCtx context.Context,
			core dataPlaneCore,
		) (coreabi.OperationID, error) {
			return core.SubmitTCPConnect(callCtx, peer, timeout)
		},
	)
	if err != nil {
		return nil, normalizeDeadlineError(err)
	}
	return newStreamConn(instance, result), nil
}
