package reactor

import (
	"errors"
	"io"
)

type readOperation struct {
	handle uint64
	done   bool
	data   []byte
	err    error
}

type writeOperation struct {
	done bool
	err  error
}

func (reactor *Reactor) StartRead(handle, operation uint64, capacity uint32) error {
	reactor.mu.Lock()
	connection, exists := reactor.streams[handle]
	if !exists || capacity == 0 {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationRead); err != nil {
		reactor.mu.Unlock()
		return err
	}
	result := &readOperation{handle: handle}
	reactor.reads[operation] = result
	reactor.workers.Add(1)
	reactor.mu.Unlock()

	go func() {
		defer reactor.workers.Done()
		buffer := make([]byte, capacity)
		n, err := connection.Read(buffer)

		reactor.mu.Lock()
		if reactor.reads[operation] != result {
			reactor.mu.Unlock()
			return
		}
		result.data = buffer[:n]
		result.err = err
		result.done = true
		reactor.mu.Unlock()
		reactor.signalCompletion()
	}()
	return nil
}

func (reactor *Reactor) TakeRead(operation uint64) ([]byte, error) {
	reactor.mu.Lock()
	result, exists := reactor.reads[operation]
	if !exists || reactor.operations[operation] != operationRead {
		reactor.mu.Unlock()
		return nil, ErrInvalid
	}
	if !result.done {
		reactor.mu.Unlock()
		return nil, ErrPending
	}
	delete(reactor.reads, operation)
	reactor.releaseOperationLocked(operation, operationRead)
	data := append([]byte(nil), result.data...)
	err := result.err
	reactor.mu.Unlock()

	if errors.Is(err, io.EOF) {
		err = nil
	}
	return data, err
}

func (reactor *Reactor) StartWrite(handle, operation uint64, data []byte) error {
	reactor.mu.Lock()
	connection, exists := reactor.streams[handle]
	if !exists {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationWrite); err != nil {
		reactor.mu.Unlock()
		return err
	}
	result := &writeOperation{}
	reactor.writes[operation] = result
	reactor.workers.Add(1)
	reactor.mu.Unlock()

	data = append([]byte(nil), data...)
	go func() {
		defer reactor.workers.Done()
		written := 0
		var writeErr error
		for written < len(data) {
			n, err := connection.Write(data[written:])
			written += n
			if err != nil {
				writeErr = err
				break
			}
			if n == 0 {
				writeErr = io.ErrShortWrite
				break
			}
		}

		reactor.mu.Lock()
		if reactor.writes[operation] != result {
			reactor.mu.Unlock()
			return
		}
		result.err = writeErr
		result.done = true
		reactor.mu.Unlock()
		reactor.signalCompletion()
	}()
	return nil
}

func (reactor *Reactor) TakeWrite(operation uint64) error {
	reactor.mu.Lock()
	result, exists := reactor.writes[operation]
	if !exists || reactor.operations[operation] != operationWrite {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if !result.done {
		reactor.mu.Unlock()
		return ErrPending
	}
	delete(reactor.writes, operation)
	reactor.releaseOperationLocked(operation, operationWrite)
	err := result.err
	reactor.mu.Unlock()
	return err
}
