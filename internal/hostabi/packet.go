package hostabi

import (
	"context"

	"github.com/tetratelabs/wazero/api"
)

const maxHostPacketLen = 1024 * 1024

func (adapter *Adapter) tryPacketWrite(
	_ context.Context,
	module api.Module,
	handle uint64,
	packetPointer uint32,
	packetLength uint32,
) int32 {
	if packetLength == 0 || packetLength > maxHostPacketLen {
		return statusInvalid
	}
	packet, ok := module.Memory().Read(packetPointer, packetLength)
	if !ok {
		return statusMemory
	}
	return operationStatus(adapter.reactor.TryPacketWrite(handle, packet))
}

func (adapter *Adapter) startPacketWriteReady(
	_ context.Context,
	_ api.Module,
	handle uint64,
	operation uint64,
) int32 {
	return operationStatus(adapter.reactor.StartPacketWriteReady(handle, operation))
}

func (adapter *Adapter) takePacketWriteReady(
	_ context.Context,
	_ api.Module,
	operation uint64,
) int32 {
	return operationStatus(adapter.reactor.TakePacketWriteReady(operation))
}
