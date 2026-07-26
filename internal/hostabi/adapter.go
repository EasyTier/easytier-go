package hostabi

import (
	"context"
	"fmt"

	"github.com/EasyTier/easytier-go-host/internal/reactor"
	"github.com/tetratelabs/wazero"
)

const importModule = "easytier_host"

type Adapter struct {
	reactor *reactor.Reactor
}

func New(runtime *reactor.Reactor) (*Adapter, error) {
	if runtime == nil {
		return nil, fmt.Errorf("create host ABI adapter with nil reactor")
	}
	return &Adapter{reactor: runtime}, nil
}

func (adapter *Adapter) Instantiate(ctx context.Context, runtime wazero.Runtime) error {
	if runtime == nil {
		return fmt.Errorf("instantiate host ABI with nil wazero runtime")
	}
	_, err := runtime.NewHostModuleBuilder(importModule).
		NewFunctionBuilder().WithFunc(adapter.startRead).Export("start_read").
		NewFunctionBuilder().WithFunc(adapter.takeRead).Export("take_read").
		NewFunctionBuilder().WithFunc(adapter.startWrite).Export("start_write").
		NewFunctionBuilder().WithFunc(adapter.takeWrite).Export("take_write").
		NewFunctionBuilder().WithFunc(adapter.startUDPReceive).Export("start_udp_recv").
		NewFunctionBuilder().WithFunc(adapter.takeUDPReceive).Export("take_udp_recv").
		NewFunctionBuilder().WithFunc(adapter.tryUDPSend).Export("try_udp_send").
		NewFunctionBuilder().WithFunc(adapter.startUDPSendReady).Export("start_udp_send_ready").
		NewFunctionBuilder().WithFunc(adapter.takeUDPSendReady).Export("take_udp_send_ready").
		NewFunctionBuilder().WithFunc(adapter.startTCPConnect).Export("start_tcp_connect").
		NewFunctionBuilder().WithFunc(adapter.takeTCPConnect).Export("take_tcp_connect").
		NewFunctionBuilder().WithFunc(adapter.startUDPBind).Export("start_udp_bind").
		NewFunctionBuilder().WithFunc(adapter.takeUDPBind).Export("take_udp_bind").
		NewFunctionBuilder().WithFunc(adapter.startTCPListen).Export("start_tcp_bind").
		NewFunctionBuilder().WithFunc(adapter.takeTCPListen).Export("take_tcp_bind").
		NewFunctionBuilder().WithFunc(adapter.startTCPAccept).Export("start_tcp_accept").
		NewFunctionBuilder().WithFunc(adapter.takeTCPAccept).Export("take_tcp_accept").
		NewFunctionBuilder().WithFunc(adapter.startDNSAddress).Export("start_dns_resolve").
		NewFunctionBuilder().WithFunc(adapter.takeDNSAddress).Export("take_dns_resolve").
		NewFunctionBuilder().WithFunc(adapter.startDNSTXT).Export("start_dns_txt").
		NewFunctionBuilder().WithFunc(adapter.takeDNSTXT).Export("take_dns_txt").
		NewFunctionBuilder().WithFunc(adapter.startDNSSRV).Export("start_dns_srv").
		NewFunctionBuilder().WithFunc(adapter.takeDNSSRV).Export("take_dns_srv").
		NewFunctionBuilder().WithFunc(adapter.startLocalAddrForRemote).Export("start_local_addr_for_remote").
		NewFunctionBuilder().WithFunc(adapter.takeLocalAddrForRemote).Export("take_local_addr_for_remote").
		NewFunctionBuilder().WithFunc(adapter.tryPacketWrite).Export("try_packet_write").
		NewFunctionBuilder().WithFunc(adapter.startPacketWriteReady).Export("start_packet_write_ready").
		NewFunctionBuilder().WithFunc(adapter.takePacketWriteReady).Export("take_packet_write_ready").
		NewFunctionBuilder().WithFunc(adapter.cancelOperation).Export("cancel_operation").
		NewFunctionBuilder().WithFunc(adapter.closeHandle).Export("close").
		Instantiate(ctx)
	return err
}
