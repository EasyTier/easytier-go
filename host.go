package host

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/EasyTier/easytier-go-host/internal/artifact"
	"github.com/EasyTier/easytier-go-host/internal/coreabi"
	"github.com/EasyTier/easytier-go-host/internal/engine"
	"github.com/EasyTier/easytier-go-host/platform"
	"github.com/EasyTier/easytier-go-host/platform/netstd"
)

type State int32

const (
	StateCreated State = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
)

type Options struct {
	Platform            platform.Services
	PacketQueueCapacity int
}

type EmbeddedCoreInfo struct {
	EasyTierCommit string
	SHA256         string
}

// Event is one best-effort notification emitted by an EasyTier instance.
type Event = engine.Event

type Host struct {
	engine *engine.Host
}

type Instance struct {
	engine *engine.Instance
}

func New(ctx context.Context, options Options) (*Host, error) {
	if ctx == nil {
		return nil, fmt.Errorf("create EasyTier host with nil context")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	runtime, err := engine.NewHost(
		ctx,
		engine.Options{
			Services:            mergeServices(options.Platform),
			PacketQueueCapacity: options.PacketQueueCapacity,
		},
	)
	if err != nil {
		return nil, err
	}
	return &Host{engine: runtime}, nil
}

func CoreInfo() EmbeddedCoreInfo {
	return EmbeddedCoreInfo{
		EasyTierCommit: artifact.Commit,
		SHA256:         artifact.SHA256,
	}
}

// CreateInstance creates an EasyTier instance from a built configuration.
func (host *Host) CreateInstance(
	ctx context.Context,
	config InstanceConfig,
) (*Instance, error) {
	if host == nil {
		return nil, fmt.Errorf("create instance with nil EasyTier host")
	}
	configTOML, err := encodeInstanceConfig(config)
	if err != nil {
		return nil, err
	}
	runtime, err := host.engine.CreateInstance(ctx, configTOML)
	if err != nil {
		return nil, err
	}
	return &Instance{engine: runtime}, nil
}

func (host *Host) Close(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("close EasyTier host with nil context")
	}
	return host.engine.Close(ctx)
}

func (instance *Instance) Start(ctx context.Context) error {
	if instance == nil || instance.engine == nil {
		return fmt.Errorf("start nil EasyTier instance")
	}
	return instance.engine.Start(ctx)
}

func (instance *Instance) Stop(ctx context.Context) error {
	if instance == nil || instance.engine == nil {
		return fmt.Errorf("stop nil EasyTier instance")
	}
	return instance.engine.Stop(ctx)
}

func (instance *Instance) SendPacket(ctx context.Context, packet []byte) error {
	if instance == nil || instance.engine == nil {
		return fmt.Errorf("send packet through nil EasyTier instance")
	}
	return instance.engine.SendPacket(ctx, packet)
}

func (instance *Instance) ReceivePacket(ctx context.Context) ([]byte, error) {
	if instance == nil || instance.engine == nil {
		return nil, fmt.Errorf("receive packet from nil EasyTier instance")
	}
	return instance.engine.ReceivePacket(ctx)
}

// Events returns the instance's bounded event stream.
//
// Slow consumers may miss events. The channel closes with the instance.
func (instance *Instance) Events() <-chan Event {
	if instance == nil || instance.engine == nil {
		return nil
	}
	return instance.engine.Events()
}

func (instance *Instance) Dial(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if instance == nil || instance.engine == nil {
		return nil, fmt.Errorf("dial through nil EasyTier instance")
	}
	if ctx == nil {
		return nil, fmt.Errorf("dial through EasyTier with nil context")
	}
	switch network {
	case "tcp", "tcp4", "udp", "udp4":
	default:
		return nil, net.UnknownNetworkError(network)
	}
	peer, err := parseDialAddress(address)
	if err != nil {
		return nil, err
	}
	var connection net.Conn
	switch network {
	case "tcp", "tcp4":
		connection, err = instance.engine.Dial(ctx, peer)
	case "udp", "udp4":
		connection, err = instance.engine.DialUDP(ctx, peer)
	}
	if err != nil {
		var target net.Addr = net.TCPAddrFromAddrPort(peer)
		if network == "udp" || network == "udp4" {
			target = net.UDPAddrFromAddrPort(peer)
		}
		return nil, &net.OpError{
			Op:   "dial",
			Net:  network,
			Addr: target,
			Err:  err,
		}
	}
	return connection, nil
}

func (instance *Instance) Listen(
	network string,
	address string,
) (net.Listener, error) {
	if instance == nil || instance.engine == nil {
		return nil, fmt.Errorf("listen through nil EasyTier instance")
	}
	if err := requireNetwork(network, "tcp"); err != nil {
		return nil, err
	}
	port, err := parseBindPort(address)
	if err != nil {
		return nil, err
	}
	listener, err := instance.engine.Listen(port)
	if err != nil {
		return nil, &net.OpError{
			Op:  "listen",
			Net: network,
			Addr: &net.TCPAddr{
				IP:   net.IPv4zero,
				Port: int(port),
			},
			Err: err,
		}
	}
	return listener, nil
}

func (instance *Instance) ListenPacket(
	network string,
	address string,
) (net.PacketConn, error) {
	if instance == nil || instance.engine == nil {
		return nil, fmt.Errorf("listen for packets through nil EasyTier instance")
	}
	if err := requireNetwork(network, "udp"); err != nil {
		return nil, err
	}
	port, err := parseBindPort(address)
	if err != nil {
		return nil, err
	}
	connection, err := instance.engine.ListenPacket(port)
	if err != nil {
		return nil, &net.OpError{
			Op:  "listen",
			Net: network,
			Addr: &net.UDPAddr{
				IP:   net.IPv4zero,
				Port: int(port),
			},
			Err: err,
		}
	}
	return connection, nil
}

func (instance *Instance) Wait(ctx context.Context) error {
	if instance == nil || instance.engine == nil {
		return fmt.Errorf("wait for nil EasyTier instance")
	}
	return instance.engine.Wait(ctx)
}

func (instance *Instance) State() State {
	if instance == nil || instance.engine == nil {
		return StateStopped
	}
	return mapState(instance.engine.State())
}

func (instance *Instance) Close(ctx context.Context) error {
	if instance == nil || instance.engine == nil {
		return nil
	}
	return instance.engine.Close(ctx)
}

func mergeServices(configured platform.Services) platform.Services {
	services := netstd.Services()
	if configured.Sockets != nil {
		services.Sockets = configured.Sockets
	}
	if configured.DNS != nil {
		services.DNS = configured.DNS
	}
	if configured.Environment != nil {
		services.Environment = configured.Environment
	}
	services.Snapshot = configured.Snapshot
	return services
}

func mapState(state coreabi.State) State {
	switch state {
	case coreabi.StateCreated:
		return StateCreated
	case coreabi.StateStarting:
		return StateStarting
	case coreabi.StateRunning:
		return StateRunning
	case coreabi.StateStopping:
		return StateStopping
	case coreabi.StateStopped:
		return StateStopped
	default:
		return StateStopped
	}
}

func requireNetwork(network string, protocol string) error {
	if network == protocol || network == protocol+"4" {
		return nil
	}
	return net.UnknownNetworkError(network)
}

func parseDialAddress(address string) (netip.AddrPort, error) {
	host, port, err := splitAddress(address)
	if err != nil {
		return netip.AddrPort{}, err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.Is4() {
		return netip.AddrPort{}, &net.AddrError{
			Err:  "EasyTier data plane requires an IPv4 literal",
			Addr: address,
		}
	}
	return netip.AddrPortFrom(ip, port), nil
}

func parseBindPort(address string) (uint16, error) {
	host, port, err := splitAddress(address)
	if err != nil {
		return 0, err
	}
	if host != "" {
		ip, parseErr := netip.ParseAddr(host)
		if parseErr != nil || !ip.Is4() || !ip.IsUnspecified() {
			return 0, &net.AddrError{
				Err:  "EasyTier listeners bind all overlay IPv4 addresses",
				Addr: address,
			}
		}
	}
	return port, nil
}

func splitAddress(address string) (string, uint16, error) {
	host, service, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(service, 10, 16)
	if err != nil {
		return "", 0, &net.AddrError{
			Err:  "port must be a decimal number from 0 to 65535",
			Addr: address,
		}
	}
	return host, uint16(port), nil
}
