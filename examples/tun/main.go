package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	corehost "github.com/EasyTier/easytier-go-host"
	tun "github.com/sagernet/sing-tun"
)

const (
	tunMTU         = 1380
	tunNamePrefix  = "et-go"
	tunReadWorkers = 32
)

func main() {
	options := parseOptions()
	if _, err := options.validate(); err != nil {
		log.Printf("invalid arguments: %v", err)
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if err := run(ctx, options); err != nil {
		log.Printf("EasyTier TUN failed: %v", err)
		os.Exit(1)
	}
}

// EasyTier lifecycle

func run(ctx context.Context, options options) error {
	prefix, err := options.validate()
	if err != nil {
		return err
	}

	host, err := corehost.New(ctx, corehost.Options{})
	if err != nil {
		return fmt.Errorf("create EasyTier host: %w", err)
	}
	defer host.Close(context.Background())

	config, err := corehost.NewInstanceConfigBuilder(options.networkName).
		NetworkSecret(options.networkSecret).
		IPv4(prefix).
		AddPeers([]string(options.peers)...).
		Build()
	if err != nil {
		return fmt.Errorf("build EasyTier instance config: %w", err)
	}
	instance, err := host.CreateInstance(ctx, config)
	if err != nil {
		return fmt.Errorf("create EasyTier instance: %w", err)
	}
	defer instance.Close(context.Background())
	go logEvents(ctx, instance.Events())

	device, err := createTun(prefix)
	if err != nil {
		return err
	}
	defer device.Close()
	deviceName, err := device.Name()
	if err != nil {
		return fmt.Errorf("query TUN interface name: %w", err)
	}

	if err := instance.Start(ctx); err != nil {
		return fmt.Errorf("start EasyTier instance: %w", err)
	}
	forwarders, err := startPortForwards(
		ctx,
		instance.Dial,
		options.portForwards,
	)
	if err != nil {
		return err
	}
	defer forwarders.Close()
	log.Printf(
		"connected %s to EasyTier network %q as %s",
		deviceName,
		options.networkName,
		prefix,
	)
	return bridgePackets(ctx, device, instance)
}

func logEvents(ctx context.Context, events <-chan corehost.Event) {
	for {
		select {
		case event, open := <-events:
			if !open {
				return
			}
			log.Printf("EasyTier event [%s]: %s", event.Kind, event.Message)
		case <-ctx.Done():
			return
		}
	}
}

// Native TUN adapter

func createTun(prefix netip.Prefix) (*packetDevice, error) {
	name := tun.CalculateInterfaceName(tunNamePrefix)
	nativeTun, err := tun.New(tun.Options{
		Name:             name,
		Inet4Address:     []netip.Prefix{prefix},
		MTU:              tunMTU,
		GSO:              false,
		AutoRoute:        false,
		StrictRoute:      false,
		InterfaceMonitor: noRouteInterfaceMonitor{},
	})
	if err != nil {
		return nil, fmt.Errorf(
			"create TUN %q (requires elevated privileges): %w",
			name,
			err,
		)
	}
	device := newPacketDevice(nativeTun)
	if err := nativeTun.Start(); err != nil {
		_ = device.Close()
		return nil, fmt.Errorf("start TUN %q: %w", name, err)
	}
	return device, nil
}

type packetDevice struct {
	tun.Tun
	packetOffset int
	readMutex    sync.Mutex
	writeMutex   sync.Mutex
	readBuffer   []byte
	writeBuffer  []byte
	closeOnce    sync.Once
	closeErr     error
}

func newPacketDevice(nativeTun tun.Tun) *packetDevice {
	device := &packetDevice{Tun: nativeTun}
	if runtime.GOOS == "darwin" {
		device.packetOffset = 4
		device.readBuffer = make([]byte, 65535+device.packetOffset)
		device.writeBuffer = make([]byte, 65535+device.packetOffset)
	}
	return device
}

func (device *packetDevice) Read(packet []byte) (int, error) {
	device.readMutex.Lock()
	defer device.readMutex.Unlock()

	if device.packetOffset == 0 {
		return device.Tun.Read(packet)
	}
	if len(packet)+device.packetOffset > len(device.readBuffer) {
		return 0, io.ErrShortBuffer
	}
	frame := device.readBuffer[:len(packet)+device.packetOffset]
	length, err := device.Tun.Read(frame)
	if err != nil {
		return 0, err
	}
	if length < device.packetOffset {
		return 0, io.ErrUnexpectedEOF
	}
	length -= device.packetOffset
	copy(packet, frame[device.packetOffset:device.packetOffset+length])
	return length, nil
}

func (device *packetDevice) Write(packet []byte) (int, error) {
	device.writeMutex.Lock()
	defer device.writeMutex.Unlock()

	if device.packetOffset == 0 {
		return device.Tun.Write(packet)
	}
	if len(packet)+device.packetOffset > len(device.writeBuffer) {
		return 0, io.ErrShortBuffer
	}
	frame := device.writeBuffer[:len(packet)+device.packetOffset]
	if err := encodeNativeTunPacketHeader(
		frame[:device.packetOffset],
		packet,
	); err != nil {
		return 0, err
	}
	copy(frame[device.packetOffset:], packet)
	written, err := device.Tun.Write(frame)
	if written <= device.packetOffset {
		return 0, err
	}
	return written - device.packetOffset, err
}

func (device *packetDevice) Close() error {
	device.closeOnce.Do(func() {
		device.closeErr = device.Tun.Close()
	})
	return device.closeErr
}

func encodeNativeTunPacketHeader(header, packet []byte) error {
	if len(header) == 0 {
		return nil
	}
	if len(packet) == 0 {
		return fmt.Errorf("encode utun header: empty packet")
	}
	family := uint32(syscall.AF_INET)
	switch packet[0] >> 4 {
	case 4:
	case 6:
		family = syscall.AF_INET6
	default:
		return fmt.Errorf("unsupported IP version %d", packet[0]>>4)
	}
	binary.BigEndian.PutUint32(header, family)
	return nil
}

// AutoRoute is disabled, so sing-tun v0.8.11 only calls this method.
type noRouteInterfaceMonitor struct {
	tun.DefaultInterfaceMonitor
}

func (noRouteInterfaceMonitor) RegisterMyInterface(string) {
}

// Packet bridge

func bridgePackets(
	ctx context.Context,
	device *packetDevice,
	instance *corehost.Instance,
) error {
	pumpContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, tunReadWorkers+1)
	for range tunReadWorkers {
		go func() { results <- copyTunToEasyTier(pumpContext, device, instance) }()
	}
	go func() { results <- copyEasyTierToTun(pumpContext, instance, device) }()

	errs := []error{<-results}
	cancel()
	errs = append(errs, device.Close())
	for range tunReadWorkers {
		errs = append(errs, <-results)
	}
	return errors.Join(errs...)
}

func copyTunToEasyTier(
	ctx context.Context,
	device io.Reader,
	instance *corehost.Instance,
) error {
	packet := make([]byte, 65535)
	for {
		length, err := device.Read(packet)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read TUN packet: %w", err)
		}
		if length == 0 || packet[0]>>4 != 4 {
			continue
		}
		if err := instance.SendPacket(ctx, packet[:length]); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("send TUN packet to EasyTier: %w", err)
		}
	}
}

func copyEasyTierToTun(
	ctx context.Context,
	instance *corehost.Instance,
	device io.Writer,
) error {
	for {
		packet, err := instance.ReceivePacket(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive EasyTier packet: %w", err)
		}
		written, err := device.Write(packet)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("write EasyTier packet to TUN: %w", err)
		}
		// Wintun reports a full send ring as (0, nil), meaning that it has
		// dropped this packet. Keep the bridge alive for subsequent packets.
		if written == 0 {
			continue
		}
		if written != len(packet) {
			return io.ErrShortWrite
		}
	}
}

// Optional port forwarding

type portForwardRule struct {
	protocol string
	bind     netip.AddrPort
	target   netip.AddrPort
}

func (rule portForwardRule) String() string {
	return fmt.Sprintf("%s://%s/%s", rule.protocol, rule.bind, rule.target)
}

type portForwardList []portForwardRule

func (rules *portForwardList) Set(value string) error {
	rule, err := parsePortForwardRule(value)
	if err == nil {
		*rules = append(*rules, rule)
	}
	return err
}

func (rules *portForwardList) String() string {
	values := make([]string, len(*rules))
	for index, rule := range *rules {
		values[index] = rule.String()
	}
	return strings.Join(values, ",")
}

func parsePortForwardRule(value string) (portForwardRule, error) {
	protocol, addresses, ok := strings.Cut(value, "://")
	if !ok || (protocol != "tcp" && protocol != "udp") {
		return portForwardRule{}, fmt.Errorf(
			"port forward %q must use tcp:// or udp://",
			value,
		)
	}
	bindText, targetText, ok := strings.Cut(addresses, "/")
	if !ok {
		return portForwardRule{}, fmt.Errorf(
			"port forward %q must contain bind/overlay-target",
			value,
		)
	}
	bind, err := netip.ParseAddrPort(bindText)
	if err != nil {
		return portForwardRule{}, fmt.Errorf("parse bind address: %w", err)
	}
	target, err := netip.ParseAddrPort(targetText)
	if err != nil {
		return portForwardRule{}, fmt.Errorf("parse target address: %w", err)
	}
	if !bind.Addr().Is4() || !target.Addr().Is4() {
		return portForwardRule{}, fmt.Errorf("port forward requires IPv4 addresses")
	}
	return portForwardRule{protocol: protocol, bind: bind, target: target}, nil
}

type overlayDialFunc func(context.Context, string, string) (net.Conn, error)

type portForwardSet struct {
	ctx       context.Context
	cancel    context.CancelFunc
	dial      overlayDialFunc
	addresses []net.Addr
	closers   []io.Closer
}

func startPortForwards(
	ctx context.Context,
	dial overlayDialFunc,
	rules []portForwardRule,
) (*portForwardSet, error) {
	if dial == nil {
		return nil, fmt.Errorf("port forward dial function is nil")
	}
	forwardContext, cancel := context.WithCancel(ctx)
	forwards := &portForwardSet{
		ctx:    forwardContext,
		cancel: cancel,
		dial:   dial,
	}
	for _, rule := range rules {
		var err error
		switch rule.protocol {
		case "tcp":
			err = forwards.startTCP(rule)
		case "udp":
			err = forwards.startUDP(rule)
		default:
			err = fmt.Errorf("unsupported protocol %q", rule.protocol)
		}
		if err != nil {
			_ = forwards.Close()
			return nil, fmt.Errorf("start port forward %s: %w", rule, err)
		}
	}
	return forwards, nil
}

func (forwards *portForwardSet) Close() error {
	forwards.cancel()
	var errs []error
	for _, closer := range forwards.closers {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}

func (forwards *portForwardSet) startTCP(rule portForwardRule) error {
	listener, err := net.ListenTCP("tcp4", net.TCPAddrFromAddrPort(rule.bind))
	if err != nil {
		return err
	}
	forwards.closers = append(forwards.closers, listener)
	forwards.addresses = append(forwards.addresses, listener.Addr())
	log.Printf("forwarding tcp://%s to %s through EasyTier", listener.Addr(), rule.target)
	go func() {
		for {
			local, err := listener.Accept()
			if err != nil {
				return
			}
			go func(local net.Conn) {
				defer local.Close()
				overlay, err := forwards.dial(
					forwards.ctx,
					"tcp4",
					rule.target.String(),
				)
				if err != nil {
					return
				}
				defer overlay.Close()
				stopClose := context.AfterFunc(forwards.ctx, func() {
					_ = local.Close()
					_ = overlay.Close()
				})
				defer stopClose()

				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(overlay, local); done <- struct{}{} }()
				go func() { _, _ = io.Copy(local, overlay); done <- struct{}{} }()
				<-done
			}(local)
		}
	}()
	return nil
}

func (forwards *portForwardSet) startUDP(rule portForwardRule) error {
	overlay, err := forwards.dial(
		forwards.ctx,
		"udp4",
		rule.target.String(),
	)
	if err != nil {
		return err
	}
	listener, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(rule.bind))
	if err != nil {
		_ = overlay.Close()
		return err
	}
	forwards.closers = append(forwards.closers, listener, overlay)
	forwards.addresses = append(forwards.addresses, listener.LocalAddr())
	log.Printf("forwarding udp://%s to %s through EasyTier", listener.LocalAddr(), rule.target)

	var lastClient atomic.Value
	go func() {
		packet := make([]byte, 65535)
		for {
			length, client, err := listener.ReadFromUDPAddrPort(packet)
			if err != nil {
				return
			}
			lastClient.Store(client)
			if written, err := overlay.Write(packet[:length]); err != nil ||
				written != length {
				return
			}
		}
	}()
	go func() {
		packet := make([]byte, 65535)
		for {
			length, err := overlay.Read(packet)
			if err != nil {
				return
			}
			client, ok := lastClient.Load().(netip.AddrPort)
			if ok {
				_, _ = listener.WriteToUDPAddrPort(packet[:length], client)
			}
		}
	}()
	return nil
}

// Command-line options

func parseOptions() options {
	var options options
	flag.Var(&options.peers, "p", "EasyTier peer URI; may be repeated")
	flag.Var(
		&options.portForwards,
		"port-forward",
		"tcp://bind/overlay-target or udp://bind/overlay-target; may be repeated",
	)
	flag.StringVar(&options.networkName, "network-name", "", "EasyTier network name")
	flag.StringVar(&options.networkSecret, "network-secret", "", "EasyTier network secret")
	flag.StringVar(&options.ipv4, "ipv4", "", "fixed EasyTier IPv4 address and prefix")
	flag.Parse()
	return options
}

type peerList []string

func (peers *peerList) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("peer URI must not be empty")
	}
	*peers = append(*peers, value)
	return nil
}

func (peers *peerList) String() string {
	return strings.Join(*peers, ",")
}

type options struct {
	peers         peerList
	portForwards  portForwardList
	networkName   string
	networkSecret string
	ipv4          string
}

func (options options) validate() (netip.Prefix, error) {
	if len(options.peers) == 0 {
		return netip.Prefix{}, fmt.Errorf("at least one -p peer is required")
	}
	if strings.TrimSpace(options.networkName) == "" {
		return netip.Prefix{}, fmt.Errorf("--network-name is required")
	}
	if options.networkSecret == "" {
		return netip.Prefix{}, fmt.Errorf("--network-secret is required")
	}
	prefix, err := netip.ParsePrefix(options.ipv4)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("parse --ipv4: %w", err)
	}
	if !prefix.Addr().Is4() {
		return netip.Prefix{}, fmt.Errorf("--ipv4 must be an IPv4 prefix")
	}
	return prefix, nil
}
