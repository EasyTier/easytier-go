package host_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	corehost "github.com/EasyTier/easytier-go-host"
	"github.com/EasyTier/easytier-go-host/platform"
	"github.com/EasyTier/easytier-go-host/platform/netstd"
	hostproto "github.com/EasyTier/easytier-go-host/proto"
)

func TestPublicLifecycleDoesNotExposeWazero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := corehost.New(ctx, corehost.Options{})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)
	instance, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 1, "10.144.0.1", 0, false, false),
	)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	defer instance.Close(ctx)
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if state := instance.State(); state != corehost.StateRunning {
		t.Fatalf("state = %d, want running", state)
	}
	if err := instance.Start(ctx); err == nil {
		t.Fatal("started running instance twice")
	}
	if state := instance.State(); state != corehost.StateRunning {
		t.Fatalf("duplicate start terminated instance: state=%d", state)
	}
	if err := instance.Stop(ctx); err != nil {
		t.Fatalf("stop instance: %v", err)
	}
	if err := instance.Wait(ctx); err != nil {
		t.Fatalf("wait for instance: %v", err)
	}
}

func TestPublicFacadeConnectsTwoCoresAndExchangesPacket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sockets := &recordingSocketFactory{}
	host, err := corehost.New(ctx, corehost.Options{
		Platform: platform.Services{Sockets: sockets},
	})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)
	server, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 1, "10.144.0.1", 0, false, true),
	)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer server.Close(ctx)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	port := sockets.listenerPort(t)
	client, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 2, "10.144.0.2", port, true, false),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close(ctx)
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}

	packet := ipv4Packet(
		net.IPv4(10, 144, 0, 2),
		net.IPv4(10, 144, 0, 1),
		[]byte("public-go-host"),
	)
	var received []byte
	exchangeDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(exchangeDeadline) {
		if err := client.SendPacket(ctx, packet); err != nil {
			t.Fatalf("send packet: %v", err)
		}
		receiveContext, stopReceive := context.WithTimeout(ctx, 100*time.Millisecond)
		received, err = server.ReceivePacket(receiveContext)
		timedOut := errors.Is(err, context.DeadlineExceeded)
		stopReceive()
		if err == nil {
			break
		}
		if !timedOut {
			t.Fatalf("receive packet: %v", err)
		}
	}
	if string(received) != string(packet) {
		t.Fatalf("received packet = %x, want %x", received, packet)
	}
	event := waitForEvent(t, ctx, client.Events(), "peer_added")
	if !strings.Contains(event.Message, "PeerAdded") {
		t.Fatalf("peer event message = %q", event.Message)
	}
	peers, err := client.ListPeer(ctx)
	if err != nil {
		t.Fatalf("list connected peers: %v", err)
	}
	if len(peers) == 0 {
		t.Fatal("connected peer list is empty")
	}

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("stop client: %v", err)
	}
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("stop server: %v", err)
	}
}

func TestPublicConfigUsesCoreTCPPortForward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sockets := &recordingSocketFactory{}
	host, err := corehost.New(ctx, corehost.Options{
		Platform: platform.Services{Sockets: sockets},
	})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)

	server, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 201, "10.144.0.201", 0, false, true),
	)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer server.Close(ctx)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	underlayPort := sockets.listenerPort(t)
	overlayListener := listenTCPEventually(t, ctx, server)
	defer overlayListener.Close()
	go func() {
		for {
			connection, acceptErr := overlayListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()

	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port-forward address: %v", err)
	}
	forwardAddress := netip.MustParseAddrPort(probe.Addr().String())
	if err := probe.Close(); err != nil {
		t.Fatalf("release port-forward address: %v", err)
	}
	destination := netip.AddrPortFrom(
		netip.MustParseAddr("10.144.0.201"),
		uint16(overlayListener.Addr().(*net.TCPAddr).Port),
	)
	clientConfig, err := corehost.NewInstanceConfigBuilder("default").
		NetworkSecret("test").
		Hostname("go-host-202").
		IPv4(netip.MustParsePrefix("10.144.0.202/24")).
		AddPeers(fmt.Sprintf("tcp://127.0.0.1:%d", underlayPort)).
		AddPortForwards(corehost.PortForwardConfig{
			Protocol:    corehost.PortForwardTCP,
			Bind:        forwardAddress,
			Destination: destination,
		}).
		P2P(corehost.P2PPolicy{Disable: true}).
		Encryption(false).
		Build()
	if err != nil {
		t.Fatalf("build client config: %v", err)
	}
	client, err := host.CreateInstance(ctx, clientConfig)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close(ctx)
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}

	payload := []byte("core-owned-port-forward")
	var lastErr error
	forwardDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(forwardDeadline) {
		connection, dialErr := net.DialTimeout(
			"tcp4",
			forwardAddress.String(),
			200*time.Millisecond,
		)
		if dialErr == nil {
			_ = connection.SetDeadline(time.Now().Add(time.Second))
			_, writeErr := connection.Write(payload)
			response := make([]byte, len(payload))
			_, readErr := io.ReadFull(connection, response)
			_ = connection.Close()
			if writeErr == nil && readErr == nil &&
				string(response) == string(payload) {
				return
			}
			if writeErr == nil && readErr == nil {
				lastErr = fmt.Errorf("forwarded response = %q", response)
			} else {
				lastErr = errors.Join(writeErr, readErr)
			}
		} else {
			lastErr = dialErr
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("wait for core port forward: %v", ctx.Err())
		}
	}
	t.Fatalf("core TCP port forward did not carry traffic: %v", lastErr)
}

func TestPublicEventStreamClosesWithInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := corehost.New(ctx, corehost.Options{})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)
	instance, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 3, "10.144.0.3", 0, false, false),
	)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	events := instance.Events()
	if err := instance.Close(ctx); err != nil {
		t.Fatalf("close instance: %v", err)
	}
	select {
	case _, open := <-events:
		if open {
			t.Fatal("event stream remained open after instance close")
		}
	case <-ctx.Done():
		t.Fatal("event stream did not close with instance")
	}
}

func TestEmbeddedCoreInfoIsPublicWithoutArtifactBytes(t *testing.T) {
	info := corehost.CoreInfo()
	if len(info.EasyTierCommit) != 40 {
		t.Fatalf("EasyTier commit = %q", info.EasyTierCommit)
	}
	if len(info.SHA256) != 64 {
		t.Fatalf("EasyTier SHA-256 = %q", info.SHA256)
	}
	if info.EasyTierCommit != hostproto.EasyTierCommit {
		t.Fatalf(
			"EasyTier artifact commit %q != protobuf commit %q",
			info.EasyTierCommit,
			hostproto.EasyTierCommit,
		)
	}
	if len(hostproto.SchemaSHA256) != 64 {
		t.Fatalf("protobuf schema SHA-256 = %q", hostproto.SchemaSHA256)
	}
}

func TestPublicCreateInstanceAcceptsSecureModeConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := corehost.New(ctx, corehost.Options{})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)

	privateKey := make([]byte, 32)
	for index := range privateKey {
		privateKey[index] = byte(index + 1)
	}
	config, err := corehost.NewInstanceConfigBuilder("secure-test").
		NetworkSecret("test").
		P2P(corehost.P2PPolicy{Disable: true}).
		Encryption(false).
		SecureModeWithPrivateKey(privateKey).
		Build()
	if err != nil {
		t.Fatalf("build secure-mode config: %v", err)
	}
	instance, err := host.CreateInstance(ctx, config)
	if err != nil {
		t.Fatalf("create secure-mode instance: %v", err)
	}
	defer instance.Close(ctx)
}

func instanceConfig(
	t *testing.T,
	id int,
	ipv4 string,
	port int,
	connect bool,
	listen bool,
) corehost.InstanceConfig {
	t.Helper()
	builder := corehost.NewInstanceConfigBuilder("default").
		NetworkSecret("test").
		Hostname(fmt.Sprintf("go-host-%d", id)).
		IPv4(netip.MustParsePrefix(ipv4 + "/24")).
		P2P(corehost.P2PPolicy{Disable: true}).
		Encryption(false)
	if connect {
		builder.AddPeers(fmt.Sprintf("tcp://127.0.0.1:%d", port))
	} else if listen {
		builder.AddListeners(fmt.Sprintf("tcp://127.0.0.1:%d", port))
	}
	config, err := builder.Build()
	if err != nil {
		t.Fatalf("build instance config: %v", err)
	}
	return config
}

func waitForEvent(
	t *testing.T,
	ctx context.Context,
	events <-chan corehost.Event,
	kind string,
) corehost.Event {
	t.Helper()
	for {
		select {
		case event, open := <-events:
			if !open {
				t.Fatalf("event stream closed before %q", kind)
			}
			if event.Kind == kind {
				return event
			}
		case <-ctx.Done():
			t.Fatalf("wait for event %q: %v", kind, ctx.Err())
		}
	}
}

type recordingSocketFactory struct {
	inner netstd.SocketFactory
	mu    sync.Mutex
	port  int
}

func (factory *recordingSocketFactory) ConnectTCP(
	ctx context.Context,
	options platform.TCPConnectOptions,
) (net.Conn, error) {
	return factory.inner.ConnectTCP(ctx, options)
}

func (factory *recordingSocketFactory) BindUDP(
	ctx context.Context,
	options platform.UDPBindOptions,
) (net.PacketConn, error) {
	return factory.inner.BindUDP(ctx, options)
}

func (factory *recordingSocketFactory) ListenTCP(
	ctx context.Context,
	options platform.TCPListenOptions,
) (net.Listener, error) {
	listener, err := factory.inner.ListenTCP(ctx, options)
	if err != nil {
		return nil, err
	}
	factory.mu.Lock()
	factory.port = listener.Addr().(*net.TCPAddr).Port
	factory.mu.Unlock()
	return listener, nil
}

func (factory *recordingSocketFactory) listenerPort(t *testing.T) int {
	t.Helper()
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.port == 0 {
		t.Fatal("EasyTier did not create a TCP listener")
	}
	return factory.port
}

func ipv4Packet(source, destination net.IP, payload []byte) []byte {
	packet := make([]byte, 20+len(payload))
	packet[0] = 0x45
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	packet[8] = 64
	packet[9] = 1
	copy(packet[12:16], source.To4())
	copy(packet[16:20], destination.To4())
	copy(packet[20:], payload)
	binary.BigEndian.PutUint16(packet[10:12], ipv4Checksum(packet[:20]))
	return packet
}

func ipv4Checksum(header []byte) uint16 {
	var sum uint32
	for index := 0; index < len(header); index += 2 {
		sum += uint32(binary.BigEndian.Uint16(header[index : index+2]))
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}
