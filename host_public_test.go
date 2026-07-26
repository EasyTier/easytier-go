package host_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	corehost "github.com/EasyTier/easytier-go-host"
	"github.com/EasyTier/easytier-go-host/platform"
	"github.com/EasyTier/easytier-go-host/platform/netstd"
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
		instanceConfig(1, "10.144.0.1", 0, false, false),
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
		instanceConfig(1, "10.144.0.1", 0, false, true),
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
		instanceConfig(2, "10.144.0.2", port, true, false),
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

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("stop client: %v", err)
	}
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("stop server: %v", err)
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
}

func instanceConfig(
	id int,
	ipv4 string,
	port int,
	connect bool,
	listen bool,
) string {
	instanceID := fmt.Sprintf("00000000-0000-0000-0000-%012d", id)
	listener := ""
	peer := ""
	url := fmt.Sprintf("tcp://127.0.0.1:%d", port)
	if connect {
		peer = fmt.Sprintf("\n[[peer]]\nuri = %q\n", url)
	} else if listen {
		listener = fmt.Sprintf("listeners = [%q]\n", url)
	}
	return fmt.Sprintf(
		"instance_id = %q\ninstance_name = %q\nhostname = %q\nipv4 = %q\n%s%s\n[network_identity]\nnetwork_name = \"default\"\nnetwork_secret = \"test\"\n\n[flags]\ndisable_p2p = true\nenable_encryption = false\nbind_device = false\n",
		instanceID,
		fmt.Sprintf("go-host-%d", id),
		fmt.Sprintf("go-host-%d", id),
		ipv4+"/24",
		listener,
		peer,
	)
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
