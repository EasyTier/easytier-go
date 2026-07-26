//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"

	corehost "github.com/EasyTier/easytier-go-host"
	"github.com/vishvananda/netlink"
)

const (
	tunMTU         = 1380
	tunName        = "et-go0"
	tunReadWorkers = 32
)

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

func main() {
	var options options
	flag.Var(&options.peers, "p", "EasyTier peer URI; may be repeated")
	flag.StringVar(&options.networkName, "network-name", "", "EasyTier network name")
	flag.StringVar(&options.networkSecret, "network-secret", "", "EasyTier network secret")
	flag.StringVar(&options.ipv4, "ipv4", "", "fixed EasyTier IPv4 address and prefix")
	flag.Parse()

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

	instance, err := host.CreateInstance(
		ctx,
		buildConfig(options),
	)
	if err != nil {
		return fmt.Errorf("create EasyTier instance: %w", err)
	}
	defer instance.Close(context.Background())

	device, err := createTun(prefix)
	if err != nil {
		return err
	}
	defer device.Close()

	if err := instance.Start(ctx); err != nil {
		return fmt.Errorf("start EasyTier instance: %w", err)
	}
	log.Printf(
		"connected %s to EasyTier network %q as %s",
		tunName,
		options.networkName,
		prefix,
	)
	return bridgePackets(ctx, device, instance)
}

func buildConfig(options options) string {
	var config strings.Builder
	fmt.Fprintf(&config, "ipv4 = %s\n", tomlString(options.ipv4))
	fmt.Fprintf(&config, "\n[network_identity]\n")
	fmt.Fprintf(&config, "network_name = %s\n", tomlString(options.networkName))
	fmt.Fprintf(&config, "network_secret = %s\n", tomlString(options.networkSecret))
	for _, peer := range options.peers {
		fmt.Fprintf(&config, "\n[[peer]]\nuri = %s\n", tomlString(peer))
	}
	return config.String()
}

func tomlString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func createTun(prefix netip.Prefix) (*os.File, error) {
	link := &netlink.Tuntap{
		LinkAttrs:  netlink.LinkAttrs{Name: tunName},
		Mode:       netlink.TUNTAP_MODE_TUN,
		Flags:      netlink.TUNTAP_DEFAULTS | netlink.TUNTAP_NO_PI,
		NonPersist: true,
		Queues:     1,
	}
	if err := netlink.LinkAdd(link); err != nil {
		return nil, fmt.Errorf(
			"create TUN %q (requires root or CAP_NET_ADMIN): %w",
			tunName,
			err,
		)
	}
	cleanup := func() {
		for _, file := range link.Fds {
			_ = file.Close()
		}
	}
	if len(link.Fds) != 1 {
		cleanup()
		return nil, fmt.Errorf(
			"create TUN %q: received %d queues",
			tunName,
			len(link.Fds),
		)
	}
	if err := netlink.LinkSetMTU(link, tunMTU); err != nil {
		cleanup()
		return nil, fmt.Errorf("set TUN %q MTU: %w", tunName, err)
	}
	address, err := netlink.ParseAddr(prefix.String())
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("convert TUN IPv4 prefix: %w", err)
	}
	if err := netlink.AddrReplace(link, address); err != nil {
		cleanup()
		return nil, fmt.Errorf("set TUN %q IPv4 address: %w", tunName, err)
	}
	if err := netlink.LinkSetUp(link); err != nil {
		cleanup()
		return nil, fmt.Errorf("bring TUN %q up: %w", tunName, err)
	}
	return link.Fds[0], nil
}

func bridgePackets(
	ctx context.Context,
	device *os.File,
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
		if written != len(packet) {
			return io.ErrShortWrite
		}
	}
}
