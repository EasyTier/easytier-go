//go:build linux

package main

import (
	"net/netip"
	"strings"
	"testing"
)

func TestOptionsValidate(t *testing.T) {
	valid := options{
		peers:         peerList{"tcp://198.51.100.10:11010"},
		networkName:   "office",
		networkSecret: "secret",
		ipv4:          "10.144.0.10/24",
	}

	tests := []struct {
		name    string
		mutate  func(*options)
		wantErr string
	}{
		{
			name:    "missing peer",
			mutate:  func(value *options) { value.peers = nil },
			wantErr: "at least one -p",
		},
		{
			name:    "missing network name",
			mutate:  func(value *options) { value.networkName = "" },
			wantErr: "--network-name",
		},
		{
			name:    "missing network secret",
			mutate:  func(value *options) { value.networkSecret = "" },
			wantErr: "--network-secret",
		},
		{
			name:    "invalid IPv4 prefix",
			mutate:  func(value *options) { value.ipv4 = "10.144.0.10" },
			wantErr: "--ipv4",
		},
		{
			name:    "IPv6 prefix",
			mutate:  func(value *options) { value.ipv4 = "fd00::10/64" },
			wantErr: "IPv4",
		},
	}

	prefix, err := valid.validate()
	if err != nil {
		t.Fatalf("validate valid options: %v", err)
	}
	if prefix != netip.MustParsePrefix("10.144.0.10/24") {
		t.Fatalf("validated prefix = %s", prefix)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			value.peers = append(peerList(nil), valid.peers...)
			test.mutate(&value)
			_, err := value.validate()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validate error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestBuildConfig(t *testing.T) {
	value := options{
		peers: peerList{
			"tcp://198.51.100.10:11010",
			"udp://203.0.113.20:11010",
		},
		networkName:   `office "blue"`,
		networkSecret: "line1\nline2",
		ipv4:          "10.144.0.10/24",
	}

	config := buildConfig(value)
	want := `ipv4 = "10.144.0.10/24"

[network_identity]
network_name = "office \"blue\""
network_secret = "line1\nline2"

[[peer]]
uri = "tcp://198.51.100.10:11010"

[[peer]]
uri = "udp://203.0.113.20:11010"
`
	if config != want {
		t.Fatalf("generated config:\n%s\nwant:\n%s", config, want)
	}
}

func TestPeerListRejectsEmptyPeer(t *testing.T) {
	var peers peerList
	if err := peers.Set(" "); err == nil {
		t.Fatal("empty peer was accepted")
	}
	if err := peers.Set("tcp://198.51.100.10:11010"); err != nil {
		t.Fatalf("set peer: %v", err)
	}
	if got := peers.String(); got != "tcp://198.51.100.10:11010" {
		t.Fatalf("peer list string = %q", got)
	}
}
