package netstd

import (
	"context"
	"testing"

	"github.com/EasyTier/easytier-go-host/platform"
)

func TestDNSResolverNormalizesIPv4Literal(t *testing.T) {
	addresses, err := (DNSResolver{}).LookupIP(
		context.Background(),
		platform.DNSQuery{Host: "127.0.0.1"},
	)
	if err != nil {
		t.Fatalf("resolve IPv4 literal: %v", err)
	}
	if len(addresses) != 1 || !addresses[0].Is4() {
		t.Fatalf("resolved addresses = %v, want one canonical IPv4", addresses)
	}
}
