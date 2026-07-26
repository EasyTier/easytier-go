# EasyTier Go Host

`easytier-go-host` runs the `wasm32-wasip1` build of `easytier-core` in a
pure-Go process through wazero. EasyTier remains the source and producer of the
embedded WASM; this repository adapts Go host capabilities to the ABI exported
and imported by that artifact.

```go
import corehost "github.com/EasyTier/easytier-go-host"
```

## Public API

The public package owns wazero, standard WASI, the EasyTier host ABI, guest
driving, completion notification, and resource shutdown. Applications create a
host and instance from normal EasyTier TOML, then use standard Go network
interfaces:

```go
host, err := corehost.New(ctx, corehost.Options{})
if err != nil {
    return err
}
defer host.Close(ctx)

instance, err := host.CreateInstance(ctx, easyTierTOML)
if err != nil {
    return err
}
defer instance.Close(ctx)

if err := instance.Start(ctx); err != nil {
    return err
}
if err := instance.SendPacket(ctx, packet); err != nil {
    return err
}
received, err := instance.ReceivePacket(ctx)

listener, err := instance.Listen("tcp4", ":8080")
connection, err := instance.Dial(ctx, "tcp4", "10.144.0.2:8080")
packets, err := instance.ListenPacket("udp4", ":5353")
```

`Dial` returns `net.Conn`, `Listen` returns `net.Listener`, and `ListenPacket`
returns `net.PacketConn`. ABI v2 currently supports `tcp`, `tcp4`, `udp`, and
`udp4`; destinations must be IPv4 literals and listeners bind all overlay IPv4
addresses. These APIs are overlay-only: an absent EasyTier route is returned as
a normal network error and never falls back to the host network.

No public type exposes wazero runtimes, WebAssembly pointers, raw handles,
submit/take operations, or the cooperative `drive` loop. Each host owns one
wazero runtime, guest module, and host completion domain; each instance is
represented by an EasyTier guest handle. Per-instance drivers serialize guest
calls through the host. The engine continues driving EasyTier after `Start`
returns, calls `easytier_instance_notify_completions` before driving a host
completion, and drains bounded data-plane completion batches after each guest
turn.

The host internally wraps the TOML in EasyTier's version 14 create envelope and
adds the configured environment snapshot. Schema versions and JSON envelopes
are not application-facing APIs.

## Linux TUN example

The Linux TUN example joins an existing EasyTier network with a fixed virtual
IPv4 address. It creates and configures the TUN interface itself, then forwards
raw IPv4 packets through `SendPacket` and `ReceivePacket`:

```sh
cd examples/tun
sudo go run . \
  -p tcp://198.51.100.10:11010 \
  --network-name office \
  --network-secret secret \
  --ipv4 10.144.0.10/24
```

Repeat `-p` to configure more peers. The command creates `et-go0` with an MTU of
1380 and requires Linux, `/dev/net/tun`, and root or `CAP_NET_ADMIN`. Closing
the command also removes the non-persistent TUN interface.

## Platform capabilities

The default platform implementation uses Go's standard `net` and
`net.Resolver` packages. Applications that need netns, socket marks, device
binding, reuse policy, or custom DNS can inject capabilities through
`platform.Services`:

```go
host, err := corehost.New(ctx, corehost.Options{
    Platform: platform.Services{
        Sockets:     socketFactory,
        DNS:         dnsResolver,
        Environment: connectorEnvironment,
        Snapshot:    environmentSnapshot,
    },
})
```

`platform.SocketFactory` owns only TCP connect, UDP bind, and TCP listen
creation. Once a standard Go network resource is returned, the host runtime
owns its reads, writes, accepts, cancellation, and close path. EasyTier retains
all routing, peer admission, protocol, retry, and connection policy.

The implementation is split by responsibility:

- `platform` defines public capability ports; `platform/netstd` implements
  their portable defaults.
- `internal/reactor` owns typed asynchronous operations, resources, operation
  IDs, backpressure, and completion signals without depending on wazero.
- `internal/hostabi` implements the custom `easytier_host` imports, guest
  memory copying, wire codecs, and ABI status translation.
- `internal/coreabi` owns guest memory, the big-endian data-plane wire codec,
  ABI discovery, and typed `easytier_instance_*` /
  `easytier_data_plane_*` export calls.
- `internal/engine` composes standard WASI, both EasyTier ABI directions, the
  single-owner driver, operation cancellation, deadlines, standard Go network
  resources, and instance shutdown.
- `internal/artifact` contains only the embedded core and its provenance.

## Embedded artifact

The committed WASM lets downstream Go builds and tests run without a Rust
toolchain. Refresh it from a clean EasyTier checkout whenever the guest ABI or
core implementation changes:

```sh
EASYTIER_SOURCE=/path/to/EasyTier go generate ./...
```

Generation builds EasyTier's release `easytier_core.wasm` with the
`proxy-smoltcp-stack` and `aes-gcm` features, a fixed source path remap, and a
source-date epoch, then records the EasyTier commit and SHA-256 beside the
artifact. Tracked EasyTier changes block generation; unrelated untracked files
do not. `corehost.CoreInfo()` exposes that provenance without exposing the
artifact bytes.

The test-only socket probe is retained from
[EasyTier commit 6a3d15f](https://github.com/EasyTier/EasyTier/tree/6a3d15f8758eed759d55401ff4ed7c47021b0819/tools/wasi-socket-poc/guest);
its full commit and checksum are recorded in
`testdata/wasi_socket_guest.source`.

Run all reactor, ABI conformance, lifecycle, and two-instance network tests
with:

```sh
go test -count=1 ./...
```
