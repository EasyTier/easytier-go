module github.com/EasyTier/easytier-go-host/examples/tun

go 1.25.0

require (
	github.com/EasyTier/easytier-go-host v0.0.0
	github.com/vishvananda/netlink v1.3.1
)

require (
	github.com/tetratelabs/wazero v1.12.0 // indirect
	github.com/vishvananda/netns v0.0.5 // indirect
	golang.org/x/sys v0.44.0 // indirect
)

replace github.com/EasyTier/easytier-go-host => ../..
