# tigerbeetle-go

[TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle) client for Go.

[![Go Reference](https://pkg.go.dev/badge/github.com/tigerbeetledb/tigerbeetle-go.svg)](https://pkg.go.dev/github.com/tigerbeetledb/tigerbeetle-go)

## Basic example

See [./samples/basic](./samples/basic) for a minimal Go project
showing most features of the client.

## Development Setup

*Prerequisites:*
- go 1.17+
- zig 0.9.1

```sh
# Step 1 - Set up Zig
$ cd src/clients/go
$ ./tigerbeetle/scripts/install_zig.sh

# Step 2 - Build tb_client for your platform and use it in go
$ ./scripts/rebuild_binaries.sh

# Step 3 - Build and test tigerbeetle-go (zgo = go with CGO env setup)
$ ./zgo.sh test # on unix
$ zgo.bat test # on windows
```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/tigerbeetledb/tigerbeetle-node)
