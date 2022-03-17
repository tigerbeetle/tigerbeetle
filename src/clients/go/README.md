# tigerbeetle-go
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for Go.

This is still a **WIP** and only supports Linux + macOS.


## Local development

*Prerequisites:*
- zig
- go
- docker

```sh
./tigerbeetle/scripts/install_zig.sh
(cd tigerbeetle && ../zig/zig build -Drelease-safe && mv tigerbeetle/zig-out/bin/tigerbeetle ./tb)

zig/zig build-lib -dynamic -lc --main-pkg-path ./tigerbeetle/src ./tigerbeetle/src/c/tb_client.zig
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.

go test
```