# tigerbeetle-go
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for Go.

This is still a **WIP** and only supports Linux + macOS.


## Local development

*Prerequisites:*
- zig
- go

```sh

zig build-lib -dynamic -lc --main-pkg-path ./internal/tigerbeetle/src internal/tigerbeetle/src/c/tb_client.zig

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
go test
```