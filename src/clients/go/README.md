# tigerbeetle-go
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for Go.

This is still a **WIP** and only supports Linux.


## Local development

*Prerequisites:*
- zig
- go

```sh
zig build-lib  -dynamic -lc --main-pkg-path ./internal internal/client_c/client_c.zig

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
go test
```