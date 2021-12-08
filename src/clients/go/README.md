# tigerbeetle-go
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for Go.

This is still a WIP and only supports Linux.

At the moment, the Go client needs to be driven by an event loop. As such it exposes a `Tick` method.

## Local development

*Prerequisites:*
- zig
- go

```sh
zig build-lib  -dynamic -lc --main-pkg-path ./internal internal/client_c/client_c.zig

go run main.go
```