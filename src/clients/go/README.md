# tigerbeetle-go
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for Go.

This is still a **WIP** and only supports Linux + macOS.

## Local development

*Prerequisites:*
- go
- zig (see below)

```sh
# Step 1 - get tigerbeetle
git submodule init      # load the tigerbeetle submodule
git submodule update    # git clone tigerbeetle/tb_client branch

# Step 2 - (optional) download zig
(cd ./tigerbeetle && ./scripts/install_zig.sh)

# Step 3 - build tb_client for your platform and use it in go
TB_TARGET=x86_64-linux
(cd ./tigerbeetle && zig/zig build tb_client -Drelease-safe -Dtarget=$TB_TARGET)
cp ./tigerbeetle/zig-out/libtb_client.a ./pkg/native/$TB_TARGET/libtb_client.a
cp ./tigerbeetle/src/c/tb_client.h ./pkg/native/tb_client.h

# Step 3.5 - build tigerbeetle binary for `go test`
(cd ./tigerbeetle && zig/zig build -Drelease-safe -Dtarget=$TB_TARGET)
cp ./tigerbeetle/zig-out/bin/tigerbeetle ./pkg/native/$TB_TARGET/tigerbeetle
```