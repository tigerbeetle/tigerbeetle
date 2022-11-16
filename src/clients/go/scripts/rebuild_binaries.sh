#!/usr/bin/env sh

set -e

ZIG_BIN=./zig/zig
RELEASE=-Drelease-safe

"$ZIG_BIN" build "$RELEASE" -Dtarget=x86_64-windows
"$ZIG_BIN" build tb_client "$RELEASE" -Dtarget=x86_64-windows
mv ./zig-out/tb_client.lib ../pkg/native/x86_64-windows/tb_client.lib
mv ./zig-out/bin/tigerbeetle ../pkg/native/x86_64-windows/tigerbeetle
mv ./zig-out/bin/tigerbeetle.pdb ../pkg/native/x86_64-windows/tigerbeetle.pdb

"$ZIG_BIN" build "$RELEASE" -Dtarget=x86_64-macos
"$ZIG_BIN" build tb_client "$RELEASE" -Dtarget=x86_64-macos
mv ./zig-out/libtb_client.a ../pkg/native/x86_64-macos/libtb_client.a
mv ./zig-out/bin/tigerbeetle ../pkg/native/x86_64-macos/tigerbeetle

"$ZIG_BIN" build "$RELEASE" -Dtarget=x86_64-linux
"$ZIG_BIN" build tb_client "$RELEASE" -Dtarget=x86_64-linux
mv ./zig-out/libtb_client.a ../pkg/native/x86_64-linux/libtb_client.a
mv ./zig-out/bin/tigerbeetle ../pkg/native/x86_64-linux/tigerbeetle

"$ZIG_BIN" build "$RELEASE" -Dtarget=aarch64-macos
"$ZIG_BIN" build tb_client "$RELEASE" -Dtarget=aarch64-macos
mv ./zig-out/libtb_client.a ../pkg/native/aarch64-macos/libtb_client.a
mv ./zig-out/bin/tigerbeetle ../pkg/native/aarch64-macos/tigerbeetle

"$ZIG_BIN" build "$RELEASE" -Dtarget=aarch64-linux
"$ZIG_BIN" build tb_client "$RELEASE" -Dtarget=aarch64-linux
mv ./zig-out/libtb_client.a ../pkg/native/aarch64-linux/libtb_client.a
mv ./zig-out/bin/tigerbeetle ../pkg/native/aarch64-linux/tigerbeetle
