# Vortex Rust Driver

This implements a driver for Vortex, using the Rust client.

Run the following to test with this driver:

```
./zig/zig build clients:rust
(cd src/clients/rust && cargo build)
(cd src/testing/vortex/rust_driver && cargo build)
zig build vortex -- supervisor \
    --driver-command='./src/testing/vortex/rust_driver/target/debug/vortex-driver-rust'
```
