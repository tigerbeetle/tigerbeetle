---
title: Rust
---

# tigerbeetle-rust

The TigerBeetle client for Rust.

This is a work-in-progress client for the [TigerBeetle] financial database.
It is not yet production-ready.

[TigerBeetle]: https://tigerbeetle.com

## Usage

This crate is not yet published to crates.io and must be built from
a full TigerBeetle source checkout.

Add this to your Cargo.toml

```toml
tigerbeetle.path = "path/to/tigerbeetle/src/clients/rust"
```

Then create a `Client`:

```rust
use tigerbeetle as tb;

let client = tb::Client::new(0, address)?;

let res = client.submit_create_accounts(&[
    tb::Account {
        id: 1,
        debits_pending: 0,
        debits_posted: 0,
        credits_pending: 0,
        credits_posted: 0,
        user_data_128: 0,
        user_data_64: 0,
        user_data_32: 0,
        reserved: tb::Reserved::default(),
        ledger: 2,
        code: 3,
        flags: tb::AccountFlags::History,
        timestamp: 0,
    },
]).await?;

println!("{res:?}");
```
