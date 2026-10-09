const Docs = @import("../docs_types.zig").Docs;

pub const RustDocs = Docs{
    .directory = "rust",

    .markdown_name = "rust",
    .extension = "rs",
    .proper_name = "Rust",

    .test_source_path = "src/",

    .name = "tigerbeetle-rust",
    .description =
    \\The TigerBeetle client for Rust.
    \\
    \\[![crates.io](https://img.shields.io/crates/v/tigerbeetle)](https://crates.io/crates/tigerbeetle)
    \\[![docs.rs](https://img.shields.io/docsrs/tigerbeetle)](https://docs.rs/tigerbeetle)
    \\
    \\The client must not be newer than the TigerBeetle cluster it connects to, so always pin exact
    \\versions: `cargo add tigerbeetle@=X.Y.Z`, where `X.Y.Z` is the version of your cluster.
    ,

    .prerequisites =
    \\* Rust 1.63+
    ,

    .project_file_name = "Cargo.toml",
    .project_file =
    \\[package]
    \\name = "tigerbeetle-test"
    \\version = "0.1.0"
    \\edition = "2021"
    \\
    \\[profile.release]
    \\# It is *strongly* recommended that Rust applications using TigerBeetle
    \\# enable overflow checks, because the nature of accounting makes overflow
    \\# errors catastrophic.
    \\#
    \\# Note that the following line enables the checks only for this crate, so
    \\# overflow checks must always be enabled in the Cargo.toml of the end application.
    \\overflow-checks = true
    ,

    .test_file_name = "main",

    .install_commands = "cargo add tigerbeetle",

    .run_commands = "cargo run",

    .examples = "",

    .client_object_documentation = "",

    .create_accounts_documentation = "",

    .account_flags_documentation =
    \\To toggle behavior for an account, use the `AccountFlags` bitflags.
    \\You can combine multiple flags using the `|` operator. Here are a
    \\few examples:
    \\
    \\* `AccountFlags::Linked`
    \\* `AccountFlags::DebitsMustNotExceedCredits`
    \\* `AccountFlags::CreditsMustNotExceedDebits`
    \\* `AccountFlags::History`
    \\* `AccountFlags::Linked | AccountFlags::History`
    ,

    .create_accounts_errors_documentation = "",

    .create_transfers_documentation =
    \\Transfers support various types including regular, pending, linked,
    \\and two-phase transfers. Use `TransferFlags` to specify behavior:
    \\
    \\```rust
    \\let transfer = Transfer {
    \\    id: tb::id(),
    \\    debit_account_id: account1_id,
    \\    credit_account_id: account2_id,
    \\    amount: 100,
    \\    ledger: 1,
    \\    code: 1,
    \\    flags: TransferFlags::Pending,
    \\    ..Default::default()
    \\};
    \\```
    \\
    \\For linked transfers, set the `Linked` flag on all transfers in the
    \\chain except the last one. If any transfer in a linked chain fails,
    \\the entire chain is rolled back.
    ,
    .create_transfers_errors_documentation = "",

    .transfer_flags_documentation =
    \\To toggle behavior for a transfer, use the `TransferFlags` bitflags.
    \\You can combine multiple flags using the `|` operator. Here are a
    \\few examples:
    \\
    \\* `TransferFlags::Linked`
    \\* `TransferFlags::Pending`
    \\* `TransferFlags::PostPendingTransfer`
    \\* `TransferFlags::VoidPendingTransfer`
    \\* `TransferFlags::Linked | TransferFlags::Pending`
    ,
};
