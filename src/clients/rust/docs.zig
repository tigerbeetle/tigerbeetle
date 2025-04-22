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
    \\[![Rust Reference](todo)](https://docs.rs/tigerbeetle)
    ,

    .prerequisites =
    \\* todo.
    ,

    .project_file_name = "Cargo.toml",
    .project_file =
    \\ todo
    ,

    .test_file_name = "main",

    .install_commands =
    \\cargo add tigerbeetle
    ,
    .run_commands = "cargo run",

    .examples = "",

    .client_object_documentation = "",

    .create_accounts_documentation =
    \\The `Uint128` fields like `ID`, `UserData128`, `Amount` and
    \\account balances have a few helper functions to make it easier
    \\to convert 128-bit little-endian unsigned integers between
    \\`string`, `math/big.Int`, and `[]byte`.
    \\
    \\See the type [Uint128](https://pkg.go.dev/github.com/tigerbeetle/tigerbeetle-go/pkg/types#Uint128) for more details.
    ,

    .account_flags_documentation =
    \\To toggle behavior for an account, use the `types.AccountFlags` struct
    \\to combine enum values and generate a `uint16`. Here are a
    \\few examples:
    \\
    \\* `AccountFlags{Linked: true}.ToUint16()`
    \\* `AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16()`
    \\* `AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16()`
    \\* `AccountFlags{History: true}.ToUint16()`
    ,

    .create_accounts_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createAccounts` with enum values in the
    \\`CreateAccountError` object, or you can 2) look up the error code in
    \\the `CreateAccountError` object for a human-readable string.
    ,

    .create_transfers_documentation = "",
    .create_transfers_errors_documentation = "",

    .transfer_flags_documentation =
    \\To toggle behavior for an account, use the `types.TransferFlags` struct
    \\to combine enum values and generate a `uint16`. Here are a
    \\few examples:
    \\
    \\* `TransferFlags{Linked: true}.ToUint16()`
    \\* `TransferFlags{Pending: true}.ToUint16()`
    \\* `TransferFlags{PostPendingTransfer: true}.ToUint16()`
    \\* `TransferFlags{VoidPendingTransfer: true}.ToUint16()`
    ,
};
