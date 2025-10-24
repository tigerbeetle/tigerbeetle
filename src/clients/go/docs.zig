const Docs = @import("../docs_types.zig").Docs;

pub const GoDocs = Docs{
    .directory = "go",

    .markdown_name = "go",
    .extension = "go",
    .proper_name = "Go",

    .test_source_path = "",

    .name = "tigerbeetle-go",
    .description =
    \\The TigerBeetle client for Go.
    \\
    \\[![Go Reference](https://pkg.go.dev/badge/github.com/tigerbeetle/tigerbeetle-go.svg)](https://pkg.go.dev/github.com/tigerbeetle/tigerbeetle-go)
    \\
    \\Make sure to import `github.com/tigerbeetle/tigerbeetle-go`, not
    \\this repo and subdirectory.
    ,

    .prerequisites =
    \\* Go >= 1.21
    \\
    \\**Additionally on Windows**: you must install [Zig
    \\0.14.1](https://ziglang.org/download/#release-0.14.1) and set the
    \\`CC` environment variable to `zig.exe cc`. Use the full path for
    \\`zig.exe`.
    ,

    .project_file = "",
    .project_file_name = "",

    .test_file_name = "main",

    .install_commands =
    \\go mod init tbtest
    \\go get github.com/tigerbeetle/tigerbeetle-go
    ,
    .run_commands = "go run main.go",

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

    .create_accounts_errors_documentation = "",

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
