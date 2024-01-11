const std = @import("std");
const builtin = @import("builtin");

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
    \\* Go >= 1.17
    \\
    \\**Additionally on Windows**: you must install [Zig
    \\0.11.0](https://ziglang.org/download/#release-0.11.0) and set the
    \\`CC` environment variable to `zig.exe cc`. Use the full path for
    \\`zig.exe`.
    ,

    .project_file = "",
    .project_file_name = "",

    .test_file_name = "",

    .install_sample_file =
    \\package main
    \\
    \\import _ "github.com/tigerbeetle/tigerbeetle-go"
    \\import _ "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
    \\import "fmt"
    \\
    \\func main() {
    \\	fmt.Println("Import ok!")
    \\}
    ,

    .install_prereqs = "",

    .install_commands =
    \\go mod init tbtest
    \\go get github.com/tigerbeetle/tigerbeetle-go
    ,
    .build_commands =
    \\go mod tidy
    \\go build main.go
    ,
    .run_commands = "go run main.go",

    .install_documentation = "",

    .examples = "",

    .client_object_documentation =
    \\The third argument to `NewClient` is a `uint` max concurrency
    \\setting. `256` is a good default and can increase to `8192`
    \\as you need increased throughput.
    ,

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

    .developer_setup_documentation = "",

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_sh_commands =
    \\./zig/zig build go_client -Drelease -Dconfig=production
    \\cd src/clients/go
    \\if [ "$TEST" = "true" ]; then go test; else echo "Skipping client unit tests"; fi
    ,

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_pwsh_commands =
    \\.\zig\zig build go_client -Drelease -Dconfig=production
    \\cd src\clients\go
    \\if ($env:TEST -eq 'true') { go test } else { echo "Skipping client unit test" }
    ,

    .test_main_prefix =
    \\package main
    \\
    \\import "log"
    \\import "os"
    \\
    \\import . "github.com/tigerbeetle/tigerbeetle-go"
    \\import . "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
    \\
    \\func main() {
    ,

    .test_main_suffix = "}",
};
