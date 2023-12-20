const std = @import("std");
const builtin = @import("builtin");

const Docs = @import("../docs_types.zig").Docs;
const run = @import("../shutil.zig").run;
const file_or_directory_exists = @import("../shutil.zig").file_or_directory_exists;
const binary_filename = @import("../shutil.zig").binary_filename;

fn go_current_commit_pre_install_hook(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    _: []const u8,
) !void {
    for (&[_][]const u8{ "go.mod", "go.sum" }) |file| {
        const path = try std.fmt.allocPrint(
            arena.allocator(),
            "{s}/{s}",
            .{ sample_dir, file },
        );
        // TODO(Zig): Mismatching internal std.os.unlink() error in Zig 0.11.0
        const unlink_result = switch (builtin.os.tag) {
            .windows => blk: {
                const path_w = try std.os.windows.sliceToPrefixedFileW(path);
                break :blk std.os.windows.DeleteFile(path_w.span(), .{ .dir = std.fs.cwd().fd });
            },
            else => std.os.unlink(path),
        };
        unlink_result catch {
            // Delete only if they exist.
        };
    }
}

fn go_current_commit_post_install_hook(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    root: []const u8,
) !void {
    try std.os.chdir(root);
    if (!file_or_directory_exists("src/clients/go/pkg/native/x86_64-linux")) {
        try run(arena, &[_][]const u8{
            try binary_filename(arena, &[_][]const u8{ "zig", "zig" }),
            "build",
            "go_client",
        });
    }

    try std.os.chdir(sample_dir);
    const go_mod = try std.fs.cwd().openFile(
        "go.mod",
        .{ .mode = .read_write },
    );

    const file_size = try go_mod.getEndPos();
    var go_mod_contents = try arena.allocator().alloc(u8, file_size);

    _ = try go_mod.read(go_mod_contents);

    go_mod_contents = try std.mem.replaceOwned(
        u8,
        arena.allocator(),
        go_mod_contents,
        "require",
        try std.fmt.allocPrint(
            arena.allocator(),
            "replace github.com/tigerbeetle/tigerbeetle-go => {s}/src/clients/go\n\nrequire",
            .{root},
        ),
    );
    std.debug.print("go.mod:\n\n{s}\n\n", .{go_mod_contents});

    // First truncate.
    try go_mod.setEndPos(0);
    // Reset cursor.
    try go_mod.seekTo(0);
    try go_mod.writeAll(go_mod_contents);
    go_mod.close();

    try run(arena, &[_][]const u8{
        "go",
        "mod",
        "tidy",
    });
}

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

    .current_commit_install_commands_hook = null,
    .current_commit_build_commands_hook = null,
    .current_commit_run_commands_hook = null,

    .install_documentation = "",

    .examples = "",

    .client_object_example =
    \\tbAddress := os.Getenv("TB_ADDRESS")
    \\if len(tbAddress) == 0 {
    \\  tbAddress = "3000"
    \\}
    \\client, err := NewClient(ToUint128(0), []string{tbAddress}, 256)
    \\if err != nil {
    \\	log.Printf("Error creating client: %s", err)
    \\	return
    \\}
    \\defer client.Close()
    ,

    .client_object_documentation =
    \\The third argument to `NewClient` is a `uint` max concurrency
    \\setting. `256` is a good default and can increase to `8192`
    \\as you need increased throughput.
    ,

    .create_accounts_example =
    \\accountsRes, err := client.CreateAccounts([]Account{
    \\	{
    \\		ID:             ToUint128(137),
    \\		DebitsPending:  ToUint128(0),
    \\		DebitsPosted:   ToUint128(0),
    \\		CreditsPending: ToUint128(0),
    \\		CreditsPosted:  ToUint128(0),
    \\		UserData128:    ToUint128(0),
    \\		UserData64:     0,
    \\		UserData32:     0,
    \\		Reserved:       0,
    \\		Ledger:         1,
    \\		Code:           718,
    \\		Flags:          0,
    \\		Timestamp:      0,
    \\	},
    \\})
    \\if err != nil {
    \\	log.Printf("Error creating accounts: %s", err)
    \\	return
    \\}
    \\
    \\for _, err := range accountsRes {
    \\	log.Printf("Error creating account %d: %s", err.Index, err.Result)
    \\	return
    \\}
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
    .account_flags_example =
    \\account0 := Account{ /* ... account values ... */ }
    \\account1 := Account{ /* ... account values ... */ }
    \\account0.Flags = AccountFlags{Linked: true}.ToUint16()
    \\
    \\accountErrors, err := client.CreateAccounts([]Account{account0, account1})
    \\if err != nil {
    \\	log.Printf("Error creating accounts: %s", err)
    \\	return
    \\}
    ,

    .create_accounts_errors_example =
    \\account2 := Account{ /* ... account values ... */ }
    \\account3 := Account{ /* ... account values ... */ }
    \\account4 := Account{ /* ... account values ... */ }
    \\
    \\accountErrors, err = client.CreateAccounts([]Account{account2, account3, account4})
    \\if err != nil {
    \\	log.Printf("Error creating accounts: %s", err)
    \\	return
    \\}
    \\for _, err := range accountErrors {
    \\	log.Printf("Error creating account %d: %s", err.Index, err.Result)
    \\	return
    \\}
    ,

    .create_accounts_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createAccounts` with enum values in the
    \\`CreateAccountError` object, or you can 2) look up the error code in
    \\the `CreateAccountError` object for a human-readable string.
    ,
    .lookup_accounts_example =
    \\accounts, err := client.LookupAccounts([]Uint128{ToUint128(137), ToUint128(138)})
    \\if err != nil {
    \\	log.Printf("Could not fetch accounts: %s", err)
    \\	return
    \\}
    \\log.Println(accounts)
    ,

    .create_transfers_example =
    \\transfer := Transfer{
    \\	ID:              ToUint128(1),
    \\	DebitAccountID:  ToUint128(1),
    \\	CreditAccountID: ToUint128(2),
    \\	Amount:          ToUint128(10),
    \\	PendingID:       ToUint128(0),
    \\	UserData128:     ToUint128(2),
    \\	UserData64:      0,
    \\	UserData32:      0,
    \\	Timeout:         0,
    \\	Ledger:          1,
    \\	Code:            1,
    \\	Flags:           0,
    \\	Timestamp:       0,
    \\}
    \\
    \\transfersRes, err := client.CreateTransfers([]Transfer{transfer})
    \\if err != nil {
    \\	log.Printf("Error creating transfer batch: %s", err)
    \\	return
    \\}
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_example =
    \\for _, err := range transfersRes {
    \\	log.Printf("Batch transfer at %d failed to create: %s", err.Index, err.Result)
    \\	return
    \\}
    ,
    .create_transfers_errors_documentation = "",

    .no_batch_example =
    \\for i := 0; i < len(transfers); i++ {
    \\	transfersRes, err = client.CreateTransfers([]Transfer{transfers[i]})
    \\	// error handling omitted
    \\}
    ,

    .batch_example =
    \\BATCH_SIZE := 8190
    \\for i := 0; i < len(transfers); i += BATCH_SIZE {
    \\	batch := BATCH_SIZE
    \\	if i + BATCH_SIZE > len(transfers) {
    \\		batch = len(transfers) - i
    \\	}
    \\	transfersRes, err = client.CreateTransfers(transfers[i:i + batch])
    \\	// error handling omitted
    \\}
    ,

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

    .transfer_flags_link_example =
    \\transfer0 := Transfer{ /* ... account values ... */ }
    \\transfer1 := Transfer{ /* ... account values ... */ }
    \\transfer0.Flags = TransferFlags{Linked: true}.ToUint16()
    \\transfersRes, err = client.CreateTransfers([]Transfer{transfer0, transfer1})
    \\// error handling omitted
    ,
    .transfer_flags_post_example =
    \\transfer = Transfer{
    \\	ID:         ToUint128(2),
    \\	PendingID:  ToUint128(1),
    \\	Flags:      TransferFlags{PostPendingTransfer: true}.ToUint16(),
    \\	Timestamp:  0,
    \\}
    \\transfersRes, err = client.CreateTransfers([]Transfer{transfer})
    \\// error handling omitted
    ,
    .transfer_flags_void_example =
    \\transfer = Transfer{
    \\	ID:         ToUint128(2),
    \\	PendingID:  ToUint128(1),
    \\	Flags:      TransferFlags{VoidPendingTransfer: true}.ToUint16(),
    \\	Timestamp:  0,
    \\}
    \\transfersRes, err = client.CreateTransfers([]Transfer{transfer})
    \\// error handling omitted
    ,

    .lookup_transfers_example =
    \\transfers, err := client.LookupTransfers([]Uint128{ToUint128(1), ToUint128(2)})
    \\if err != nil {
    \\	log.Printf("Could not fetch transfers: %s", err)
    \\	return
    \\}
    \\log.Println(transfers)
    ,

    .get_account_transfers_example =
    \\filter := GetAccountTransfers{
    \\		AccountID: ToUint128(2),
    \\		Timestamp: 0, // No filter by Timestamp.
    \\		Limit:     10, // Limit to ten transfers at most.
    \\		Flags:     GetAccountTransfersFlags{
    \\			Debits:    true, // Include transfer from the debit side.
    \\			Credits:   true, // Include transfer from the credit side.
    \\			Reversed:  true, // Sort by timestamp in reverse-chronological order.
    \\		}.ToUint32(),
    \\}
    \\transfers, err = client.GetAccountTransfers(filter)
    \\if err != nil {
    \\	log.Printf("Could not fetch transfers: %s", err)
    \\	return
    \\}
    \\log.Println(transfers)
    ,

    .linked_events_example =
    \\batch := []Transfer{}
    \\linkedFlag := TransferFlags{Linked: true}.ToUint16()
    \\
    \\// An individual transfer (successful):
    \\batch = append(batch, Transfer{ID: ToUint128(1), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
    \\batch = append(batch, Transfer{ID: ToUint128(2), /* ... , */ Flags: linkedFlag }) // Commit/rollback.
    \\batch = append(batch, Transfer{ID: ToUint128(3), /* ... , */ Flags: linkedFlag }) // Commit/rollback.
    \\batch = append(batch, Transfer{ID: ToUint128(2), /* ... , */ Flags: linkedFlag }) // Fail with exists
    \\batch = append(batch, Transfer{ID: ToUint128(4), /* ... , */ }) // Fail without committing
    \\
    \\// An individual transfer (successful):
    \\// This should not see any effect from the failed chain above.
    \\batch = append(batch, Transfer{ID: ToUint128(2), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 2 transfers (the first transfer fails the chain):
    \\batch = append(batch, Transfer{ID: ToUint128(2), /* ... rest of transfer ... */ Flags: linkedFlag })
    \\batch = append(batch, Transfer{ID: ToUint128(3), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 2 transfers (successful):
    \\batch = append(batch, Transfer{ID: ToUint128(3), /* ... rest of transfer ... */ Flags: linkedFlag })
    \\batch = append(batch, Transfer{ID: ToUint128(4), /* ... rest of transfer ... */ })
    \\
    \\transfersRes, err = client.CreateTransfers(batch)
    ,

    .developer_setup_documentation = "",

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_sh_commands =
    \\./zig/zig build go_client -Drelease -Dconfig=production
    \\cd src/clients/go
    \\if [ "$TEST" = "true" ]; then go test; else echo "Skipping client unit tests"; fi
    ,

    .current_commit_pre_install_hook = go_current_commit_pre_install_hook,
    .current_commit_post_install_hook = go_current_commit_post_install_hook,

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
