const std = @import("std");

const Docs = @import("../docs_types.zig").Docs;
const run = @import("../shutil.zig").run;
const file_or_directory_exists = @import("../shutil.zig").file_or_directory_exists;
const script_filename = @import("../shutil.zig").script_filename;

fn go_current_commit_pre_install_hook(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    _: []const u8,
) !void {
    for (&[_][]const u8{ "go.mod", "go.sum" }) |file| {
        std.os.unlink(try std.fmt.allocPrint(
            arena.allocator(),
            "{s}/{s}",
            .{ sample_dir, file },
        )) catch {
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
            try script_filename(arena, &[_][]const u8{ "scripts", "build" }),
            "go_client",
        });
    }

    try std.os.chdir(sample_dir);
    const go_mod = try std.fs.cwd().openFile(
        "go.mod",
        .{ .write = true, .read = true },
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
            "replace github.com/tigerbeetledb/tigerbeetle-go => {s}/src/clients/go\n\nrequire",
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
    \\[![Go Reference](https://pkg.go.dev/badge/github.com/tigerbeetledb/tigerbeetle-go.svg)](https://pkg.go.dev/github.com/tigerbeetledb/tigerbeetle-go)
    \\
    \\Make sure to import `github.com/tigerbeetledb/tigerbeetle-go`, not
    \\this repo and subdirectory.
    ,

    .prerequisites = 
    \\* Go >= 1.17
    \\
    \\**Additionally on Windows**: you must install [Zig
    \\0.9.1](https://ziglang.org/download/#release-0.9.1) and set the
    \\`CC` environment variable to `zig.exe cc`. Use the full path for
    \\`zig.exe`.
    ,

    .project_file = "",
    .project_file_name = "",

    .test_file_name = "",

    .install_sample_file = 
    \\package main
    \\
    \\import _ "github.com/tigerbeetledb/tigerbeetle-go"
    \\import "fmt"
    \\
    \\func main() {
    \\	fmt.Println("Import ok!")
    \\}
    ,

    .install_prereqs = "",

    .install_commands = 
    \\go mod init tbtest
    \\go get github.com/tigerbeetledb/tigerbeetle-go
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

    .examples = 
    \\### Sidenote: `uint128`
    \\
    \\Throughout this README there will be a reference to a
    \\helper, `uint128`, that converts a string to TigerBeetle's
    \\representation of a 128-bit integer. That helper can be
    \\defined like so:
    \\
    \\```go
    \\func uint128(value string) tb_types.Uint128 {
    \\	x, err := tb_types.HexStringToUint128(value)
    \\	if err != nil {
    \\		panic(err)
    \\	}
    \\	return x
    \\}
    \\```
    ,

    .client_object_example = 
    \\tbAddress := os.Getenv("TB_ADDRESS")
    \\if len(tbAddress) == 0 {
    \\  tbAddress = "3000"
    \\} 
    \\client, err := tb.NewClient(0, []string{tbAddress}, 32)
    \\if err != nil {
    \\	log.Printf("Error creating client: %s", err)
    \\	return
    \\}
    \\defer client.Close()
    ,

    .client_object_documentation = 
    \\The third argument to `NewClient` is a `uint` max concurrency
    \\setting. `32` is a good default and can increase to `4096`
    \\as you need increased throughput.
    ,

    .create_accounts_example = 
    \\accountsRes, err := client.CreateAccounts([]tb_types.Account{
    \\	{
    \\		ID:     	uint128("137"),
    \\		UserData:	tb_types.Uint128{},
    \\		Reserved:   	[48]uint8{},
    \\		Ledger:		1,
    \\		Code:   	718,
    \\		Flags:   	0,
    \\		DebitsPending: 	0,
    \\		DebitsPosted: 	0,
    \\		CreditsPending:	0,
    \\		CreditsPosted: 	0,
    \\		Timestamp: 	0,
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
    \\The `tb_types` package can be imported from `"github.com/tigerbeetledb/tigerbeetle-go/pkg/types"`.
    ,

    .account_flags_documentation = 
    \\To toggle behavior for an account, use the `tb_types.AccountFlags` struct
    \\to combine enum values and generate a `uint16`. Here are a
    \\few examples:
    \\
    \\* `tb_types.AccountFlags{Linked: true}.ToUint16()`
    \\* `tb_types.AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16()`
    \\* `tb_types.AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16()`
    ,
    .account_flags_example = 
    \\account0 := tb_types.Account{ /* ... account values ... */ }
    \\account1 := tb_types.Account{ /* ... account values ... */ }
    \\account0.Flags = tb_types.AccountFlags{Linked: true}.ToUint16()
    \\
    \\accountErrors, err := client.CreateAccounts([]tb_types.Account{account0, account1})
    \\if err != nil {
    \\	log.Printf("Error creating accounts: %s", err)
    \\	return
    \\}
    ,

    .create_accounts_errors_example = 
    \\account2 := tb_types.Account{ /* ... account values ... */ }
    \\account3 := tb_types.Account{ /* ... account values ... */ }
    \\account4 := tb_types.Account{ /* ... account values ... */ }
    \\
    \\accountErrors, err = client.CreateAccounts([]tb_types.Account{account2, account3, account4})
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
    \\accounts, err := client.LookupAccounts([]tb_types.Uint128{uint128("137"), uint128("138")})
    \\if err != nil {
    \\	log.Printf("Could not fetch accounts: %s", err)
    \\	return
    \\}
    \\log.Println(accounts)
    ,

    .create_transfers_example = 
    \\transfer := tb_types.Transfer{
    \\	ID:			uint128("1"),
    \\	PendingID:		tb_types.Uint128{},
    \\	DebitAccountID:		uint128("1"),
    \\	CreditAccountID:	uint128("2"),
    \\	UserData:		uint128("2"),
    \\	Reserved:		tb_types.Uint128{},
    \\	Timeout:		0,
    \\	Ledger:			1,
    \\	Code:			1,
    \\	Flags:			0,
    \\	Amount:			10,
    \\	Timestamp:		0,
    \\}
    \\
    \\transfersRes, err := client.CreateTransfers([]tb_types.Transfer{transfer})
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
    \\	transfersRes, err = client.CreateTransfers([]tb_types.Transfer{transfers[i]})
    \\	// error handling omitted
    \\}
    ,

    .batch_example = 
    \\BATCH_SIZE := 8191
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
    \\To toggle behavior for an account, use the `tb_types.TransferFlags` struct
    \\to combine enum values and generate a `uint16`. Here are a
    \\few examples:
    \\
    \\* `tb_types.TransferFlags{Linked: true}.ToUint16()`
    \\* `tb_types.TransferFlags{Pending: true}.ToUint16()`
    \\* `tb_types.TransferFlags{PostPendingTransfer: true}.ToUint16()`
    \\* `tb_types.TransferFlags{VoidPendingTransfer: true}.ToUint16()`
    ,

    .transfer_flags_link_example = 
    \\transfer0 := tb_types.Transfer{ /* ... account values ... */ }
    \\transfer1 := tb_types.Transfer{ /* ... account values ... */ }
    \\transfer0.Flags = tb_types.TransferFlags{Linked: true}.ToUint16()
    \\transfersRes, err = client.CreateTransfers([]tb_types.Transfer{transfer0, transfer1})
    \\// error handling omitted
    ,
    .transfer_flags_post_example = 
    \\transfer = tb_types.Transfer{
    \\	ID:		uint128("2"),
    \\	PendingID:	uint128("1"),
    \\	Flags:		tb_types.TransferFlags{PostPendingTransfer: true}.ToUint16(),
    \\	Timestamp:	0,
    \\}
    \\transfersRes, err = client.CreateTransfers([]tb_types.Transfer{transfer})
    \\// error handling omitted
    ,
    .transfer_flags_void_example = 
    \\transfer = tb_types.Transfer{
    \\	ID:		uint128("2"),
    \\	PendingID:	uint128("1"),
    \\	Flags:		tb_types.TransferFlags{VoidPendingTransfer: true}.ToUint16(),
    \\	Timestamp:	0,
    \\}
    \\transfersRes, err = client.CreateTransfers([]tb_types.Transfer{transfer})
    \\// error handling omitted
    ,

    .lookup_transfers_example = 
    \\transfers, err := client.LookupTransfers([]tb_types.Uint128{uint128("1"), uint128("2")})
    \\if err != nil {
    \\	log.Printf("Could not fetch transfers: %s", err)
    \\	return
    \\}
    \\log.Println(transfers)
    ,

    .linked_events_example = 
    \\batch := []tb_types.Transfer{}
    \\linkedFlag := tb_types.TransferFlags{Linked: true}.ToUint16()
    \\
    \\// An individual transfer (successful):
    \\batch = append(batch, tb_types.Transfer{ID: uint128("1"), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
    \\batch = append(batch, tb_types.Transfer{ID: uint128("2"), /* ... , */ Flags: linkedFlag }) // Commit/rollback.
    \\batch = append(batch, tb_types.Transfer{ID: uint128("3"), /* ... , */ Flags: linkedFlag }) // Commit/rollback.
    \\batch = append(batch, tb_types.Transfer{ID: uint128("2"), /* ... , */ Flags: linkedFlag }) // Fail with exists
    \\batch = append(batch, tb_types.Transfer{ID: uint128("4"), /* ... , */ }) // Fail without committing
    \\
    \\// An individual transfer (successful):
    \\// This should not see any effect from the failed chain above.
    \\batch = append(batch, tb_types.Transfer{ID: uint128("2"), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 2 transfers (the first transfer fails the chain):
    \\batch = append(batch, tb_types.Transfer{ID: uint128("2"), /* ... rest of transfer ... */ Flags: linkedFlag })
    \\batch = append(batch, tb_types.Transfer{ID: uint128("3"), /* ... rest of transfer ... */ })
    \\
    \\// A chain of 2 transfers (successful):
    \\batch = append(batch, tb_types.Transfer{ID: uint128("3"), /* ... rest of transfer ... */ Flags: linkedFlag })
    \\batch = append(batch, tb_types.Transfer{ID: uint128("4"), /* ... rest of transfer ... */ })
    \\
    \\transfersRes, err = client.CreateTransfers(batch)
    ,

    .developer_setup_documentation = "",

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_sh_commands = 
    \\./scripts/build.sh go_client -Drelease-safe
    \\cd src/clients/go
    \\if [ "$TEST" = "true" ]; then go test; else echo "Skipping client unit tests"; fi
    ,

    .current_commit_pre_install_hook = go_current_commit_pre_install_hook,
    .current_commit_post_install_hook = go_current_commit_post_install_hook,

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_pwsh_commands = 
    \\.\scripts\build.bat go_client -Drelease-safe
    \\cd src\clients\go
    \\if ($env:TEST -eq 'true') { go test } else { echo "Skipping client unit test" }
    ,

    .test_main_prefix = 
    \\package main
    \\
    \\import "log"
    \\import "os"
    \\
    \\import tb "github.com/tigerbeetledb/tigerbeetle-go"
    \\import tb_types "github.com/tigerbeetledb/tigerbeetle-go/pkg/types"
    \\
    \\func uint128(value string) tb_types.Uint128 {
    \\	x, err := tb_types.HexStringToUint128(value)
    \\	if err != nil {
    \\		panic(err)
    \\	}
    \\	return x
    \\}
    \\
    \\func main() {
    ,

    .test_main_suffix = "}",
};
