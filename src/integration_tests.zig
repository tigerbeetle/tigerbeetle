//! Integration tests for TigerBeetle. Although the term is not particularly well-defined, here
//! it means a specific thing:
//!
//!   * the test binary itself doesn't contain any code from TigerBeetle,
//!   * but it has access to a pre-build `./tigerbeetle` binary.
//!
//! All the testing is done through interacting with a separate tigerbeetle process.

const std = @import("std");
const builtin = @import("builtin");

const Shell = @import("./shell.zig");
const Snap = @import("./testing/snaptest.zig").Snap;
const snap = Snap.snap;
const TmpTigerBeetle = @import("./testing/tmp_tigerbeetle.zig");

const tigerbeetle: []const u8 = @import("test_options").tigerbeetle_exe;

test "repl integration" {
    const Context = struct {
        const Context = @This();

        shell: *Shell,
        tigerbeetle_exe: []const u8,
        tmp_beetle: TmpTigerBeetle,

        fn init() !Context {
            const shell = try Shell.create(std.testing.allocator);
            errdefer shell.destroy();

            var tmp_beetle = try TmpTigerBeetle.init(std.testing.allocator, .{
                .prebuilt = tigerbeetle,
            });
            errdefer tmp_beetle.deinit(std.testing.allocator);

            return Context{
                .shell = shell,
                .tigerbeetle_exe = tigerbeetle,
                .tmp_beetle = tmp_beetle,
            };
        }

        fn deinit(context: *Context) void {
            context.tmp_beetle.deinit(std.testing.allocator);
            context.shell.destroy();
            context.* = undefined;
        }

        fn repl_command(context: *Context, command: []const u8) ![]const u8 {
            return try context.shell.exec_stdout(
                \\{tigerbeetle} repl --cluster=0 --addresses={addresses} --command={command}
            , .{
                .tigerbeetle = context.tigerbeetle_exe,
                .addresses = context.tmp_beetle.port_str.slice(),
                .command = command,
            });
        }

        fn check(context: *Context, command: []const u8, want: Snap) !void {
            const got = try context.repl_command(command);
            try want.diff(got);
        }
    };

    var context = try Context.init();
    defer context.deinit();

    try context.check(
        \\create_accounts id=1 flags=linked|history code=10 ledger=700, id=2 code=10 ledger=700
    , snap(@src(), ""));

    try context.check(
        \\create_transfers id=1 debit_account_id=1
        \\  credit_account_id=2 amount=10 ledger=700 code=10
    , snap(@src(), ""));

    try context.check(
        \\lookup_accounts id=1
    , snap(@src(),
        \\{
        \\  "id": "1",
        \\  "debits_pending": "0",
        \\  "debits_posted": "10",
        \\  "credits_pending": "0",
        \\  "credits_posted": "0",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": ["linked","history"],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\lookup_accounts id=2
    , snap(@src(),
        \\{
        \\  "id": "2",
        \\  "debits_pending": "0",
        \\  "debits_posted": "0",
        \\  "credits_pending": "0",
        \\  "credits_posted": "10",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\query_accounts code=10 ledger=700
    , snap(@src(),
        \\{
        \\  "id": "1",
        \\  "debits_pending": "0",
        \\  "debits_posted": "10",
        \\  "credits_pending": "0",
        \\  "credits_posted": "0",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": ["linked","history"],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\{
        \\  "id": "2",
        \\  "debits_pending": "0",
        \\  "debits_posted": "0",
        \\  "credits_pending": "0",
        \\  "credits_posted": "10",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\lookup_transfers id=1
    , snap(@src(),
        \\{
        \\  "id": "1",
        \\  "debit_account_id": "1",
        \\  "credit_account_id": "2",
        \\  "amount": "10",
        \\  "pending_id": "0",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "timeout": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\query_transfers code=10 ledger=700
    , snap(@src(),
        \\{
        \\  "id": "1",
        \\  "debit_account_id": "1",
        \\  "credit_account_id": "2",
        \\  "amount": "10",
        \\  "pending_id": "0",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "timeout": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\get_account_transfers account_id=2
    , snap(@src(),
        \\{
        \\  "id": "1",
        \\  "debit_account_id": "1",
        \\  "credit_account_id": "2",
        \\  "amount": "10",
        \\  "pending_id": "0",
        \\  "user_data_128": "0",
        \\  "user_data_64": "0",
        \\  "user_data_32": "0",
        \\  "timeout": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));

    try context.check(
        \\get_account_balances account_id=1
    , snap(@src(),
        \\{
        \\  "debits_pending": "0",
        \\  "debits_posted": "10",
        \\  "credits_pending": "0",
        \\  "credits_posted": "0",
        \\  "timestamp": "<snap:ignore>"
        \\}
        \\
    ));
}

test "benchmark/inspect smoke" {
    const data_file = data_file: {
        var random_bytes: [4]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);
        const random_suffix: [8]u8 = std.fmt.bytesToHex(random_bytes, .lower);
        break :data_file "0_0-" ++ random_suffix ++ ".tigerbeetle.benchmark";
    };
    defer std.fs.cwd().deleteFile(data_file) catch {};

    const shell = try Shell.create(std.testing.allocator);
    defer shell.destroy();

    const status_ok_benchmark = try shell.exec_status_ok(
        "{tigerbeetle} benchmark --transfer-count=10_000 --transfer-batch-size=10 --validate " ++
            "--file={file}",
        .{ .tigerbeetle = tigerbeetle, .file = data_file },
    );
    try std.testing.expect(status_ok_benchmark);

    inline for (.{
        "{tigerbeetle} inspect superblock              {path}",
        "{tigerbeetle} inspect wal --slot=0            {path}",
        "{tigerbeetle} inspect replies                 {path}",
        "{tigerbeetle} inspect replies --slot=0        {path}",
        "{tigerbeetle} inspect grid                    {path}",
        "{tigerbeetle} inspect manifest                {path}",
        "{tigerbeetle} inspect tables --tree=transfers {path}",
    }) |command| {
        const status_ok_inspect = try shell.exec_status_ok(
            command,
            .{ .tigerbeetle = tigerbeetle, .path = data_file },
        );
        try std.testing.expect(status_ok_inspect);
    }
}

test "help/version smoke" {
    const shell = try Shell.create(std.testing.allocator);
    defer shell.destroy();

    // The substring is chosen to be mostly stable, but from (near) the end of the output, to catch
    // a missed buffer flush.
    inline for (.{
        .{ .command = "{tigerbeetle} --help", .substring = "tigerbeetle repl" },
        .{ .command = "{tigerbeetle} inspect --help", .substring = "tables --tree" },
        .{ .command = "{tigerbeetle} version", .substring = "TigerBeetle version" },
        .{ .command = "{tigerbeetle} version --verbose", .substring = "process.aof_recovery=" },
    }) |check| {
        const output = try shell.exec_stdout(check.command, .{ .tigerbeetle = tigerbeetle });
        try std.testing.expect(output.len > 0);
        try std.testing.expect(std.mem.indexOf(u8, output, check.substring) != null);
    }
}
