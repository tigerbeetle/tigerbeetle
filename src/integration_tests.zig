//! Integration tests for TigerBeetle. Although the term is not particularly well-defined, here
//! it means a specific thing:
//!
//!   * the test binary itself doesn't contain any code from TigerBeetle,
//!   * but it has access to a pre-build `./tigerbeetle` binary.
//!
//! All the testing is done through interacting with a separate tigerbeetle process.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("./shell.zig");
const Snap = @import("./testing/snaptest.zig").Snap;
const snap = Snap.snap;
const TmpTigerBeetle = @import("./testing/tmp_tigerbeetle.zig");

const tigerbeetle: []const u8 = @import("test_options").tigerbeetle_exe;
const tigerbeetle_past: []const u8 = @import("test_options").tigerbeetle_exe_past;

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

test "in-place upgrade" {
    // Smoke test that in-place upgrades work.
    //
    // Stats a cluster of three replicas using the previous release of TigerBeetle and then replaces
    // the binaries on disk with a new version.
    //
    // Against this upgrading cluster, we are running a benchmark load and check that it finishes
    // with zero status.
    //
    // To spice things up, replicas are periodically killed and restarted.

    if (builtin.target.os.tag != .linux) {
        // For now, test in-place upgrades only on Linux.
        return error.SkipZigTest;
    }

    const Context = struct {
        const Context = @This();

        const replica_count = 3;
        // The test uses this hard-coded address, so only one instance can be running at a time.
        const addresses = "127.0.0.1:7121,127.0.0.1:7122,127.0.0.1:7123";

        shell: *Shell,
        tmp: []const u8,

        rng: std.rand.DefaultPrng,
        replicas: [replica_count]?std.process.Child = .{null} ** replica_count,
        replica_exe: [replica_count][]const u8,
        replica_datafile: [replica_count][]const u8,
        replica_upgraded: [replica_count]bool = .{false} ** replica_count,

        fn init(options: struct { seed: u64 }) !Context {
            const shell = try Shell.create(std.testing.allocator);
            errdefer shell.destroy();

            const tmp = try shell.fmt("./.zig-cache/tmp/{}", .{
                std.crypto.random.int(u64),
            });
            errdefer shell.cwd.deleteTree(tmp) catch {};

            try shell.cwd.makePath(tmp);

            var replica_exe: [replica_count][]const u8 = .{""} ** replica_count;
            var replica_datafile: [replica_count][]const u8 = .{""} ** replica_count;
            for (0..replica_count) |replica_index| {
                replica_exe[replica_index] = try shell.fmt("{s}/tigerbeetle{}{s}", .{
                    tmp,
                    replica_index,
                    builtin.target.exeFileExt(),
                });
                replica_datafile[replica_index] = try shell.fmt("{s}/0_{}.tigerbeetle", .{
                    tmp,
                    replica_index,
                });
            }

            const rng = std.rand.DefaultPrng.init(options.seed);
            return .{
                .shell = shell,
                .tmp = tmp,
                .rng = rng,
                .replica_exe = replica_exe,
                .replica_datafile = replica_datafile,
            };
        }

        fn deinit(context: *Context) void {
            for (&context.replicas) |*replica| {
                if (replica.*) |*alive| {
                    _ = alive.kill() catch {};
                }
            }

            // context.shell.cwd.deleteTree(context.tmp) catch {};
            context.shell.destroy();
            context.* = undefined;
        }

        fn run(context: *Context) !void {
            const random = context.rng.random();

            for (0..replica_count) |replica_index| {
                try context.install_replica(context.replica_exe[replica_index], .past);
            }
            for (0..replica_count) |replica_index| {
                try context.shell.exec_options(.{ .echo = false },
                    \\{tigerbeetle} format --cluster=0 --replica={replica} --replica-count=3
                    \\    {datafile}
                , .{
                    .tigerbeetle = context.replica_exe[replica_index],
                    .replica = replica_index,
                    .datafile = context.replica_datafile[replica_index],
                });
            }

            const tigerbeetle_load = try context.shell.fmt("{s}/tigerbeetle-load", .{context.tmp});
            try context.install_replica(tigerbeetle_load, .past);

            var load = try context.shell.spawn(.{},
                \\{tigerbeetle} benchmark
                \\    --print-batch-timings
                \\    --transfer-count=2_000_000
                \\    --addresses={addresses}
            , .{
                .tigerbeetle = tigerbeetle_load,
                .addresses = addresses,
            });
            defer {
                _ = load.kill() catch {};
            }

            for (0..replica_count) |replica_index| {
                try context.spawn_replica(replica_index);
            }

            const ticks_max = 50;
            var upgrade_tick: [replica_count]u8 = .{0} ** replica_count;
            for (0..replica_count) |replica_index| {
                upgrade_tick[replica_index] = random.uintLessThan(u8, ticks_max);
            }

            for (0..ticks_max) |tick| {
                std.time.sleep(2 * std.time.ns_per_s);

                for (0..replica_count) |replica_index| {
                    if (tick == upgrade_tick[replica_index]) {
                        assert(!context.replica_upgraded[replica_index]);
                        try context.upgrade_replica(replica_index);
                        assert(context.replica_upgraded[replica_index]);
                    }
                }

                const replica_index = random.uintLessThan(u8, replica_count);
                const crash = random.uintLessThan(u8, 4) == 0;
                const restart = random.uintLessThan(u8, 2) == 0;

                if (context.replicas[replica_index] == null and restart) {
                    try context.spawn_replica(replica_index);
                } else if (context.replicas[replica_index] != null and crash) {
                    try context.kill_replica(replica_index);
                }
            }

            for (0..replica_count) |replica_index| {
                assert(context.replica_upgraded[replica_index]);
                if (context.replicas[replica_index] == null) {
                    try context.spawn_replica(replica_index);
                }
            }

            const term = try load.wait();
            assert(term.Exited == 0);
        }

        fn install_replica(
            context: *Context,
            destination: []const u8,
            version: enum { past, current },
        ) !void {
            try context.shell.cwd.copyFile(
                switch (version) {
                    .past => tigerbeetle_past,
                    .current => tigerbeetle,
                },
                context.shell.cwd,
                destination,
                .{},
            );
            try context.shell.file_make_executable(destination);
        }

        fn upgrade_replica(context: *Context, replica_index: usize) !void {
            assert(!context.replica_upgraded[replica_index]);
            context.shell.cwd.deleteFile(context.replica_exe[replica_index]) catch {};
            try context.install_replica(context.replica_exe[replica_index], .current);
            context.replica_upgraded[replica_index] = true;
        }

        fn spawn_replica(context: *Context, replica_index: usize) !void {
            assert(context.replicas[replica_index] == null);
            context.replicas[replica_index] = try context.shell.spawn(.{},
                \\{tigerbeetle} start --addresses={addresses} {datafile}
            , .{
                .tigerbeetle = context.replica_exe[replica_index],
                .addresses = addresses,
                .datafile = context.replica_datafile[replica_index],
            });
        }

        fn kill_replica(context: *Context, replica_index: usize) !void {
            assert(context.replicas[replica_index] != null);
            _ = context.replicas[replica_index].?.kill() catch {};
            context.replicas[replica_index] = null;
        }
    };

    const seed = std.crypto.random.int(u64);
    log.info("seed = {}", .{seed});
    var context = try Context.init(.{ .seed = seed });
    defer context.deinit();

    try context.run();
}
