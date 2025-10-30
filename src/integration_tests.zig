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
const Snap = stdx.Snap;
const snap = Snap.snap_fn("src");
const TmpTigerBeetle = @import("./testing/tmp_tigerbeetle.zig");

const stdx = @import("stdx");
const ratio = stdx.PRNG.ratio;

const vortex_exe: []const u8 = @import("test_options").vortex_exe;
const tigerbeetle: []const u8 = @import("test_options").tigerbeetle_exe;
const tigerbeetle_past: []const u8 = @import("test_options").tigerbeetle_exe_past;

comptime {
    _ = @import("clients/c/tb_client_header_test.zig");
}

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
                .development = true,
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
                .addresses = context.tmp_beetle.port_str,
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

    const trace_file = data_file ++ ".json";
    defer std.fs.cwd().deleteFile(trace_file) catch {};

    const shell = try Shell.create(std.testing.allocator);
    defer shell.destroy();

    try shell.exec(
        "{tigerbeetle} benchmark" ++
            " --transfer-count=10_000" ++
            " --transfer-batch-size=10" ++
            " --validate" ++
            " --trace={trace_file}" ++
            " --statsd=127.0.0.1:65535" ++
            " --file={data_file}",
        .{
            .tigerbeetle = tigerbeetle,
            .trace_file = trace_file,
            .data_file = data_file,
        },
    );

    inline for (.{
        "{tigerbeetle} inspect constants",
        "{tigerbeetle} inspect metrics",
    }) |command| {
        log.debug("{s}", .{command});
        try shell.exec(command, .{ .tigerbeetle = tigerbeetle });
    }

    inline for (.{
        "{tigerbeetle} inspect superblock              {path}",
        "{tigerbeetle} inspect wal --slot=0            {path}",
        "{tigerbeetle} inspect replies                 {path}",
        "{tigerbeetle} inspect replies --slot=0        {path}",
        "{tigerbeetle} inspect grid                    {path}",
        "{tigerbeetle} inspect manifest                {path}",
        "{tigerbeetle} inspect tables --tree=transfers {path}",
        "{tigerbeetle} inspect integrity               {path}",
    }) |command| {
        log.debug("{s}", .{command});

        try shell.exec(
            command,
            .{ .tigerbeetle = tigerbeetle, .path = data_file },
        );
    }

    // Corrupt the data file, and ensure the integrity check fails. Due to how it works, the
    // corruption has to be in a spot that's actually used. Take the first offset from
    // `tigerbeetle inspect tables --tree=transfers`.
    const tables_output = try shell.exec_stdout(
        "{tigerbeetle} inspect tables --tree=transfers {path}",
        .{ .tigerbeetle = tigerbeetle, .path = data_file },
    );

    const offset = try std.fmt.parseInt(
        u64,
        stdx.cut(tables_output, "O=").?[1],
        10,
    );

    {
        const file = try std.fs.cwd().openFile(data_file, .{ .mode = .read_write });
        defer file.close();

        var prng = stdx.PRNG.from_seed_testing();
        var random_bytes: [256]u8 = undefined;
        prng.fill(&random_bytes);

        try file.pwriteAll(&random_bytes, offset);
    }

    // `shell.exec` assumes that success is a zero exit code; but in this case the test expects
    // corruption to be found and wants to assert a non-zero exit code.
    var child = std.process.Child.init(
        &.{ tigerbeetle, "inspect", "integrity", data_file },
        std.testing.allocator,
    );
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;

    const term = try child.spawnAndWait();
    switch (term) {
        .Exited, .Signal => |value| try std.testing.expect(value != 0),
        else => unreachable,
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
    // Starts a cluster of three replicas using the previous release of TigerBeetle and then
    // replaces the binaries on disk with a new version.
    //
    // Against this upgrading cluster, we are running a benchmark load and checking that it finishes
    // with a zero status.
    //
    // To spice things up, replicas are periodically killed and restarted.

    if (builtin.target.os.tag == .windows) {
        return error.SkipZigTest; // Coming soon!
    }

    const replica_count = TmpCluster.replica_count;

    var cluster = try TmpCluster.init();
    defer cluster.deinit();

    for (0..replica_count) |replica_index| {
        try cluster.replica_install(replica_index, .past);
        try cluster.replica_format(replica_index);
    }
    try cluster.workload_start(.{ .transfer_count = 2_000_000 });

    for (0..replica_count) |replica_index| {
        try cluster.replica_spawn(replica_index);
    }

    const ticks_max = 50;
    var upgrade_tick: [replica_count]u8 = @splat(0);
    for (0..replica_count) |replica_index| {
        upgrade_tick[replica_index] = cluster.prng.int_inclusive(u8, ticks_max - 1);
    }

    for (0..ticks_max) |tick| {
        std.time.sleep(2 * std.time.ns_per_s);

        for (0..replica_count) |replica_index| {
            if (tick == upgrade_tick[replica_index]) {
                assert(!cluster.replica_upgraded[replica_index]);
                try cluster.replica_upgrade(replica_index);
                assert(cluster.replica_upgraded[replica_index]);
            }
        }

        const replica_index = cluster.prng.index(cluster.replicas);
        const crash = cluster.prng.chance(ratio(1, 4));
        const restart = cluster.prng.chance(ratio(1, 2));

        if (cluster.replicas[replica_index] == null and restart) {
            try cluster.replica_spawn(replica_index);
        } else if (cluster.replicas[replica_index] != null and crash) {
            try cluster.replica_kill(replica_index);
        }
    }

    for (0..replica_count) |replica_index| {
        assert(cluster.replica_upgraded[replica_index]);
        if (cluster.replicas[replica_index] == null) {
            try cluster.replica_spawn(replica_index);
        }
    }

    cluster.workload_finish();
}

test "recover smoke" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const replica_count = TmpCluster.replica_count;

    var cluster = try TmpCluster.init();
    defer cluster.deinit();

    for (0..replica_count) |replica_index| {
        try cluster.replica_install(replica_index, .past);
    }
    try cluster.replica_format(0);
    try cluster.replica_format(1);
    try cluster.replica_format(2);
    try cluster.workload_start(.{ .transfer_count = 200_000 });
    try cluster.replica_spawn(0);
    try cluster.replica_spawn(1);
    try cluster.replica_spawn(2);
    std.time.sleep(2 * std.time.ns_per_s);

    try cluster.replica_kill(2);
    try cluster.replica_reformat(2);

    try cluster.replica_kill(1);
    try cluster.replica_spawn(2);
    cluster.workload_finish();
}

test "vortex smoke" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const shell = try Shell.create(std.testing.allocator);
    defer shell.destroy();

    try shell.exec(
        "{vortex_exe} supervisor --test-duration=1s --replica-count=1",
        .{ .vortex_exe = vortex_exe },
    );
}

const TmpCluster = struct {
    const replica_count = 3;
    // The test uses this hard-coded address, so only one instance can be running at a time.
    const addresses = "127.0.0.1:7121,127.0.0.1:7122,127.0.0.1:7123";

    shell: *Shell,
    tmp: []const u8,

    prng: stdx.PRNG,
    replicas: [replica_count]?std.process.Child = @splat(null),
    replica_exe: [replica_count][]const u8,
    replica_datafile: [replica_count][]const u8,
    replica_upgraded: [replica_count]bool = @splat(false),

    workload_thread: ?std.Thread = null,
    workload_exit_ok: bool = false,

    fn init() !TmpCluster {
        const shell = try Shell.create(std.testing.allocator);
        errdefer shell.destroy();

        const tmp = try shell.fmt("./.zig-cache/tmp/{}", .{std.crypto.random.int(u64)});
        errdefer shell.cwd.deleteTree(tmp) catch {};

        try shell.cwd.makePath(tmp);

        var replica_exe: [replica_count][]const u8 = @splat("");
        var replica_datafile: [replica_count][]const u8 = @splat("");
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

        const prng = stdx.PRNG.from_seed_testing();
        return .{
            .shell = shell,
            .tmp = tmp,
            .prng = prng,
            .replica_exe = replica_exe,
            .replica_datafile = replica_datafile,
        };
    }

    fn deinit(cluster: *TmpCluster) void {
        // Sadly, killing workload process is not easy, so, in case of an error, we'll wait
        // for full timeout.
        if (cluster.workload_thread) |workload_thread| {
            workload_thread.join();
        }

        for (&cluster.replicas) |*replica| {
            if (replica.*) |*alive| {
                _ = alive.kill() catch {};
            }
        }

        cluster.shell.cwd.deleteTree(cluster.tmp) catch {};
        cluster.shell.destroy();
        cluster.* = undefined;
    }

    fn replica_install(
        cluster: *TmpCluster,
        replica_index: usize,
        version: enum { past, current },
    ) !void {
        const destination = cluster.replica_exe[replica_index];
        try cluster.shell.cwd.copyFile(
            switch (version) {
                .past => tigerbeetle_past,
                .current => tigerbeetle,
            },
            cluster.shell.cwd,
            destination,
            .{},
        );
        try cluster.shell.file_make_executable(destination);
    }

    fn replica_format(cluster: *TmpCluster, replica_index: usize) !void {
        assert(cluster.replicas[replica_index] == null);

        try cluster.shell.exec(
            \\{tigerbeetle} format --cluster=0 --replica={replica} --replica-count=3 {datafile}
        , .{
            .tigerbeetle = cluster.replica_exe[replica_index],
            .replica = replica_index,
            .datafile = cluster.replica_datafile[replica_index],
        });
    }

    fn replica_reformat(cluster: *TmpCluster, replica_index: usize) !void {
        assert(cluster.replicas[replica_index] == null);

        cluster.shell.cwd.deleteFile(cluster.replica_datafile[replica_index]) catch {};

        try cluster.shell.exec(
            \\{tigerbeetle} recover
            \\    --cluster=0
            \\    --replica={replica}
            \\    --replica-count=3
            \\    --addresses={addresses}
            \\    {datafile}
        , .{
            .tigerbeetle = cluster.replica_exe[replica_index],
            .replica = replica_index,
            .addresses = addresses,
            .datafile = cluster.replica_datafile[replica_index],
        });
    }

    fn replica_upgrade(cluster: *TmpCluster, replica_index: usize) !void {
        assert(!cluster.replica_upgraded[replica_index]);

        const upgrade_requires_restart = builtin.os.tag != .linux;
        if (upgrade_requires_restart) {
            if (cluster.replicas[replica_index] != null) {
                try cluster.replica_kill(replica_index);
            }
            assert(cluster.replicas[replica_index] == null);
        }

        cluster.shell.cwd.deleteFile(cluster.replica_exe[replica_index]) catch {};
        try cluster.replica_install(replica_index, .current);
        cluster.replica_upgraded[replica_index] = true;

        if (upgrade_requires_restart) {
            assert(cluster.replicas[replica_index] == null);
            try cluster.replica_spawn(replica_index);
            assert(cluster.replicas[replica_index] != null);
        }
    }

    fn replica_spawn(cluster: *TmpCluster, replica_index: usize) !void {
        assert(cluster.replicas[replica_index] == null);
        cluster.replicas[replica_index] = try cluster.shell.spawn(.{},
            \\{tigerbeetle} start --addresses={addresses} {datafile}
        , .{
            .tigerbeetle = cluster.replica_exe[replica_index],
            .addresses = addresses,
            .datafile = cluster.replica_datafile[replica_index],
        });
    }

    fn replica_kill(cluster: *TmpCluster, replica_index: usize) !void {
        assert(cluster.replicas[replica_index] != null);
        _ = cluster.replicas[replica_index].?.kill() catch {};
        cluster.replicas[replica_index] = null;
    }

    const WorkloadStartOptions = struct {
        transfer_count: usize,
    };

    fn workload_start(cluster: *TmpCluster, options: WorkloadStartOptions) !void {
        assert(cluster.workload_thread == null);
        assert(!cluster.workload_exit_ok);
        // Run workload in a separate thread, to collect it's stdout and stderr, and to
        // forcefully terminate it after 10 minutes.
        cluster.workload_thread = try std.Thread.spawn(.{}, struct {
            fn thread_main(
                workload_exit_ok_ptr: *bool,
                tigerbeetle_path: []const u8,
                benchmark_options: WorkloadStartOptions,
            ) !void {
                const shell = try Shell.create(std.testing.allocator);
                defer shell.destroy();

                try shell.exec_options(.{ .timeout = .minutes(10) },
                    \\{tigerbeetle} benchmark
                    \\    --print-batch-timings
                    \\    --transfer-count={transfer_count}
                    \\    --addresses={addresses}
                , .{
                    .tigerbeetle = tigerbeetle_path,
                    .addresses = addresses,
                    .transfer_count = benchmark_options.transfer_count,
                });
                workload_exit_ok_ptr.* = true;
            }
        }.thread_main, .{ &cluster.workload_exit_ok, tigerbeetle_past, options });
    }

    fn workload_finish(cluster: *TmpCluster) void {
        cluster.workload_thread.?.join();
        cluster.workload_thread = null;
        assert(cluster.workload_exit_ok);
    }
};
