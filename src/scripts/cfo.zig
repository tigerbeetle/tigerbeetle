//! Continuous Fuzzing Orchestrator.
//!
//! We have a number of machines which run
//!
//!     git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle
//!     while True:
//!         git fetch origin && git reset --hard origin/main
//!         ./scripts/install_zig.sh
//!         ./zig/zig build scripts -- cfo
//!
//! By modifying this script, we can make those machines do interesting things.
//!
//! The primary use-case is fuzzing: `cfo` runs a random fuzzer, and, if it finds a failure, it is
//! recorded in devhubdb.
//!
//! Specifically:
//!
//! CFO keeps `args.concurrency` fuzzes running at the same time. For simplicity, it polls currently
//! running fuzzers for completion every second in a fuzzing loop. A fuzzer fails if it returns
//! non-zero error code.
//!
//! The fuzzing loops runs for `args.budget_minutes`. To detect hangs, if any fuzzer is still
//! running after additional `args.hang_minutes`, it is killed (thus returning non-zero status and
//! recording a failure).
//!
//! It is important that the caller (systemd typically) arranges for CFO to be a process group
//! leader. It is not possible to reliably wait for (grand) children with POSIX, so its on the
//! call-site to cleanup any run-away subprocesses
//!
//! After the fuzzing loop, CFO collects a list of seeds, some of which are failing. Next, it
//! merges, this list into previous set of seeds (persisting seeds is to be implemented, at the
//! moment the old list is always empty).
//!
//! Rules for merging:
//!
//! - Keep seeds for at most `commit_count_max` distinct commits.
//! - Prefer fresher commits (based on commit time stamp).
//! - For each commit and fuzzer combination, keep at most `seed_count_max` seeds.
//! - Prefer failing seeds to successful seeds.
//! - Prefer older seeds.
//!
//! These rules ensure that in the steady state (assuming fuzzer's clock doesn't fail) the set of
//! seeds is stable. If the clock goes backwards, there might be churn in the set of a seed, but the
//! number of failing seeds will never decrease.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

pub const CliArgs = struct {
    budget_minutes: u64 = 10,
    hang_minutes: u64 = 30,
    concurrency: ?u32 = null,
    // Personal Access Token for <https://github.com/tigerbeetle/devhubdb>.
    devhgubdb_pat: ?[]const u8 = null,
};

const Fuzzer = enum {
    ewah,
    vsr_free_set,
    vsr_superblock,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    if (builtin.os.tag == .windows) {
        return error.NotSupported;
    }

    const concurrency = cli_args.concurrency orelse try std.Thread.getCpuCount();

    _ = gpa;
    assert(try shell.exec_status_ok("git --version", .{}));

    const commit_sha_str = try shell.exec_stdout("git rev-parse HEAD", .{});
    assert(commit_sha_str.len == 40);
    const commit_sha: [40]u8 = commit_sha_str[0..40].*;

    const commit_timestamp_str = try shell.exec_stdout(
        "git show -s --format=%ct {sha}",
        .{ .sha = commit_sha_str },
    );
    const commit_timestamp = try std.fmt.parseInt(u64, commit_timestamp_str, 10);

    log.info("commit={s} timestamp={s}", .{ commit_sha_str, commit_timestamp_str });

    const random = std.crypto.random;

    try shell.exec("zig/zig build -Drelease build_fuzz", .{});

    const FuzzerChild = struct {
        child: std.ChildProcess,
        seed: SeedRecord,
    };

    var seeds = std.ArrayList(SeedRecord).init(shell.arena.allocator());
    var fuzzers = try shell.arena.allocator().alloc(?FuzzerChild, concurrency);
    for (fuzzers) |*fuzzer| fuzzer.* = null;
    defer for (fuzzers) |*fuzzer_or_null| {
        if (fuzzer_or_null.*) |*fuzzer| {
            _ = fuzzer.child.kill() catch {};
            fuzzer_or_null.* = null;
        }
    };

    const total_budget_seconds =
        (cli_args.budget_minutes + cli_args.hang_minutes) * std.time.s_per_min;
    for (0..total_budget_seconds) |second| {
        const last_iteration = second == total_budget_seconds - 1;

        if (second < cli_args.budget_minutes * std.time.s_per_min) {
            // Start new fuzzer processes if we have more time.
            for (fuzzers) |*fuzzer_or_null| {
                if (fuzzer_or_null.* == null) {
                    const fuzzer = random.enumValue(Fuzzer);
                    const seed = random.int(u64);
                    const command = try shell.print(
                        "zig/zig build -Drelease fuzz -- --seed={d} {s}",
                        .{ seed, @tagName(fuzzer) },
                    );
                    const seed_record = SeedRecord{
                        .commit_timestamp = commit_timestamp,
                        .commit_sha = commit_sha,
                        .fuzzer = fuzzer,
                        .ok = false,
                        .seed = seed,
                        .seed_timestamp_start = @intCast(std.time.timestamp()),
                        .seed_timestamp_end = 0,
                        .command = command,
                    };
                    log.debug("will start '{s}'", .{command});
                    const child = try shell.spawn_options(
                        .{ .stdin_behavior = .Pipe },
                        "zig/zig build -Drelease fuzz -- --seed={seed} {fuzzer}",
                        .{ .seed = try shell.print("{d}", .{seed}), .fuzzer = @tagName(fuzzer) },
                    );
                    _ = try std.os.fcntl(
                        child.stdin.?.handle,
                        std.os.F.SETFD,
                        @as(u32, std.os.O.NONBLOCK),
                    );
                    fuzzer_or_null.* = .{ .seed = seed_record, .child = child };
                }
            }
        }

        // Wait for a second before polling for completion.
        std.time.sleep(1 * std.time.ns_per_s);

        var running_count: u32 = 0;
        for (fuzzers) |*fuzzer_or_null| {
            // Poll for completed fuzzers.
            if (fuzzer_or_null.*) |*fuzzer| {
                running_count += 1;

                var fuzzer_done = false;
                _ = fuzzer.child.stdin.?.write(&.{1}) catch |err| {
                    switch (err) {
                        error.WouldBlock => {},
                        error.BrokenPipe => fuzzer_done = true,
                        else => return err,
                    }
                };

                if (fuzzer_done or last_iteration) {
                    log.debug("will reap '{s}'", .{fuzzer.seed.command});
                    const term = try if (fuzzer_done) fuzzer.child.wait() else fuzzer.child.kill();
                    var seed_record = fuzzer.seed;
                    seed_record.ok = std.meta.eql(term, .{ .Exited = 0 });
                    seed_record.seed_timestamp_end = @intCast(std.time.timestamp());
                    try seeds.append(seed_record);
                    fuzzer_or_null.* = null;
                }
            }
        }

        if (second < cli_args.budget_minutes * std.time.s_per_min) {
            assert(running_count == cli_args.concurrency);
        }
        if (running_count == 0) break;
    }

    const merge_result = try SeedRecord.merge(shell.arena.allocator(), .{}, &.{}, seeds.items);
    const merged = switch (merge_result) {
        .updated => |updated| updated,
        .up_to_date => &.{},
    };

    const json = try std.json.stringifyAlloc(
        shell.arena.allocator(),
        merged,
        .{ .whitespace = .indent_2 },
    );
    std.debug.print("{s}\n", .{json});
}

const SeedRecord = struct {
    const MergeOptions = struct {
        commit_count_max: u32 = 32,
        seed_count_max: u32 = 4,
    };

    // NB: The order of fields is significant and defines comparison.
    commit_timestamp: u64, // compared in inverse order
    commit_sha: [40]u8,
    fuzzer: Fuzzer,
    ok: bool,
    seed: u64,
    seed_timestamp_start: u64,
    seed_timestamp_end: u64,
    command: []const u8, // excluded from comparison

    fn order(a: SeedRecord, b: SeedRecord) std.math.Order {
        inline for (comptime std.meta.fieldNames(SeedRecord)) |field_name| {
            if (comptime std.mem.eql(u8, field_name, "command")) continue;

            const a_field = @field(a, field_name);
            const b_field = @field(b, field_name);

            const field_order = switch (@TypeOf(@field(a, field_name))) {
                [40]u8 => std.mem.order(u8, &a_field, &b_field),
                bool => std.math.order(@intFromBool(a_field), @intFromBool(b_field)),
                Fuzzer => std.math.order(@intFromEnum(a_field), @intFromEnum(b_field)),
                else => std.math.order(a_field, b_field),
            };

            if (field_order != .eq) {
                if (comptime std.mem.eql(u8, field_name, "commit_timestamp")) {
                    return field_order.invert();
                }
                return field_order;
            }
        }
        return .eq;
    }

    fn less_than(_: void, a: SeedRecord, b: SeedRecord) bool {
        return a.order(b) == .lt;
    }

    // Merges two sets of seeds keeping the more interesting one. A direct way to write this would
    // be to group the seeds by commit & fuzzer and do a union of nested hash maps, but that's a
    // pain to implement in Zig. Luckily, by cleverly implementing the ordering on seeds it is
    // possible to implement the merge by concatenation, sorting, and a single-pass counting scan.
    fn merge(
        arena: std.mem.Allocator,
        options: MergeOptions,
        current: []const SeedRecord,
        new: []const SeedRecord,
    ) !union(enum) { updated: []const SeedRecord, up_to_date } {
        var current_and_new = try std.mem.concat(arena, SeedRecord, &.{ current, new });
        std.mem.sort(SeedRecord, current_and_new, {}, SeedRecord.less_than);

        var result = try std.ArrayList(SeedRecord).initCapacity(arena, current.len);

        var commit_sha_previous: ?[40]u8 = null;
        var commit_count: u32 = 0;

        var fuzzer_previous: ?Fuzzer = null;
        var seed_count: u32 = 0;

        for (current_and_new) |record| {
            if (commit_sha_previous == null or
                !std.meta.eql(commit_sha_previous.?, record.commit_sha))
            {
                commit_sha_previous = record.commit_sha;
                commit_count += 1;
                fuzzer_previous = null;
            }

            if (commit_count > options.commit_count_max) {
                break;
            }

            if (fuzzer_previous == null or
                fuzzer_previous.? != record.fuzzer)
            {
                fuzzer_previous = record.fuzzer;
                seed_count = 0;
            }

            seed_count += 1;
            if (seed_count <= options.seed_count_max) {
                try result.append(record);
            }
        }

        if (result.items.len != current.len) {
            return .{ .updated = result.items };
        }
        for (result.items, current) |new_record, current_record| {
            if (new_record.order(current_record) != .eq) {
                return .{ .updated = result.items };
            }
        }
        return .up_to_date;
    }
};

test "cfo: SeedRecord.merge" {
    const Snap = @import("../testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    const T = struct {
        fn check(current: []const SeedRecord, new: []const SeedRecord, want: Snap) !void {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();

            const options = SeedRecord.MergeOptions{
                .commit_count_max = 2,
                .seed_count_max = 2,
            };
            const got = switch (try SeedRecord.merge(arena.allocator(), options, current, new)) {
                .up_to_date => current,
                .updated => |updated| updated,
            };
            try want.diff_json(got, .{ .whitespace = .indent_2 });
        }
    };

    try T.check(&.{}, &.{}, snap(@src(),
        \\[]
    ));

    try T.check(
        &.{
            // First commit, one failure.
            .{
                .commit_timestamp = 1,
                .commit_sha = 1,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            //  Second commit, two successes.
            .{
                .commit_timestamp = 2,
                .commit_sha = 2,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = 2,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp = 2,
                .seed = 2,
                .command = "fuzz ewah",
            },
        },
        &.{
            // Two new failures for the first commit, one will be added.
            .{
                .commit_timestamp = 1,
                .commit_sha = 1,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 2,
                .seed = 2,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = 1,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 3,
                .seed = 3,
                .command = "fuzz ewah",
            },
            // One failure for the second commit, it will replace one success.
            .{
                .commit_timestamp = 2,
                .commit_sha = 2,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 4,
                .seed = 4,
                .command = "fuzz ewah",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": 2,
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed_timestamp": 4,
            \\    "seed": 4,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": 2,
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "seed_timestamp": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": 1,
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed_timestamp": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": 1,
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed_timestamp": 2,
            \\    "seed": 2,
            \\    "command": "fuzz ewah"
            \\  }
            \\]
        ),
    );

    try T.check(
        &.{
            // Two failing commits.
            .{
                .commit_timestamp = 1,
                .commit_sha = 1,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = 2,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        &.{
            // A new successful commit displaces the older failure.
            .{
                .commit_timestamp = 3,
                .commit_sha = 3,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 3,
            \\    "commit_sha": 3,
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "seed_timestamp": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": 2,
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed_timestamp": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah"
            \\  }
            \\]
        ),
    );
}
