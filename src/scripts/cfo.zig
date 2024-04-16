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
};

const Fuzzer = enum {
    canary,
    ewah,
    vsr_free_set,
    vsr_superblock,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    if (builtin.os.tag == .windows) {
        return error.NotSupported;
    }

    assert(try shell.exec_status_ok("git --version", .{}));

    const commit_sha: [40]u8 = commit_sha: {
        const commit_str = try shell.exec_stdout("git rev-parse HEAD", .{});
        assert(commit_str.len == 40);
        break :commit_sha commit_str[0..40].*;
    };

    var seeds = std.ArrayList(SeedRecord).init(shell.arena.allocator());
    try run_fuzzers(shell, &seeds, .{
        .commit = commit_sha,
        .concurrency = cli_args.concurrency orelse try std.Thread.getCpuCount(),
        .budget_seconds = cli_args.budget_minutes * std.time.s_per_min,
        .hang_seconds = cli_args.hang_minutes * std.time.s_per_min,
    });
    try upload_results(shell, gpa, seeds.items);
}

fn run_fuzzers(
    shell: *Shell,
    seeds: *std.ArrayList(SeedRecord),
    options: struct {
        commit: [40]u8,
        concurrency: usize,
        budget_seconds: u64,
        hang_seconds: u64,
    },
) !void {
    // Fuzz an independent clone of the repository, so that CFO and the fuzzer could be on
    // different branches (to fuzz PRs and releases).
    shell.project_root.deleteTree("working") catch {};
    try shell.exec("git clone https://github.com/tigerbeetle/tigerbeetle working", .{});

    try shell.pushd("./working");
    defer shell.popd();

    assert(try shell.dir_exists(".git"));

    const commit_str: []const u8 = &options.commit;
    try shell.exec("git switch --detach {commit}", .{ .commit = commit_str });

    const commit_timestamp = commit_timestamp: {
        const timestamp = try shell.exec_stdout(
            "git show -s --format=%ct {sha}",
            .{ .sha = commit_str },
        );
        break :commit_timestamp try std.fmt.parseInt(u64, timestamp, 10);
    };

    log.info("fuzzing commit={s} timestamp={d}", .{ options.commit, commit_timestamp });

    const random = std.crypto.random;

    try shell.exec("../zig/zig build -Drelease build_fuzz", .{});

    const FuzzerChild = struct {
        child: std.ChildProcess,
        seed: SeedRecord,
    };

    var fuzzers = try shell.arena.allocator().alloc(?FuzzerChild, options.concurrency);
    for (fuzzers) |*fuzzer| fuzzer.* = null;
    defer for (fuzzers) |*fuzzer_or_null| {
        if (fuzzer_or_null.*) |*fuzzer| {
            _ = fuzzer.child.kill() catch {};
            fuzzer_or_null.* = null;
        }
    };

    const total_budget_seconds = options.budget_seconds + options.hang_seconds;
    for (0..total_budget_seconds) |second| {
        const last_iteration = second == total_budget_seconds - 1;

        if (second < options.budget_seconds) {
            // Start new fuzzer processes if we have more time.
            for (fuzzers) |*fuzzer_or_null| {
                if (fuzzer_or_null.* == null) {
                    const fuzzer = random.enumValue(Fuzzer);
                    const seed = random.int(u64);
                    const command = try shell.print(
                        "./zig/zig build -Drelease fuzz -- --seed={d} {s}",
                        .{ seed, @tagName(fuzzer) },
                    );
                    const seed_record = SeedRecord{
                        .commit_timestamp = commit_timestamp,
                        .commit_sha = options.commit,
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
                        "../zig/zig build -Drelease fuzz -- --seed={seed} {fuzzer}",
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

        if (second < options.budget_seconds) {
            assert(running_count == options.concurrency);
        }
        if (running_count == 0) break;
    }
}

fn upload_results(
    shell: *Shell,
    gpa: std.mem.Allocator,
    seeds_new: []const SeedRecord,
) !void {
    log.info("uploading {} seeds", .{seeds_new.len});

    // Personal Access Token for <https://github.com/tigerbeetle/devhubdb>.
    const token = try shell.env_get("DEVHUBDB_PAT");

    _ = try shell.cwd.deleteTree("./devhubdb");
    try shell.exec(
        \\git clone --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/devhubdb.git
        \\  devhubdb
    , .{
        .token = token,
    });
    try shell.pushd("./devhubdb");
    defer shell.popd();

    for (0..32) |_| {
        // As we need a retry loop here to deal with git conflicts, let's use per-iteration arena.
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();

        try shell.exec("git fetch origin", .{});
        try shell.exec("git reset --hard origin/main", .{});

        const max_size = 1024 * 1024;
        const data = try shell.cwd.readFileAlloc(
            arena.allocator(),
            "./fuzzing/data.json",
            max_size,
        );

        const seeds_old = try std.json.parseFromSliceLeaky(
            []SeedRecord,
            arena.allocator(),
            data,
            .{},
        );

        switch (try SeedRecord.merge(arena.allocator(), .{}, seeds_old, seeds_new)) {
            .up_to_date => {
                log.info("seeds already up to date", .{});
                break;
            },
            .updated => |seeds_merged| {
                const json = try std.json.stringifyAlloc(
                    shell.arena.allocator(),
                    seeds_merged,
                    .{ .whitespace = .indent_2 },
                );
                try shell.cwd.writeFile("./fuzzing/data.json", json);
                try shell.exec("git add ./fuzzing/data.json", .{});
                try shell.git_env_setup();
                try shell.exec("git commit -m 🌱", .{});
                if (shell.exec("git push", .{})) {
                    log.info("seeds updated", .{});
                    break;
                } else |_| {
                    log.info("conflict, retrying", .{});
                }
            },
        }
    } else {
        log.err("can't push new data to devhub", .{});
        return error.CanNotPush;
    }
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

        var seed_previous: ?u64 = null;
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
                seed_previous = null;
                seed_count = 0;
            }

            if (seed_previous == record.seed) {
                continue;
            }
            seed_previous = record.seed;

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
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            //  Second commit, two successes.
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp_start = 2,
                .seed_timestamp_end = 2,
                .seed = 2,
                .command = "fuzz ewah",
            },
        },
        &.{
            // Two new failures for the first commit, one will be added.
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 2,
                .seed_timestamp_end = 2,
                .seed = 2,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 3,
                .seed_timestamp_end = 3,
                .seed = 3,
                .command = "fuzz ewah",
            },
            // One failure for the second commit, it will replace one success.
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 4,
                .seed_timestamp_end = 4,
                .seed = 4,
                .command = "fuzz ewah",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed": 4,
            \\    "seed_timestamp_start": 4,
            \\    "seed_timestamp_end": 4,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "seed": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed": 2,
            \\    "seed_timestamp_start": 2,
            \\    "seed_timestamp_end": 2,
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
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        &.{
            // A new successful commit displaces the older failure.
            .{
                .commit_timestamp = 3,
                .commit_sha = .{'3'} ** 40,
                .fuzzer = .ewah,
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 3,
            \\    "commit_sha": "3333333333333333333333333333333333333333",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "seed": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "command": "fuzz ewah"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "command": "fuzz ewah"
            \\  }
            \\]
        ),
    );

    // Deduplicates identical seeds
    try T.check(
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = .ewah,
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "seed": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "command": "fuzz ewah"
            \\  }
            \\]
        ),
    );
}
