//! Continuous Fuzzing Orchestrator.
//!
//! We have a number of machines which run
//!
//!     git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle
//!     while True:
//!         git fetch origin && git reset --hard origin/main
//!         ./zig/download.sh
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
//! call-site to cleanup any run-away subprocesses. See `./cfo_supervisor.sh` for one way to
//! arrange that.
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
//! - Prefer seeds that failed faster
//! - Prefer older seeds.
//! - When dropping a non-failing seed, add its count to some other non-failing seeds.
//!
//! The idea here is that we want to keep the set of failing seeds stable, while maintaining some
//! measure of how much fuzzing work was done in total.

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
    lsm_cache_map,
    lsm_forest,
    lsm_manifest_level,
    lsm_manifest_log,
    lsm_scan,
    lsm_segmented_array,
    lsm_tree,
    storage,
    vopr_lite,
    vopr_testing_lite,
    vopr_testing,
    vopr,
    vsr_free_set,
    vsr_journal_format,
    vsr_superblock_quorums,
    vsr_superblock,

    fn fill_args_build(fuzzer: Fuzzer, accumulator: *std.ArrayList([]const u8)) !void {
        switch (fuzzer) {
            .vopr, .vopr_testing, .vopr_lite, .vopr_testing_lite => |f| {
                if (f == .vopr_testing or f == .vopr_testing_lite) {
                    try accumulator.append("-Dvopr-state-machine=testing");
                }
                try accumulator.appendSlice(&.{"vopr:build"});
            },
            else => try accumulator.appendSlice(&.{"fuzz:build"}),
        }
    }

    fn fill_args_run(fuzzer: Fuzzer, accumulator: *std.ArrayList([]const u8)) !void {
        switch (fuzzer) {
            .vopr, .vopr_testing, .vopr_lite, .vopr_testing_lite => |f| {
                if (f == .vopr_testing or f == .vopr_testing_lite) {
                    try accumulator.append("-Dvopr-state-machine=testing");
                }
                try accumulator.appendSlice(&.{ "vopr", "--" });
                if (f == .vopr_lite or f == .vopr_testing_lite) {
                    try accumulator.append("--lite");
                }
            },
            else => |f| try accumulator.appendSlice(&.{ "fuzz", "--", @tagName(f) }),
        }
    }
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    if (builtin.os.tag == .windows) {
        return error.NotSupported;
    }

    assert(try shell.exec_status_ok("git --version", .{}));

    // Read-write token for <https://github.com/tigerbeetle/devhubdb>.
    const devhub_token_option = shell.env_get_option("DEVHUBDB_PAT");
    if (devhub_token_option == null) {
        log.err("'DEVHUB_PAT' environmental variable is not set, will not upload results", .{});
    }

    // Readonly token for PR metadata of <https://github.com/tigerbeetle/tigerbeetle>.
    const gh_token_option = shell.env_get_option("GH_TOKEN");
    if (gh_token_option == null) {
        log.err("'GH_TOKEN' environmental variable is not set, will not fetch pull requests", .{});
    } else {
        assert(try shell.exec_status_ok("gh --version", .{}));
    }

    var seeds = std.ArrayList(SeedRecord).init(shell.arena.allocator());
    try run_fuzzers(shell, &seeds, gh_token_option, .{
        .concurrency = cli_args.concurrency orelse try std.Thread.getCpuCount(),
        .budget_seconds = cli_args.budget_minutes * std.time.s_per_min,
        .hang_seconds = cli_args.hang_minutes * std.time.s_per_min,
    });
    if (devhub_token_option) |token| {
        try upload_results(shell, gpa, token, seeds.items);
    } else {
        log.info("skipping upload, no token", .{});
        for (seeds.items) |seed_record| {
            const seed_record_json = try std.json.stringifyAlloc(
                shell.arena.allocator(),
                seed_record,
                .{},
            );
            log.info("{s}", .{seed_record_json});
        }
    }
}

fn run_fuzzers(
    shell: *Shell,
    seeds: *std.ArrayList(SeedRecord),
    gh_token: ?[]const u8,
    options: struct {
        concurrency: usize,
        budget_seconds: u64,
        hang_seconds: u64,
    },
) !void {
    const tasks = try run_fuzzers_prepare_tasks(shell, gh_token);
    log.info("fuzzing {} tasks", .{tasks.seed_record.len});
    for (tasks.seed_record, tasks.weight) |seed_record, weight| {
        log.info("fuzzing commit={s} timestamp={} fuzzer={s} branch='{s}' weight={}", .{
            seed_record.commit_sha[0..7],
            seed_record.commit_timestamp,
            seed_record.fuzzer,
            seed_record.branch,
            weight,
        });
    }

    const random = std.crypto.random;

    const FuzzerChild = struct {
        child: std.process.Child,
        seed: SeedRecord,
    };

    const children = try shell.arena.allocator().alloc(?FuzzerChild, options.concurrency);
    @memset(children, null);
    defer for (children) |*fuzzer_or_null| {
        if (fuzzer_or_null.*) |*fuzzer| {
            _ = fuzzer.child.kill() catch {};
            fuzzer_or_null.* = null;
        }
    };

    var args = std.ArrayList([]const u8).init(shell.arena.allocator());
    const total_budget_seconds = options.budget_seconds + options.hang_seconds;
    for (0..total_budget_seconds) |second| {
        const last_iteration = second == total_budget_seconds - 1;

        if (second < options.budget_seconds) {
            // Start new fuzzer processes if we have more time.
            for (children) |*child_or_null| {
                if (child_or_null.* == null) {
                    const task_index = random.weightedIndex(u32, tasks.weight);
                    const working_directory = tasks.working_directory[task_index];
                    var seed_record = tasks.seed_record[task_index];
                    const fuzzer = std.meta.stringToEnum(Fuzzer, seed_record.fuzzer).?;

                    try shell.pushd(working_directory);
                    defer shell.popd();

                    assert(try shell.dir_exists(".git") or shell.file_exists(".git"));

                    {
                        // First, build the fuzzer separately to exclude compilation from the
                        // recorded timings.
                        args.clearRetainingCapacity();
                        try args.appendSlice(&.{ "build", "-Drelease" });
                        try fuzzer.fill_args_build(&args);
                        shell.exec_options(.{ .echo = false }, "{zig} {args}", .{
                            .zig = shell.zig_exe.?,
                            .args = args.items,
                        }) catch {
                            // Ignore the error, it'll get recorded by the run anyway.
                        };
                    }

                    seed_record.seed = random.int(u64);
                    seed_record.seed_timestamp_start = @intCast(std.time.timestamp());

                    args.clearRetainingCapacity();
                    try args.appendSlice(&.{ "build", "-Drelease" });
                    try fuzzer.fill_args_run(&args);
                    try args.append(try shell.fmt("{d}", .{seed_record.seed}));

                    var command = std.ArrayList(u8).init(shell.arena.allocator());
                    try command.appendSlice("./zig/zig");
                    for (args.items) |arg| {
                        try command.append(' ');
                        try command.appendSlice(arg);
                    }

                    seed_record.command = command.items;

                    log.debug("will start '{s}'", .{seed_record.command});
                    child_or_null.* = .{
                        .seed = seed_record,
                        .child = try shell.spawn(
                            .{ .stdin_behavior = .Pipe },
                            "{zig} {args}",
                            .{ .zig = shell.zig_exe.?, .args = args.items },
                        ),
                    };

                    // Zig doesn't have non-blocking version of child.wait, so we use `BrokenPipe`
                    // on writing to child's stdin to detect if a child is dead in a non-blocking
                    // manner.
                    _ = try std.posix.fcntl(
                        child_or_null.*.?.child.stdin.?.handle,
                        std.posix.F.SETFL,
                        @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true })),
                    );
                }
            }
        }

        // Wait for a second before polling for completion.
        std.time.sleep(1 * std.time.ns_per_s);

        var running_count: u32 = 0;
        for (children) |*fuzzer_or_null| {
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
                    log.debug(
                        "will reap '{s}'{s}",
                        .{ fuzzer.seed.command, if (!fuzzer_done) " (timeout)" else "" },
                    );
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

fn run_fuzzers_prepare_tasks(shell: *Shell, gh_token: ?[]const u8) !struct {
    working_directory: [][]const u8,
    seed_record: []SeedRecord,
    weight: []u32,
} {
    var working_directory = std.ArrayList([]const u8).init(shell.arena.allocator());
    var seed_record = std.ArrayList(SeedRecord).init(shell.arena.allocator());

    // Fuzz an independent clone of the repository, so that CFO and the fuzzer could be on
    // different branches (to fuzz PRs and releases).
    shell.project_root.deleteTree("working") catch {};

    { // Main branch fuzzing.
        const commit = if (gh_token == null)
            // Fuzz in-place when no token is specified, as a convenient shortcut for local
            // debugging.
            try run_fuzzers_commit_info(shell)
        else commit: {
            try shell.cwd.makePath("./working/main");
            try shell.pushd("./working/main");
            defer shell.popd();

            break :commit try run_fuzzers_prepare_repository(shell, .main_branch);
        };

        for (std.enums.values(Fuzzer)) |fuzzer| {
            try working_directory.append(if (gh_token == null) "." else "./working/main");
            try seed_record.append(.{
                .commit_timestamp = commit.timestamp,
                .commit_sha = commit.sha,
                .fuzzer = @tagName(fuzzer),
                .branch = "https://github.com/tigerbeetle/tigerbeetle",
            });
        }
    }
    const task_main_count: u32 = @intCast(seed_record.items.len);

    if (gh_token != null) {
        // Any PR labeled like 'fuzz lsm_tree'
        const GhPullRequest = struct {
            const Label = struct {
                id: []const u8,
                name: []const u8,
                description: []const u8,
                color: []const u8,
            };
            number: u32,
            labels: []Label,
        };

        const pr_list_text = try shell.exec_stdout(
            "gh pr list --state open --json number,labels",
            .{},
        );
        const pr_list = try std.json.parseFromSliceLeaky(
            []GhPullRequest,
            shell.arena.allocator(),
            pr_list_text,
            .{},
        );

        for (pr_list) |pr| {
            for (pr.labels) |label| {
                if (stdx.cut(label.name, "fuzz ") != null) break;
            } else continue;

            const pr_directory = try shell.fmt("./working/{d}", .{pr.number});
            try shell.cwd.makePath(pr_directory);
            try shell.pushd(pr_directory);
            defer shell.popd();

            const commit = try run_fuzzers_prepare_repository(
                shell,
                .{ .pull_request = pr.number },
            );

            var pr_fuzzers_count: u32 = 0;
            for (std.enums.values(Fuzzer)) |fuzzer| {
                const labeled = for (pr.labels) |label| {
                    if (stdx.cut(label.name, "fuzz ")) |cut| {
                        if (std.mem.eql(u8, cut.suffix, @tagName(fuzzer))) {
                            break true;
                        }
                    }
                } else false;

                if (labeled or fuzzer == .canary) {
                    pr_fuzzers_count += 1;
                    try working_directory.append(pr_directory);
                    try seed_record.append(.{
                        .commit_timestamp = commit.timestamp,
                        .commit_sha = commit.sha,
                        .fuzzer = @tagName(fuzzer),
                        .branch = try shell.fmt(
                            "https://github.com/tigerbeetle/tigerbeetle/pull/{d}",
                            .{pr.number},
                        ),
                    });
                }
            }
            assert(pr_fuzzers_count >= 2); // The canary and at least one different fuzzer.
        }
    }
    const task_pr_count: u32 = @intCast(seed_record.items.len - task_main_count);

    // Split time 50:50 between fuzzing main and fuzzing labeled PRs.
    const weight = try shell.arena.allocator().alloc(u32, working_directory.items.len);
    var weight_main_total: usize = 0;
    var weight_pr_total: usize = 0;
    for (weight[0..task_main_count]) |*weight_main| {
        weight_main.* = @max(task_pr_count, 1);
        weight_main_total += weight_main.*;
    }
    for (weight[task_main_count..]) |*weight_pr| {
        weight_pr.* = @max(task_main_count, 1);
        weight_pr_total += weight_pr.*;
    }
    if (weight_main_total > 0 and weight_pr_total > 0) {
        assert(weight_main_total == weight_pr_total);
    }

    for (weight, seed_record.items) |*weight_ptr, seed| {
        const fuzzer = std.meta.stringToEnum(Fuzzer, seed.fuzzer).?;
        if (fuzzer == .vopr or fuzzer == .vopr_lite or
            fuzzer == .vopr_testing or fuzzer == .vopr_testing_lite)
        {
            weight_ptr.* *= 2; // Bump relative priority of VOPR runs.
        }
    }

    return .{
        .working_directory = working_directory.items,
        .seed_record = seed_record.items,
        .weight = weight,
    };
}

const Commit = struct {
    sha: [40]u8,
    timestamp: u64,
};

// Clones the specified branch or pull request, builds the code and returns the commit that the
// branch/PR resolves to.
fn run_fuzzers_prepare_repository(shell: *Shell, target: union(enum) {
    main_branch,
    pull_request: u32,
}) !Commit {
    const commit = switch (target) {
        .main_branch => commit: {
            // NB: for the main branch, carefully checkout the commit of the CFO itself, and not
            // just the current tip of the branch. This way, it is easier to atomically adjust
            // fuzzers and CFO.
            const commit = try run_fuzzers_commit_info(shell);
            try shell.exec("git clone https://github.com/tigerbeetle/tigerbeetle .", .{});
            try shell.exec(
                "git switch --detach {commit}",
                .{ .commit = @as([]const u8, &commit.sha) },
            );
            break :commit commit;
        },
        .pull_request => |pr_number| commit: {
            try shell.exec("git clone https://github.com/tigerbeetle/tigerbeetle .", .{});
            try shell.exec(
                "git fetch origin refs/pull/{pr_number}/head",
                .{ .pr_number = pr_number },
            );
            try shell.exec("git switch --detach FETCH_HEAD", .{});
            break :commit try run_fuzzers_commit_info(shell);
        },
    };
    return commit;
}

fn run_fuzzers_commit_info(shell: *Shell) !Commit {
    const commit_sha: [40]u8 = commit_sha: {
        const commit_str = try shell.exec_stdout("git rev-parse HEAD", .{});
        assert(commit_str.len == 40);
        break :commit_sha commit_str[0..40].*;
    };
    const commit_timestamp = commit_timestamp: {
        const timestamp = try shell.exec_stdout(
            "git show -s --format=%ct {sha}",
            .{ .sha = @as([]const u8, &commit_sha) },
        );
        break :commit_timestamp try std.fmt.parseInt(u64, timestamp, 10);
    };
    return .{ .sha = commit_sha, .timestamp = commit_timestamp };
}

fn upload_results(
    shell: *Shell,
    gpa: std.mem.Allocator,
    token: []const u8,
    seeds_new: []const SeedRecord,
) !void {
    log.info("uploading {} seeds", .{seeds_new.len});

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

        const seeds_old = try SeedRecord.from_json(arena.allocator(), data);
        const seeds_merged = try SeedRecord.merge(arena.allocator(), .{}, seeds_old, seeds_new);
        const json = try SeedRecord.to_json(arena.allocator(), seeds_merged);
        try shell.cwd.writeFile(.{ .sub_path = "./fuzzing/data.json", .data = json });
        try shell.exec("git add ./fuzzing/data.json", .{});
        try shell.git_env_setup();
        try shell.exec("git commit -m 🌱", .{});
        if (shell.exec("git push", .{})) {
            log.info("seeds updated", .{});
            break;
        } else |_| {
            log.info("conflict, retrying", .{});
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

    commit_timestamp: u64,
    commit_sha: [40]u8,
    // NB: Use []const u8 rather than Fuzzer to support deserializing unknown fuzzers.
    fuzzer: []const u8,
    ok: bool = false,
    // Counts the number of seeds merged into the current one.
    count: u32 = 1,
    seed_timestamp_start: u64 = 0,
    seed_timestamp_end: u64 = 0,
    seed: u64 = 0,
    // The following fields are excluded from comparison:
    command: []const u8 = "",
    // Branch is an GitHub URL. It only affects the UI, where the seeds are grouped by the branch.
    branch: []const u8,

    fn order(a: SeedRecord, b: SeedRecord) std.math.Order {
        return order_by_field(b.commit_timestamp, a.commit_timestamp) orelse // NB: reverse order.
            order_by_field(a.commit_sha, b.commit_sha) orelse
            order_by_field(a.fuzzer, b.fuzzer) orelse
            order_by_field(a.ok, b.ok) orelse
            order_by_field(b.count, a.count) orelse // NB: reverse order.
            order_by_field(a.seed_duration(), b.seed_duration()) orelse // Coarse seed minimization.
            order_by_seed_timestamp_start(a, b) orelse
            order_by_field(a.seed_timestamp_end, b.seed_timestamp_end) orelse
            order_by_field(a.seed, b.seed) orelse
            .eq;
    }

    fn order_by_field(lhs: anytype, rhs: @TypeOf(lhs)) ?std.math.Order {
        const full_order = switch (@TypeOf(lhs)) {
            []const u8 => std.mem.order(u8, lhs, rhs),
            [40]u8 => std.mem.order(u8, &lhs, &rhs),
            bool => std.math.order(@intFromBool(lhs), @intFromBool(rhs)),
            Fuzzer => std.math.order(@intFromEnum(lhs), @intFromEnum(rhs)),
            else => std.math.order(lhs, rhs),
        };
        return if (full_order == .eq) null else full_order;
    }

    fn order_by_seed_timestamp_start(a: SeedRecord, b: SeedRecord) ?std.math.Order {
        // For canaries, prefer newer seeds to show that the canary is alive.
        // For other fuzzers, prefer older seeds to keep them stable.
        return if (std.mem.eql(u8, a.fuzzer, "canary"))
            order_by_field(b.seed_timestamp_start, a.seed_timestamp_start)
        else
            order_by_field(a.seed_timestamp_start, b.seed_timestamp_start);
    }

    fn less_than(_: void, a: SeedRecord, b: SeedRecord) bool {
        return a.order(b) == .lt;
    }

    fn seed_duration(record: SeedRecord) u64 {
        return record.seed_timestamp_end - record.seed_timestamp_start;
    }

    fn from_json(arena: std.mem.Allocator, json_str: []const u8) ![]SeedRecord {
        return try std.json.parseFromSliceLeaky([]SeedRecord, arena, json_str, .{});
    }

    fn to_json(arena: std.mem.Allocator, records: []const SeedRecord) ![]const u8 {
        return try std.json.stringifyAlloc(arena, records, .{
            .whitespace = .indent_2,
        });
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
    ) ![]const SeedRecord {
        const current_and_new = try std.mem.concat(arena, SeedRecord, &.{ current, new });
        std.mem.sort(SeedRecord, current_and_new, {}, SeedRecord.less_than);

        var result = try std.ArrayList(SeedRecord).initCapacity(arena, current.len);

        var commit_sha_previous: ?[40]u8 = null;
        var commit_count: u32 = 0;

        var fuzzer_previous: ?[]const u8 = null;

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
                !std.mem.eql(u8, fuzzer_previous.?, record.fuzzer))
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
            } else {
                if (record.ok) {
                    // Merge counts with the first ok record for this fuzzer/commit, to make it
                    // easy for the front-end to show the total count by displaying just the first
                    // record
                    var last_ok_index = result.items.len;
                    while (last_ok_index > 0 and
                        result.items[last_ok_index - 1].ok and
                        std.mem.eql(u8, result.items[last_ok_index - 1].fuzzer, record.fuzzer) and
                        std.meta.eql(result.items[last_ok_index - 1].commit_sha, record.commit_sha))
                    {
                        last_ok_index -= 1;
                    }
                    if (last_ok_index != result.items.len) {
                        result.items[last_ok_index].count += record.count;
                    }
                }
            }
        }

        return result.items;
    }
};

const Snap = @import("../testing/snaptest.zig").Snap;
const snap = Snap.snap;

test "cfo: deserialization" {
    // Smoke test that we can still deserialize&migrate old devhub data.
    // Handy when adding new fields!
    const old_json =
        \\[{
        \\    "commit_timestamp": 1721095881,
        \\    "commit_sha": "c4bb1eaa658b77c37646d3854dd911adba71b764",
        \\    "fuzzer": "canary",
        \\    "ok": false,
        \\    "seed_timestamp_start": 1721096948,
        \\    "seed_timestamp_end": 1721096949,
        \\    "seed": 17154947449604939200,
        \\    "command": "./zig/zig build -Drelease fuzz -- canary 17154947449604939200",
        \\    "branch": "https://github.com/tigerbeetle/tigerbeetle/pull/2104",
        \\    "count": 1
        \\}]
    ;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const old_records = try SeedRecord.from_json(arena.allocator(), old_json);

    const new_records = try SeedRecord.merge(arena.allocator(), .{}, old_records, &.{});
    const new_json = try SeedRecord.to_json(arena.allocator(), new_records);

    try snap(@src(),
        \\[
        \\  {
        \\    "commit_timestamp": 1721095881,
        \\    "commit_sha": "c4bb1eaa658b77c37646d3854dd911adba71b764",
        \\    "fuzzer": "canary",
        \\    "ok": false,
        \\    "count": 1,
        \\    "seed_timestamp_start": 1721096948,
        \\    "seed_timestamp_end": 1721096949,
        \\    "seed": 17154947449604939200,
        \\    "command": "./zig/zig build -Drelease fuzz -- canary 17154947449604939200",
        \\    "branch": "https://github.com/tigerbeetle/tigerbeetle/pull/2104"
        \\  }
        \\]
    ).diff(new_json);
}

test "cfo: SeedRecord.merge" {
    const T = struct {
        fn check(current: []const SeedRecord, new: []const SeedRecord, want: Snap) !void {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();

            const options = SeedRecord.MergeOptions{
                .commit_count_max = 2,
                .seed_count_max = 2,
            };
            const got = try SeedRecord.merge(arena.allocator(), options, current, new);
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
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
            //  Second commit, two successes.
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 2,
                .seed_timestamp_end = 2,
                .seed = 2,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        &.{
            // Two new failures for the first commit, one will be added.
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 2,
                .seed_timestamp_end = 2,
                .seed = 2,
                .command = "fuzz ewah",
                .branch = "main",
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 3,
                .seed_timestamp_end = 3,
                .seed = 3,
                .command = "fuzz ewah",
                .branch = "main",
            },
            // One failure for the second commit, it will replace one success.
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 4,
                .seed_timestamp_end = 4,
                .seed = 4,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 4,
            \\    "seed_timestamp_end": 4,
            \\    "seed": 4,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "count": 2,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 2,
            \\    "seed_timestamp_end": 2,
            \\    "seed": 2,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
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
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
            .{
                .commit_timestamp = 2,
                .commit_sha = .{'2'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        &.{
            // A new successful commit displaces the older failure.
            .{
                .commit_timestamp = 3,
                .commit_sha = .{'3'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 3,
            \\    "commit_sha": "3333333333333333333333333333333333333333",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 2,
            \\    "commit_sha": "2222222222222222222222222222222222222222",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
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
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );

    // Prefer older seeds rather than smaller seeds.
    try T.check(
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 10,
                .seed_timestamp_end = 10,
                .seed = 10,
                .command = "fuzz ewah",
                .branch = "main",
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 20,
                .seed_timestamp_end = 20,
                .seed = 20,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 5,
                .seed_timestamp_end = 5,
                .seed = 999,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 5,
            \\    "seed_timestamp_end": 5,
            \\    "seed": 999,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 10,
            \\    "seed_timestamp_end": 10,
            \\    "seed": 10,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );

    // Prefer newer seeds for canary (special case).
    try T.check(
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "canary",
                .ok = false,
                .seed_timestamp_start = 10,
                .seed_timestamp_end = 10,
                .seed = 3,
                .command = "fuzz canary",
                .branch = "main",
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "canary",
                .ok = false,
                .seed_timestamp_start = 30,
                .seed_timestamp_end = 30,
                .seed = 2,
                .command = "fuzz canary",
                .branch = "main",
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "canary",
                .ok = false,
                .seed_timestamp_start = 20,
                .seed_timestamp_end = 20,
                .seed = 1,
                .command = "fuzz canary",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "canary",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 30,
            \\    "seed_timestamp_end": 30,
            \\    "seed": 2,
            \\    "command": "fuzz canary",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "canary",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 20,
            \\    "seed_timestamp_end": 20,
            \\    "seed": 1,
            \\    "command": "fuzz canary",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );

    // Tolerates unknown fuzzers
    try T.check(
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "American Fuzzy Lop",
                .ok = false,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "very fluffy",
                .branch = "main",
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "American Fuzzy Lop",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "very fluffy",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": false,
            \\    "count": 1,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );

    // Sums up counts
    try T.check(
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 1,
                .command = "fuzz ewah",
                .branch = "main",
                .count = 2,
            },
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 2,
                .command = "fuzz ewah",
                .branch = "main",
                .count = 1,
            },
        },
        &.{
            .{
                .commit_timestamp = 1,
                .commit_sha = .{'1'} ** 40,
                .fuzzer = "ewah",
                .ok = true,
                .seed_timestamp_start = 1,
                .seed_timestamp_end = 1,
                .seed = 3,
                .command = "fuzz ewah",
                .branch = "main",
                .count = 3,
            },
        },
        snap(@src(),
            \\[
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "count": 4,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 3,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  },
            \\  {
            \\    "commit_timestamp": 1,
            \\    "commit_sha": "1111111111111111111111111111111111111111",
            \\    "fuzzer": "ewah",
            \\    "ok": true,
            \\    "count": 2,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 1,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );
}
