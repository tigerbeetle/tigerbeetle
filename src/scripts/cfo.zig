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
//! The fuzzing loop runs for `args.budget`. Any fuzzer that runs for longer than
//! `args.timeout` is terminated and recorded as a failure. At the end of the fuzzing loop,
//! any fuzzers that are still running are cancelled. Cancelled seeds are not recorded.
//!
//! Note that the budget/refresh timers do not count time spent cloning or compiling code.
//!
//! The CFO uses Linux's process namespaces to ensure that all descendant processes are reaped.
//!
//! Every `args.refresh`, and at the end of the fuzzing loop:
//! 1. CFO collects a list of seeds (some of which are failing),
//! 2. merges this list into the previous set of seeds,
//! 3. pushes the new list to https://github.com/tigerbeetle/devhubdb/
//!
//! Rules for merging:
//!
//! - Keep seeds for at most `commit_count_max` distinct commits.
//! - Prefer fresher commits (based on commit time stamp).
//! - For each commit and fuzzer combination, keep at most `seed_count_max` seeds.
//! - Prefer failing seeds to successful seeds.
//! - Prefer seeds that failed faster.
//! - Prefer older seeds.
//! - When dropping a non-failing seed, add its count to some other non-failing seeds.
//!
//! The idea here is that we want to keep the set of failing seeds stable, while maintaining some
//! measure of how much fuzzing work was done in total.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;
const maybe = stdx.maybe;

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const MiB = stdx.MiB;
const log_size_max = 256 * stdx.KiB;

pub const CLIArgs = struct {
    /// How long to run the cfo before exiting (so that cfo_supervisor can refresh our code).
    budget: stdx.Duration = .minutes(60),
    /// The interval for flushing accumulated seeds to the devhub.
    /// In addition to this interval, any remaining seeds will be uploaded at the end of the budget.
    refresh: stdx.Duration = .minutes(5),
    /// A fuzzer which takes longer than this timeout is killed and counts as a failure.
    timeout: stdx.Duration = .minutes(30),
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
    message_bus,
    storage,
    vopr_debug,
    vopr_lite,
    vopr_testing_lite,
    vopr_testing,
    vopr,
    vortex,
    vsr_free_set,
    vsr_superblock_quorums,
    vsr_superblock,
    vsr_multi_batch,
    signal,
    state_machine,

    const weights = std.enums.EnumArray(Fuzzer, u32).init(.{
        .canary = 1,
        .ewah = 1,
        .lsm_cache_map = 2,
        .lsm_forest = 4,
        .lsm_manifest_level = 2,
        .lsm_manifest_log = 2,
        .lsm_scan = 2,
        .lsm_segmented_array = 1,
        .lsm_tree = 2,
        .message_bus = 2,
        .storage = 1,
        .vopr_debug = 1,
        .vopr_lite = 8,
        .vopr_testing_lite = 8,
        .vopr_testing = 8,
        .vopr = 8,
        .vortex = 1,
        .vsr_free_set = 1,
        .vsr_superblock_quorums = 1,
        .vsr_superblock = 1,
        .vsr_multi_batch = 1,
        .signal = 1,
        .state_machine = 2,
    });

    fn args_build(comptime fuzzer: Fuzzer) []const []const u8 {
        return comptime switch (fuzzer) {
            .vopr_debug,
            => &.{"vopr:build"},
            .vopr,
            .vopr_lite,
            => &.{ "vopr:build", "-Drelease" },
            .vopr_testing,
            .vopr_testing_lite,
            => &.{ "vopr:build", "-Drelease", "-Dvopr-state-machine=testing" },
            // TODO Once release builds have stack traces, add -Drelease so that we are testing
            // release binaries (to test as close as possible to a real cluster).
            // (Likewise in args_run()).
            .vortex => &.{"vortex:build"},
            else => &.{ "fuzz:build", "-Drelease" },
        };
    }

    fn args_run(comptime fuzzer: Fuzzer) []const []const u8 {
        return comptime switch (fuzzer) {
            .vopr_debug => .{"vopr"},
            .vopr, .vopr_lite => .{ "vopr", "-Drelease" },
            .vopr_testing,
            .vopr_testing_lite,
            => .{ "vopr", "-Drelease", "-Dvopr-state-machine=testing" },
            .vortex => .{"vortex"},
            else => .{ "fuzz", "-Drelease" },
        } ++ .{"--"} ++ args_exec(fuzzer);
    }

    fn args_exec(comptime fuzzer: Fuzzer) []const []const u8 {
        return comptime switch (fuzzer) {
            .vopr, .vopr_debug, .vopr_testing => &.{},
            .vopr_lite, .vopr_testing_lite => &.{"--lite"},
            .vortex => &.{
                "supervisor",
                "--log-debug",
                "--replica-count=3",
                "--test-duration=10m",
            },
            else => |f| &.{@tagName(f)},
        };
    }

    fn capture_logs(fuzzer: Fuzzer) bool {
        return fuzzer == .vortex;
    }
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("cfo is not supported on Windows", .{});
        return error.NotSupported;
    }

    if (cli_args.budget.ns == 0) fatal("--budget: must be greater than zero", .{});
    if (cli_args.refresh.ns == 0) fatal("--refresh: must be greater than zero", .{});
    if (cli_args.timeout.ns == 0) fatal("--timeout: must be greater than zero", .{});

    if (cli_args.budget.ns < cli_args.timeout.ns) {
        log.warn("budget={} is less than timeout={}; no seeds will time out", .{
            cli_args.budget,
            cli_args.timeout,
        });
    }

    log.info("start {}", .{stdx.DateTimeUTC.now()});
    defer log.info("end {}", .{stdx.DateTimeUTC.now()});

    try shell.exec("git --version", .{});

    // Read-write token for <https://github.com/tigerbeetle/devhubdb>.
    // See `src/scripts/devhub.zig` for generation instructions.
    const devhub_token_option = shell.env_get_option("DEVHUBDB_PAT");
    if (devhub_token_option == null) {
        log.err("'DEVHUBDB_PAT' environmental variable is not set, will not upload results", .{});
    }

    // Readonly token for PR metadata of <https://github.com/tigerbeetle/tigerbeetle>.
    const gh_token_option = shell.env_get_option("GH_TOKEN");
    if (gh_token_option == null) {
        log.err("'GH_TOKEN' environmental variable is not set, will not fetch pull requests", .{});
    } else {
        try shell.exec("gh --version", .{});
    }

    try run_fuzzers(shell, gpa, gh_token_option, .{
        .concurrency = cli_args.concurrency orelse try std.Thread.getCpuCount(),
        .budget = cli_args.budget,
        .refresh = cli_args.refresh,
        .timeout = cli_args.timeout,
        .devhub_token = devhub_token_option,
    });

    log.info("memory = {}B", .{shell.arena.queryCapacity()});
}

/// Format and print an error message to stderr, then exit with an exit code of 1.
fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.process.exit(1);
}

fn run_fuzzers(
    shell: *Shell,
    gpa: std.mem.Allocator,
    gh_token: ?[]const u8,
    options: struct {
        concurrency: usize,
        budget: stdx.Duration,
        refresh: stdx.Duration,
        timeout: stdx.Duration,
        devhub_token: ?[]const u8,
    },
) !void {
    var seeds = std.ArrayListUnmanaged(SeedRecord){};
    defer seeds.deinit(gpa);

    var seed_logs = std.ArrayListUnmanaged(?[]const u8){};
    defer {
        for (seed_logs.items) |log_or_null| {
            if (log_or_null) |log_buffer| gpa.free(log_buffer);
        }
        seed_logs.deinit(gpa);
    }

    const random = std.crypto.random;

    const FuzzerChild = struct {
        fuzzer: Fuzzer,
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

    const children_logs = try shell.arena.allocator().alloc(LogTail, options.concurrency);
    for (children_logs) |*child_log| {
        child_log.* = try LogTail.init(shell.arena.allocator(), log_size_max);
    }

    var read_buffer = try gpa.alloc(u8, log_size_max);
    defer gpa.free(read_buffer);

    var tasks = Tasks.init(shell.arena.allocator());
    defer tasks.deinit();

    var budget_timer = try std.time.Timer.start();
    const sleep_ns = 100 * std.time.ns_per_ms;

    var refresh_timer = try std.time.Timer.start();
    var refresh_first = true;

    while (true) {
        const iteration_refresh = refresh_first or refresh_timer.read() >= options.refresh.ns;
        if (iteration_refresh) refresh_timer.reset();

        const iteration_last = budget_timer.read() >= options.budget.ns;
        const iteration_push = (iteration_refresh and !refresh_first) or iteration_last;
        refresh_first = false;

        if (iteration_refresh) {
            try run_fuzzers_prepare_tasks(&tasks, shell, gh_token);

            for (tasks.list.items) |*task| {
                if (task.generation == tasks.generation) {
                    log.info(
                        "fuzzing commit={s} timestamp={} fuzzer={s} branch='{s}' weight={}",
                        .{
                            task.seed_template.commit_sha[0..7],
                            task.seed_template.commit_timestamp,
                            @tagName(task.seed_template.fuzzer),
                            task.seed_template.branch_url,
                            task.weight,
                        },
                    );
                }
            }
        }

        // Start new fuzzer processes.
        for (children) |*child_or_null| {
            if (child_or_null.* == null) {
                const task = tasks.sample();
                const seed = random.int(u64);

                // Ensure that multiple fuzzers spawned in the same tick are spread out over tasks.
                task.runtime_virtual += @divFloor(sleep_ns, task.weight);

                const child = try run_fuzzers_start_fuzzer(shell, .{
                    .working_directory = task.working_directory,
                    .fuzzer = task.seed_template.fuzzer,
                    .seed = seed,
                });
                // NB: take timestamp after spawning to exclude build time.
                const seed_timestamp_start: u64 = @intCast(std.time.nanoTimestamp());

                child_or_null.* = .{
                    .fuzzer = task.seed_template.fuzzer,
                    .child = child.process,
                    .seed = .{
                        .commit_timestamp = task.seed_template.commit_timestamp,
                        .commit_sha = task.seed_template.commit_sha,
                        .fuzzer = @tagName(task.seed_template.fuzzer),
                        .branch = task.seed_template.branch_url,

                        .count = 1,
                        .seed_timestamp_start = seed_timestamp_start,
                        .seed = seed,
                        .command = child.command,

                        .ok = false,
                        .seed_timestamp_end = 0,
                    },
                };
            }
        }

        // Wait before polling for completion, to avoid hogging the CPU.
        std.time.sleep(sleep_ns);

        // Flush stderr/stdout into the log tail.
        for (children, children_logs) |*fuzzer_or_null, *fuzzer_log| {
            if (fuzzer_or_null.*) |*fuzzer| {
                assert((fuzzer.child.stdout == null) != fuzzer.fuzzer.capture_logs());
                assert((fuzzer.child.stderr == null) != fuzzer.fuzzer.capture_logs());
                for (&[_]?std.fs.File{
                    fuzzer.child.stdout,
                    fuzzer.child.stderr,
                }) |stream_or_null| {
                    if (stream_or_null) |stream| {
                        var read_size: ?usize = null;
                        while (read_size == null or read_size.? > 0) {
                            read_size = stream.read(read_buffer) catch |err| switch (err) {
                                error.WouldBlock => break,
                                else => std.debug.panic("error: {}", .{err}),
                            };
                            fuzzer_log.append(read_buffer[0..read_size.?]);
                        }
                    }
                }
            }
        }

        var running_count: u32 = 0;
        for (children, children_logs) |*fuzzer_or_null, *fuzzer_log| {
            // Poll for completed fuzzers.

            if (fuzzer_or_null.*) |*fuzzer| {
                running_count += 1;

                // Update runtime_virtual incrementally every tick so that we have an accurate score
                // for choosing new tasks.
                const task = tasks.get(
                    std.meta.stringToEnum(Fuzzer, fuzzer.seed.fuzzer).?,
                    fuzzer.seed.commit_sha,
                    fuzzer.seed.branch,
                ).?;
                task.runtime_virtual += @divFloor(sleep_ns, task.weight);

                var fuzzer_done = false;
                _ = fuzzer.child.stdin.?.write(&.{1}) catch |err| {
                    switch (err) {
                        error.WouldBlock => {},
                        error.BrokenPipe => fuzzer_done = true,
                        else => return err,
                    }
                };

                const seed_timestamp_start_ns = fuzzer.seed.seed_timestamp_start;
                const seed_duration_ns =
                    @as(u64, @intCast(std.time.nanoTimestamp())) - seed_timestamp_start_ns;
                const seed_expired = !fuzzer_done and seed_duration_ns > options.timeout.ns;

                if (fuzzer_done or seed_expired or iteration_last) {
                    log.debug("will reap '{s}' after {}ms{s}", .{
                        fuzzer.seed.command,
                        @divFloor(seed_duration_ns, std.time.ns_per_ms),
                        if (fuzzer_done) "" else " (timeout)",
                    });

                    const term = try if (fuzzer_done) fuzzer.child.wait() else fuzzer.child.kill();
                    const term_adapted: enum { sigkill, sigterm, other } = term: {
                        const code_kill = 128 + std.posix.SIG.KILL;
                        const code_term = 128 + std.posix.SIG.TERM;
                        const vortex = fuzzer.fuzzer == .vortex;

                        if (std.meta.eql(term, .{ .Signal = std.posix.SIG.KILL }) or
                            (std.meta.eql(term, .{ .Exited = @intCast(code_kill) }) and vortex))
                        {
                            // Something killed the fuzzer. This is likely OOM, so count this seed
                            // neither as a success, nor as a failure.
                            //
                            // Special case Vortex because one of Vortex's child processes might
                            // have been SIGKILL'd, which the supervisor bubbles up as 128+SIGKILL.
                            // (It can't just SIGKILL itself because the supervisor is immune to
                            // internal SIGKILL as it is the init process of a pid namespace.)
                            break :term .sigkill;
                        }

                        if (iteration_last) {
                            if (std.meta.eql(term, .{ .Signal = std.posix.SIG.TERM }) or
                                (std.meta.eql(term, .{ .Exited = @intCast(code_term) }) and vortex))
                            {
                                // We killed the fuzzer because our budgeted time is expired, but
                                // the seed itself is indeterminate.
                                //
                                // Special case Vortex because it uses a handler for SIGTERM to
                                // ensure the supervisor's pid namespace is torn down.
                                break :term .sigterm;
                            }
                        }
                        break :term .other;
                    };

                    switch (term_adapted) {
                        .sigkill => log.info("ignored SIGKILL for '{s}'", .{fuzzer.seed.command}),
                        .sigterm => log.info("ignored SIGTERM for '{s}'", .{fuzzer.seed.command}),
                        .other => {
                            var seed_record = fuzzer.seed;
                            seed_record.ok = std.meta.eql(term, .{ .Exited = 0 });
                            // Convert seed_timestamp_start to seconds as `devhub.js` relies on it.
                            seed_record.seed_timestamp_start = @divFloor(
                                seed_timestamp_start_ns,
                                std.time.ns_per_s,
                            );
                            seed_record.seed_timestamp_end = @intCast(std.time.timestamp());
                            if (!seed_record.ok) {
                                seed_record.debug = try shell.fmt("{}", .{term});
                            }

                            if (seed_record.ok or !fuzzer.fuzzer.capture_logs()) {
                                try seed_logs.append(gpa, null);
                            } else {
                                const log_data = try gpa.alloc(u8, fuzzer_log.size());
                                errdefer gpa.free(log_data);

                                fuzzer_log.write_to(log_data);
                                try seed_logs.append(gpa, log_data);
                                seed_record.log = try create_log_path(shell.arena.allocator());
                            }
                            try seeds.append(gpa, seed_record);
                        },
                    }

                    if (std.meta.eql(term, .{ .Signal = std.posix.SIG.ABRT })) {
                        // Sanity-check that we definitely record all assertion failures.
                        assert(!seeds.getLast().ok);
                    }
                    task.runtime_total_ns += seed_duration_ns;

                    fuzzer_log.clear();
                    fuzzer_or_null.* = null;
                }
            }
        }
        assert(running_count == options.concurrency);

        if (iteration_push) {
            if (options.devhub_token) |token| {
                try upload_results(shell, gpa, token, seeds.items, seed_logs.items);
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
            for (seed_logs.items) |log_or_null| {
                if (log_or_null) |log_buffer| gpa.free(log_buffer);
            }
            seed_logs.clearRetainingCapacity();
            seeds.clearRetainingCapacity();
        }

        if (iteration_last) break;
    }
    assert(seeds.items.len == 0);

    var runtime_total_ns: u64 = 0;
    for (tasks.list.items) |*task| runtime_total_ns += task.runtime_total_ns;
    for (tasks.list.items) |*task| {
        log.info("commit={s} fuzzer={s:<24} runtime={}s {d:.2}% (active={})", .{
            task.seed_template.commit_sha[0..7],
            @tagName(task.seed_template.fuzzer),
            @divFloor(task.runtime_total_ns, std.time.ns_per_s),
            @as(f64, @floatFromInt(task.runtime_total_ns * 100)) /
                @as(f64, @floatFromInt(runtime_total_ns)),
            task.generation == tasks.generation,
        });
    }
}

const Tasks = struct {
    /// Map values index into `list`.
    const Map = std.AutoHashMap(struct {
        fuzzer: Fuzzer,
        commit: [40]u8,
        branch: SeedRecord.Template.Branch,
    }, usize);
    const List = std.ArrayList(Task);

    const Task = struct {
        // Immutable:

        working_directory: []const u8,
        seed_template: SeedRecord.Template,

        // Mutable:

        /// Higher weight fuzzers are given more runtime.
        weight: u32,
        /// Active tasks have `task.generation == tasks.generation`.
        /// Inactive tasks have `task.generation < tasks.generation`.
        generation: u64,
        /// This is just used for logging, not scheduling.
        runtime_total_ns: u64,
        /// Weight-adjusted runtime used for scheduling. Always positive and finite.
        /// Cumulative `runtime / weight`, but since `weight` can change over time, this is more
        /// precise.
        runtime_virtual: u64,
    };

    generation: u64 = 1,
    runtime_virtual_init: u64 = 1,

    list: List,
    map: Map,

    pub fn init(allocator: std.mem.Allocator) Tasks {
        return .{
            .list = Tasks.List.init(allocator),
            .map = Tasks.Map.init(allocator),
        };
    }

    pub fn deinit(tasks: *Tasks) void {
        tasks.map.deinit();
        tasks.list.deinit();
        tasks.* = undefined;
    }

    pub fn verify(tasks: *const Tasks) void {
        assert(tasks.list.items.len == tasks.map.count());

        var map_iterator = tasks.map.iterator();
        while (map_iterator.next()) |map_entry| {
            const task_index = map_entry.value_ptr.*;
            const task = &tasks.list.items[task_index];
            assert(@intFromPtr(task) >= @intFromPtr(tasks.list.items.ptr));
            assert(@intFromPtr(task) < @intFromPtr(tasks.list.items.ptr) +
                @sizeOf(Task) * tasks.list.items.len);
            assert(task.seed_template.fuzzer == map_entry.key_ptr.fuzzer);
            assert(std.mem.eql(u8, &task.seed_template.commit_sha, &map_entry.key_ptr.commit));
        }

        for (tasks.list.items) |*task| {
            assert(task.generation <= tasks.generation);
            assert(task.weight > 0);
            assert(task.runtime_virtual >= 1);
        }
    }

    /// Pick a task to run next.
    /// Returns the task with the minimal virtual runtime.
    /// Break ties by choosing the task with the greater weight.
    pub fn sample(tasks: *const Tasks) *Task {
        assert(tasks.list.items.len == tasks.map.count());
        assert(tasks.list.items.len > 0);

        var task_best: ?*Task = null;
        for (tasks.list.items) |*task| {
            assert(task.runtime_virtual > 0);
            assert(task.generation <= tasks.generation);

            if (task.generation == tasks.generation) {
                if (task_best == null or
                    task_best.?.runtime_virtual > task.runtime_virtual or
                    (task_best.?.runtime_virtual == task.runtime_virtual and
                        task_best.?.weight < task.weight))
                {
                    task_best = task;
                }
            }
        }
        return task_best.?;
    }

    pub fn get(
        tasks: *const Tasks,
        fuzzer: Fuzzer,
        commit: [40]u8,
        branch_url: []const u8,
    ) ?*Task {
        const branch = SeedRecord.Template.Branch.parse(branch_url) catch unreachable;
        const task_index = tasks.map.get(.{
            .fuzzer = fuzzer,
            .commit = commit,
            .branch = branch,
        }) orelse return null;
        return &tasks.list.items[task_index];
    }

    /// Either:
    /// - If the specified task does not already exist, create it.
    /// - Is the specified task does already exist, activate it for the new generation.
    pub fn put(
        tasks: *Tasks,
        working_directory: []const u8,
        seed_template: SeedRecord.Template,
    ) !void {
        const branch = SeedRecord.Template.Branch.parse(seed_template.branch_url) catch unreachable;
        if (tasks.map.get(.{
            .fuzzer = seed_template.fuzzer,
            .commit = seed_template.commit_sha,
            .branch = branch,
        })) |task_existing_index| {
            const task_existing = &tasks.list.items[task_existing_index];
            assert(task_existing.generation < tasks.generation);
            assert(task_existing.seed_template.fuzzer == seed_template.fuzzer);
            assert(std.mem.eql(u8, task_existing.working_directory, working_directory));

            if (tasks.runtime_virtual_init < task_existing.runtime_virtual) {
                tasks.runtime_virtual_init = task_existing.runtime_virtual;
            } else {
                if (task_existing.generation == tasks.generation - 1) {
                    // For tasks that were already active, leave their low `runtime_virtual`
                    // unmodified, to ensure they get some runtime soon.
                } else {
                    // For tasks which were active in the past, but not in the latest generation,
                    // ensure that they are not starved in the new generation.
                    task_existing.runtime_virtual = tasks.runtime_virtual_init;
                }
            }
            task_existing.generation = tasks.generation;
        } else {
            try tasks.list.append(.{
                .working_directory = working_directory,
                .seed_template = seed_template,
                .generation = tasks.generation,
                .weight = 0, // To be initialized later.
                .runtime_total_ns = 0,
                .runtime_virtual = tasks.runtime_virtual_init,
            });

            try tasks.map.putNoClobber(.{
                .fuzzer = seed_template.fuzzer,
                .commit = seed_template.commit_sha,
                .branch = branch,
            }, tasks.list.items.len - 1);
        }
    }

    pub fn soft_remove_all(tasks: *Tasks) void {
        tasks.verify();
        tasks.generation += 1;
    }
};

fn run_fuzzers_prepare_tasks(tasks: *Tasks, shell: *Shell, gh_token: ?[]const u8) !void {
    tasks.soft_remove_all();
    defer tasks.verify();

    for ([2]SeedRecord.Template.Branch{ .main, .release }) |branch| {
        if (branch == .release and gh_token == null) continue;

        const working_directory = if (gh_token == null)
            "."
        else
            try shell.fmt("./working/{s}", .{@tagName(branch)});
        const commit = if (gh_token == null)
            // Fuzz in-place when no token is specified, as a convenient shortcut for local
            // debugging.
            try run_fuzzers_commit_info(shell)
        else commit: {
            try shell.cwd.makePath(working_directory);
            try shell.pushd(working_directory);
            defer shell.popd();

            // Fuzz an independent clone of the repository, so that CFO and the fuzzer could be on
            // different branches (to fuzz PRs and releases).
            break :commit try run_fuzzers_prepare_repository(shell, .{
                .branch = @tagName(branch),
            });
        };

        // Only add fuzzers that also exist on the branch we are fuzzing.
        const branch_cfo = try shell.cwd.readFileAlloc(
            shell.arena.allocator(),
            try shell.fmt("{s}/src/scripts/cfo.zig", .{working_directory}),
            1 * MiB,
        );

        for (std.enums.values(Fuzzer)) |fuzzer| {
            const fuzzer_present_on_branch = switch (fuzzer) {
                .vopr, .vopr_debug, .vopr_lite, .vopr_testing, .vopr_testing_lite => true,
                else => std.mem.indexOf(
                    u8,
                    branch_cfo,
                    try shell.fmt("    {s},", .{@tagName(fuzzer)}), // A field in const Fuzzer enum.
                ) != null,
            };
            if (fuzzer == .canary) assert(fuzzer_present_on_branch);

            if (fuzzer_present_on_branch) {
                try tasks.put(working_directory, .{
                    .commit_timestamp = commit.timestamp,
                    .commit_sha = commit.sha,
                    .fuzzer = fuzzer,
                    .branch = branch,
                    .branch_url = switch (branch) {
                        .main => "https://github.com/tigerbeetle/tigerbeetle",
                        .release => "https://github.com/tigerbeetle/tigerbeetle/tree/release",
                        else => unreachable,
                    },
                });
            }
        }
    }

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
                    if (stdx.cut_prefix(label.name, "fuzz ")) |suffix| {
                        if (std.mem.eql(u8, suffix, @tagName(fuzzer))) {
                            break true;
                        }
                    }
                } else false;

                if (labeled or fuzzer == .canary) {
                    pr_fuzzers_count += 1;
                    try tasks.put(pr_directory, .{
                        .commit_timestamp = commit.timestamp,
                        .commit_sha = commit.sha,
                        .fuzzer = fuzzer,
                        .branch = .{ .pull = pr.number },
                        .branch_url = try shell.fmt(
                            "https://github.com/tigerbeetle/tigerbeetle/pull/{d}",
                            .{pr.number},
                        ),
                    });
                }
            }
            assert(pr_fuzzers_count >= 2); // The canary and at least one different fuzzer.
        }
    }

    {
        // Assign task weights:
        // - Start by splitting the budget 40:40:20 between main, pull requests, and release.
        // - Then, bump relative priority of more important fuzzers like VOPR.
        var task_count_main: u32 = 0;
        var task_count_pull: u32 = 0;
        var task_count_release: u32 = 0;
        for (tasks.list.items) |*task| {
            if (task.generation == tasks.generation) {
                task_count_main += @intFromBool(task.seed_template.branch == .main);
                task_count_pull += @intFromBool(task.seed_template.branch == .pull);
                task_count_release += @intFromBool(task.seed_template.branch == .release);
            }
        }

        // When the release was recently pushed (i.e. usually on Friday), give it extra fuzzing
        // weight for the next few days (i.e. until Monday).
        const release_soon = for (tasks.list.items) |*task| {
            if (task.seed_template.branch == .release) {
                break std.time.timestamp() <
                    task.seed_template.commit_timestamp + 2 * std.time.s_per_day;
            }
        } else false;

        const weight_main = 1_000_000;
        const weight_pull = 1_000_000;
        const weight_release = 500_000 * (if (release_soon) @as(u32, 3) else 1);

        for (tasks.list.items) |*task| {
            if (task.generation == tasks.generation) {
                const multiplier = Fuzzer.weights.get(task.seed_template.fuzzer);
                task.weight = switch (task.seed_template.branch) {
                    .main => @divFloor(weight_main, task_count_main),
                    .pull => @divFloor(weight_pull, task_count_pull),
                    .release => @divFloor(weight_release, task_count_release),
                } * multiplier;
            }
        }
    }
}

const Commit = struct {
    sha: [40]u8,
    timestamp: u64,
};

// Clones the specified branch or pull request, builds the code and returns the commit that the
// branch/PR resolves to.
fn run_fuzzers_prepare_repository(shell: *Shell, target: union(enum) {
    branch: []const u8,
    pull_request: u32,
}) !Commit {
    // When possible, reuse checkouts so that we can also reuse the zig cache.
    if (!try shell.dir_exists(".git")) {
        try shell.exec("git clone https://github.com/tigerbeetle/tigerbeetle .", .{});
    }

    switch (target) {
        .branch => |branch| try shell.exec("git fetch origin {branch}", .{ .branch = branch }),
        .pull_request => |pr| try shell.exec("git fetch origin refs/pull/{pr}/head", .{ .pr = pr }),
    }
    try shell.exec("git switch --detach FETCH_HEAD", .{});
    return run_fuzzers_commit_info(shell);
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

fn run_fuzzers_start_fuzzer(shell: *Shell, options: struct {
    working_directory: []const u8,
    fuzzer: Fuzzer,
    seed: u64,
}) !struct {
    command: []const u8, // User-visible string on devhub.
    process: std.process.Child,
} {
    try shell.pushd(options.working_directory);
    defer shell.popd();

    const arg_count_max = comptime arg_count_max: {
        var arg_max: u32 = 0;
        for (std.enums.values(Fuzzer)) |fuzzer| {
            arg_max = @max(arg_max, fuzzer.args_build().len, fuzzer.args_run().len);
        }
        assert(arg_max > 0);

        // +3: zig/zig build <args> SEED
        break :arg_count_max arg_max + 3;
    };
    var args: stdx.BoundedArrayType([]const u8, arg_count_max) = .{};

    assert(try shell.dir_exists(".git") or shell.file_exists(".git"));

    // DevHub displays `./zig/zig build run` invocation which you can paste in your shell directly.
    // But CFO actually builds and execs in two separate steps such that:
    // - build time is excluded from overall runtime,
    // - the exit status of the fuzzer process can be inspected, to determine if OOM happened.
    args.clear();
    args.push_slice(&.{ "./zig/zig", "build" });
    args.push_slice(switch (options.fuzzer) {
        inline else => |f| comptime f.args_run(),
    });
    var seed_buffer: [32]u8 = undefined;
    args.push(stdx.array_print(32, &seed_buffer, "{d}", .{options.seed}));
    const command = try std.mem.join(shell.arena.allocator(), " ", args.const_slice());

    const exe = exe: {
        args.clear();
        args.push_slice(&.{ "build", "-Dprint-exe" });
        args.push_slice(switch (options.fuzzer) {
            inline else => |f| comptime f.args_build(),
        });
        break :exe shell.exec_stdout("{zig} {args}", .{
            .zig = shell.zig_exe.?,
            .args = args.const_slice(),
        }) catch "false"; // Make sure that subsequent run fails if we can't build.
    };
    assert(exe.len > 0);

    log.debug("will start '{s}'", .{command});
    const process = try shell.spawn(
        .{
            .stdin_behavior = .Pipe,
            .stdout_behavior = if (options.fuzzer.capture_logs()) .Pipe else .Ignore,
            .stderr_behavior = if (options.fuzzer.capture_logs()) .Pipe else .Ignore,
        },
        "{exe} {args} {seed}",
        .{
            .exe = exe,
            .args = switch (options.fuzzer) {
                inline else => |f| comptime f.args_exec(),
            },
            .seed = options.seed,
        },
    );

    if (options.fuzzer.capture_logs()) {
        for (&[_]?std.fs.File{
            process.stdout.?,
            process.stderr.?,
        }) |file| {
            const flags = try std.posix.fcntl(file.?.handle, std.posix.F.GETFL, 0);
            _ = try std.posix.fcntl(
                file.?.handle,
                std.posix.F.SETFL,
                flags | @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true })),
            );
        }
    }

    // Zig doesn't have non-blocking version of child.wait, so we use `BrokenPipe`
    // on writing to child's stdin to detect if a child is dead in a non-blocking
    // manner.
    _ = try std.posix.fcntl(
        process.stdin.?.handle,
        std.posix.F.SETFL,
        @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true })),
    );

    return .{
        .command = command,
        .process = process,
    };
}

fn upload_results(
    shell: *Shell,
    gpa: std.mem.Allocator,
    token: []const u8,
    seeds_new: []const SeedRecord,
    seeds_new_logs: []const ?[]const u8,
) !void {
    assert(seeds_new.len == seeds_new_logs.len);

    log.info("uploading {} seeds", .{seeds_new.len});

    _ = try shell.cwd.deleteTree("./devhubdb");
    try shell.exec(
        \\git clone --single-branch --depth 1
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

        try shell.exec("git fetch origin main", .{});
        try shell.exec("git reset --hard origin/main", .{});

        const max_size = 1 * MiB;
        const data = try shell.cwd.readFileAlloc(
            arena.allocator(),
            "./fuzzing/data.json",
            max_size,
        );

        const seeds_old = try SeedRecord.from_json(arena.allocator(), data);
        const seeds_merged = try SeedRecord.merge(arena.allocator(), .{}, seeds_old, seeds_new);
        const seeds_json = try SeedRecord.to_json(arena.allocator(), seeds_merged);

        var seeds_merged_logs = std.StringHashMap(void).init(arena.allocator());
        for (seeds_merged) |*seed| {
            if (seed.log) |path| try seeds_merged_logs.putNoClobber(path, {});
        }

        try shell.cwd.makePath("./fuzzing/logs/");
        // Create new log files.
        for (seeds_new, seeds_new_logs) |*seed, seed_log_data| {
            assert((seed.log == null) == (seed_log_data == null));
            if (seed.log) |log_path| {
                if (seeds_merged_logs.contains(log_path)) {
                    try shell.cwd.writeFile(.{ .sub_path = log_path, .data = seed_log_data.? });
                    try shell.exec("git add {path}", .{ .path = log_path });
                }
            }
        }

        // Delete any now-orphaned log files.
        for (try shell.find(.{
            .where = &.{"./fuzzing/logs/"},
            .extension = ".vopr",
        })) |log_path| {
            if (!seeds_merged_logs.contains(log_path)) {
                try shell.exec("git rm {path}", .{ .path = log_path });
            }
        }

        try shell.cwd.writeFile(.{ .sub_path = "./fuzzing/data.json", .data = seeds_json });
        try shell.exec("git add ./fuzzing/data.json", .{});
        try shell.git_env_setup(.{ .use_hostname = true });
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
    // Branch is a GitHub URL. It affects the UI, where the seeds are grouped by the branch.
    branch: []const u8,
    // Arbitrary string output for debugging, e.g., process exit status.
    debug: ?[]const u8 = null,
    // Optional path to log file, relative to the devhubdb root.
    log: ?[]const u8 = null,

    const Template = struct {
        branch: Branch,
        branch_url: []const u8,
        commit_timestamp: u64,
        commit_sha: [40]u8,
        fuzzer: Fuzzer,

        const Branch = union(enum) {
            main,
            release,
            pull: u32,

            const main_url = "https://github.com/tigerbeetle/tigerbeetle";
            const release_url = "https://github.com/tigerbeetle/tigerbeetle/tree/release";
            const pull_url_prefix = "https://github.com/tigerbeetle/tigerbeetle/pull/";

            pub fn parse(string: []const u8) !@This() {
                if (std.mem.eql(u8, string, main_url)) return .main;
                if (std.mem.eql(u8, string, release_url)) return .release;
                assert(std.mem.startsWith(u8, string, pull_url_prefix));
                const pull_number_string = string[pull_url_prefix.len..];
                const pull_number = std.fmt.parseInt(u32, pull_number_string, 10) catch unreachable;
                return .{ .pull = pull_number };
            }
        };
    };

    fn is_release(record: SeedRecord) bool {
        return std.mem.eql(u8, record.branch, Template.Branch.release_url);
    }

    fn order(a: SeedRecord, b: SeedRecord) std.math.Order {
        return order_by_field(b.commit_timestamp, a.commit_timestamp) orelse // NB: reverse order.
            order_by_field(a.commit_sha, b.commit_sha) orelse
            order_by_field(a.fuzzer, b.fuzzer) orelse
            order_by_field(a.ok, b.ok) orelse
            order_by_field(b.count, a.count) orelse // NB: reverse order.
            order_by_seed_duration(a, b) orelse
            order_by_seed_timestamp_start(a, b) orelse
            order_by_field(a.seed_timestamp_end, b.seed_timestamp_end) orelse
            order_by_field(a.seed, b.seed) orelse
            .eq;
    }

    fn order_by_field(a: anytype, b: @TypeOf(a)) ?std.math.Order {
        const full_order = switch (@TypeOf(a)) {
            []const u8 => std.mem.order(u8, a, b),
            [40]u8 => std.mem.order(u8, &a, &b),
            bool => std.math.order(@intFromBool(a), @intFromBool(b)),
            Fuzzer => std.math.order(@intFromEnum(a), @intFromEnum(b)),
            else => std.math.order(a, b),
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

    fn order_by_seed_duration(a: SeedRecord, b: SeedRecord) ?std.math.Order {
        assert(a.ok == b.ok);
        if (a.ok) {
            // Passing seeds: prefer long durations -- near-timeouts might be interesting, and it
            // gives us a p100 for the fuzzer's runtime.
            return order_by_field(b.seed_duration(), a.seed_duration());
        } else {
            // Failing seeds: prefer short duration, as coarse seed minimization.
            return order_by_field(a.seed_duration(), b.seed_duration());
        }
    }

    // Normally, records are sorted by commit timestamp, such that inactive or merged pull requests
    // sink down naturally. However, we want to "pin" the latest release commit even if it is old,
    // so we rig comparison function here.
    fn less_than(release_latest: u64, a: SeedRecord, b: SeedRecord) bool {
        maybe(release_latest == 0);
        const a_latest_release = a.commit_timestamp == release_latest and a.is_release();
        const b_latest_release = b.commit_timestamp == release_latest and b.is_release();
        if (a_latest_release and !b_latest_release) return true;
        if (!a_latest_release and b_latest_release) return false;
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
            .emit_null_optional_fields = false, // Omit `"debug: ""`.
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

        var release_latest: u64 = 0;
        for (current_and_new) |record| {
            if (record.is_release() and record.commit_timestamp > release_latest) {
                release_latest = record.commit_timestamp;
            }
        }
        std.mem.sort(SeedRecord, current_and_new, release_latest, SeedRecord.less_than);

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

            if (record.ok) {
                assert(record.log == null);
                // Merge counts with the first ok record for this fuzzer/commit, to make it easy for
                // the front-end to show the total count by displaying just the first record.
                if (result.getLastOrNull()) |record_previous| {
                    if (record_previous.ok and
                        std.mem.eql(u8, record_previous.fuzzer, record.fuzzer) and
                        std.meta.eql(record_previous.commit_sha, record.commit_sha))
                    {
                        result.items[result.items.len - 1].count += record.count;
                        continue;
                    }
                }
            }

            if (seed_count < options.seed_count_max) {
                try result.append(record);
                seed_count += 1;
            }
        }

        return result.items;
    }
};

fn create_log_path(arena: std.mem.Allocator) ![]const u8 {
    const name = std.crypto.random.int(u128);
    return std.fmt.allocPrint(arena, "./fuzzing/logs/{x:0>32}.vopr", .{name});
}

// TODO(Zig) This should probably be redone once zig's new reader/writer api's are available.
const LogTail = struct {
    ring: stdx.RingBufferType(u8, .slice),

    pub fn init(gpa: std.mem.Allocator, suffix_size_max: u32) !LogTail {
        assert(suffix_size_max > 0);

        const ring = try stdx.RingBufferType(u8, .slice).init(gpa, suffix_size_max);
        errdefer ring.deinit(gpa);

        return .{ .ring = ring };
    }

    pub fn deinit(log_tail: *LogTail, gpa: std.mem.Allocator) void {
        log_tail.ring.deinit(gpa);
        log_tail.* = undefined;
    }

    pub fn clear(log_tail: *LogTail) void {
        log_tail.ring.clear();
    }

    pub fn append(log_tail: *LogTail, bytes: []const u8) void {
        const capacity = log_tail.ring.buffer.len;
        const available = log_tail.ring.spare_capacity();
        const bytes_suffix = if (bytes.len < capacity) bytes else bytes[bytes.len - capacity ..];
        assert(bytes_suffix.len <= capacity);

        log_tail.ring.advance_head_many(bytes_suffix.len -| available);
        log_tail.ring.push_slice(bytes_suffix) catch unreachable;
    }

    fn suffix(log_tail: *const LogTail) [2][]const u8 {
        if (log_tail.ring.index + log_tail.ring.count < log_tail.ring.buffer.len) {
            return .{
                log_tail.ring.buffer[log_tail.ring.index..][0..log_tail.ring.count],
                "",
            };
        } else {
            return .{
                log_tail.ring.buffer[log_tail.ring.index..],
                log_tail.ring.buffer[0..log_tail.ring.index],
            };
        }
    }

    pub fn size(log_tail: *const LogTail) u32 {
        const parts = log_tail.suffix();
        return @intCast(parts[0].len + parts[1].len);
    }

    pub fn write_to(log_tail: *const LogTail, target: []u8) void {
        assert(target.len == log_tail.size());
        const parts = log_tail.suffix();
        stdx.copy_disjoint(.inexact, u8, target, parts[0]);
        stdx.copy_disjoint(.inexact, u8, target[parts[0].len..], parts[1]);
    }
};

test "cfo: LogTail" {
    var prng = stdx.PRNG.from_seed_testing();

    const log_tail_size = 8;
    var log_tail = try LogTail.init(std.testing.allocator, log_tail_size);
    defer log_tail.deinit(std.testing.allocator);

    var log_tail_model = std.ArrayList(u8).init(std.testing.allocator);
    defer log_tail_model.deinit();

    var buffer = try std.testing.allocator.alloc(u8, log_tail_size);
    defer std.testing.allocator.free(buffer);

    for (0..1024) |_| {
        const append_size = prng.int_inclusive(u32, log_tail_size);
        const append = buffer[0..append_size];
        prng.fill(append);

        try log_tail_model.appendSlice(append);
        log_tail.append(append);

        const suffix = buffer[0..log_tail.size()];
        assert(suffix.len <= log_tail_size);
        log_tail.write_to(suffix);

        try std.testing.expectEqualSlices(
            u8,
            suffix,
            log_tail_model.items[log_tail_model.items.len -| log_tail_size..],
        );
    }
}

const Snap = stdx.Snap;
const snap = Snap.snap_fn("src");

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
            const merged = try SeedRecord.merge(arena.allocator(), options, current, new);
            const got = try SeedRecord.to_json(arena.allocator(), merged);
            try want.diff(got);
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
            \\    "count": 6,
            \\    "seed_timestamp_start": 1,
            \\    "seed_timestamp_end": 1,
            \\    "seed": 3,
            \\    "command": "fuzz ewah",
            \\    "branch": "main"
            \\  }
            \\]
        ),
    );
}
