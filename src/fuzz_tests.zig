const std = @import("std");
const assert = std.debug.assert;

const flags = @import("./flags.zig");
const constants = @import("./constants.zig");
const fuzz = @import("./testing/fuzz.zig");

const log = std.log.scoped(.fuzz);

// NB: this changes values in `constants.zig`!
pub const tigerbeetle_config = @import("config.zig").configs.test_min;
comptime {
    assert(constants.storage_size_limit_max == tigerbeetle_config.process.storage_size_limit_max);
}

const MiB = 1024 * 1024;

pub const std_options: std.Options = .{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .storage, .level = .err },
        .{ .scope = .superblock_quorums, .level = .err },
        .{ .scope = .state_machine, .level = .err },
    },
};

const Fuzzers = .{
    .ewah = @import("./ewah_fuzz.zig"),
    .lsm_scan = @import("./lsm/scan_fuzz.zig"),
    .lsm_cache_map = @import("./lsm/cache_map_fuzz.zig"),
    .lsm_forest = @import("./lsm/forest_fuzz.zig"),
    .lsm_manifest_log = @import("./lsm/manifest_log_fuzz.zig"),
    .lsm_manifest_level = @import("./lsm/manifest_level_fuzz.zig"),
    .lsm_segmented_array = @import("./lsm/segmented_array_fuzz.zig"),
    .lsm_tree = @import("./lsm/tree_fuzz.zig"),
    .storage = @import("./storage_fuzz.zig"),
    .vsr_free_set = @import("./vsr/free_set_fuzz.zig"),
    .vsr_journal_format = @import("./vsr/journal_format_fuzz.zig"),
    .vsr_superblock = @import("./vsr/superblock_fuzz.zig"),
    .vsr_superblock_quorums = @import("./vsr/superblock_quorums_fuzz.zig"),
    .vsr_multi_batch = @import("./vsr/multi_batch_fuzz.zig"),
    .signal = @import("./clients/c/tb_client/signal_fuzz.zig"),
    .state_machine = @import("./state_machine_fuzz.zig"),
    // A fuzzer that intentionally fails, to test fuzzing infrastructure itself
    .canary = {},
    // Quickly run all fuzzers as a smoke test
    .smoke = {},
};

const FuzzersEnum = std.meta.FieldEnum(@TypeOf(Fuzzers));

const CLIArgs = struct {
    events_max: ?usize = null,
    positional: struct {
        fuzzer: FuzzersEnum,
        seed: ?u64 = null,
    },
};

pub fn main() !void {
    fuzz.limit_ram();
    probe_stack(3 * MiB);

    var gpa_allocator: std.heap.GeneralPurposeAllocator(.{}) = .{};
    const gpa = gpa_allocator.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli_args = flags.parse(&args, CLIArgs);

    switch (cli_args.positional.fuzzer) {
        .smoke => {
            assert(cli_args.positional.seed == null);
            assert(cli_args.events_max == null);
            try main_smoke(gpa);
        },
        else => try main_single(gpa, cli_args),
    }
}

// See https://ziggit.dev/t/stack-probe-puzzle/10291 for the full story of why this is here.
//
// TL;DR: on CFO, we observed forest_fuzz segfaulting in the stack probe, despite using only
// half a MiB of stack, because, for some reason, a heap allocation was mmapped into the stack
// region. The root cause of this is unclear. It looks like `forest_fuzz` does a lot of mmap/munmap
// when simulating crash/restart, and that somehow confuses the kernel into picking a wrong address
// for mmap. As a work-around, we eagerly probe the stack at the start.
//
// If you are from the future, consider removing this and seeing if the issue reproduces. Chances
// are a kernel upgrade can fix it? The CFO kernel at the time of writing is 6.1.0-25-amd64.
noinline fn probe_stack(comptime size: usize) void {
    var big: [size]u8 = undefined;
    touch(&big);
    assert(big[0] == 92 and big[size - 1] == 92);
}

noinline fn touch(slice: []u8) void {
    slice[0] = 92;
    slice[slice.len - 1] = 92;
}

fn main_smoke(gpa: std.mem.Allocator) !void {
    var timer_all = try std.time.Timer.start();
    inline for (comptime std.enums.values(FuzzersEnum)) |fuzzer| {
        const events_max = switch (fuzzer) {
            .smoke => continue,
            .canary => continue,

            .lsm_cache_map => 20_000,
            .lsm_forest => 10_000,
            .lsm_manifest_log => 2_000,
            .lsm_scan => 100,
            .lsm_tree => 400,
            .state_machine => 10_000,
            .storage => 1_000,
            .vsr_free_set => 10_000,
            .vsr_multi_batch => 128,
            .vsr_superblock => 3,

            inline .ewah,
            .lsm_segmented_array,
            .lsm_manifest_level,
            .vsr_journal_format,
            .vsr_superblock_quorums,
            .signal,
            => null,
        };

        var timer_single = try std.time.Timer.start();
        try @field(Fuzzers, @tagName(fuzzer)).main(gpa, .{
            .seed = 123,
            .events_max = events_max,
        });
        const fuzz_duration = timer_single.lap();
        if (fuzz_duration > 10 * std.time.ns_per_s) {
            log.err("fuzzer too slow for the smoke mode: " ++ @tagName(fuzzer) ++ " {}", .{
                std.fmt.fmtDuration(fuzz_duration),
            });
        }
    }

    log.info("done in {}", .{std.fmt.fmtDuration(timer_all.lap())});
}

fn main_single(gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    assert(cli_args.positional.fuzzer != .smoke);

    const seed = cli_args.positional.seed orelse std.crypto.random.int(u64);
    log.info("Fuzz seed = {}", .{seed});

    var timer = try std.time.Timer.start();
    switch (cli_args.positional.fuzzer) {
        .smoke => unreachable,
        .canary => {
            if (seed % 100 == 0) {
                std.process.exit(1);
            }
        },
        inline else => |fuzzer| try @field(Fuzzers, @tagName(fuzzer)).main(gpa, .{
            .seed = seed,
            .events_max = cli_args.events_max,
        }),
    }
    log.info("done in {}", .{std.fmt.fmtDuration(timer.lap())});
}
