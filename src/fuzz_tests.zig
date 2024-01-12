const std = @import("std");
const assert = std.debug.assert;

const flags = @import("./flags.zig");
const constants = @import("./constants.zig");
const fuzz = @import("./testing/fuzz.zig");
const fatal = flags.fatal;

const log = std.log.scoped(.fuzz);

// NB: this changes values in `constants.zig`!
pub const tigerbeetle_config = @import("config.zig").configs.fuzz_min;
comptime {
    assert(constants.storage_size_limit_max == tigerbeetle_config.process.storage_size_limit_max);
}

pub const std_options = struct {
    pub const log_level: std.log.Level = .info;
    pub const log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .superblock_quorums, .level = .err },
    };
};

const Fuzzers = .{
    .ewah = @import("./ewah_fuzz.zig"),
    .lsm_cache_map = @import("./lsm/cache_map_fuzz.zig"),
    .lsm_forest = @import("./lsm/forest_fuzz.zig"),
    .lsm_manifest_log = @import("./lsm/manifest_log_fuzz.zig"),
    // TODO: This one currently doesn't compile.
    .lsm_manifest_level = @import("./lsm/manifest_level_fuzz.zig"),
    .lsm_segmented_array = @import("./lsm/segmented_array_fuzz.zig"),
    .lsm_tree = @import("./lsm/tree_fuzz.zig"),
    .vsr_free_set = @import("./vsr/free_set_fuzz.zig"),
    .vsr_journal_format = @import("./vsr/journal_format_fuzz.zig"),
    .vsr_superblock = @import("./vsr/superblock_fuzz.zig"),
    .vsr_superblock_quorums = @import("./vsr/superblock_quorums_fuzz.zig"),
    // Quickly run all fuzzers as a smoke test
    .smoke = {},
};

const FuzzersEnum = std.meta.FieldEnum(@TypeOf(Fuzzers));

const CliArgs = struct {
    seed: ?u64 = null,
    events_max: ?usize = null,
    positional: struct {
        fuzzer: FuzzersEnum,
    },
};

pub fn main() !void {
    var args = try std.process.argsWithAllocator(fuzz.allocator);
    defer args.deinit();

    assert(args.skip()); // Discard executable name.
    const cli_args = flags.parse(&args, CliArgs);

    switch (cli_args.positional.fuzzer) {
        .smoke => {
            assert(cli_args.seed == null);
            assert(cli_args.events_max == null);
            try main_smoke();
        },
        else => try main_single(cli_args),
    }
}

fn main_smoke() !void {
    var timer_all = try std.time.Timer.start();
    inline for (comptime std.enums.values(FuzzersEnum)) |fuzzer| {
        const events_max = switch (fuzzer) {
            .smoke => continue,

            .lsm_cache_map => 20_000,
            .lsm_forest => 10_000,
            .lsm_manifest_log => 2_000,
            .lsm_tree => 400,
            .vsr_free_set => 10_000,
            .vsr_superblock => 3,

            inline .ewah,
            .lsm_segmented_array,
            .lsm_manifest_level,
            .vsr_journal_format,
            .vsr_superblock_quorums,
            => null,
        };

        var timer_single = try std.time.Timer.start();
        try @field(Fuzzers, @tagName(fuzzer)).main(.{ .seed = 123, .events_max = events_max });
        const fuzz_duration = timer_single.lap();
        if (fuzz_duration > 60 * std.time.ns_per_s) {
            log.err("fuzzer too slow for the smoke mode: " ++ @tagName(fuzzer) ++ " {}", .{
                std.fmt.fmtDuration(fuzz_duration),
            });
        }
    }

    log.info("done in {}", .{std.fmt.fmtDuration(timer_all.lap())});
}

fn main_single(cli_args: CliArgs) !void {
    assert(cli_args.positional.fuzzer != .smoke);

    const seed: usize = cli_args.seed orelse seed: {
        // If no seed was given, use a random seed instead.
        var seed_random: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed_random));
        break :seed seed_random;
    };
    log.info("Fuzz seed = {}", .{seed});

    var timer = try std.time.Timer.start();
    switch (cli_args.positional.fuzzer) {
        .smoke => unreachable,
        inline else => |fuzzer| try @field(Fuzzers, @tagName(fuzzer)).main(
            .{ .seed = seed, .events_max = cli_args.events_max },
        ),
    }
    log.info("done in {}", .{std.fmt.fmtDuration(timer.lap())});
}
