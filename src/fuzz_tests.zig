const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const constants = @import("./constants.zig");
const fuzz = @import("./testing/fuzz.zig");

const log = std.log.scoped(.fuzz);

// This enables `constants.verify` for this file.
pub const vsr_options = .{
    .config_verify = true,
    .git_commit = @import("vsr_options").git_commit,
    .release = @import("vsr_options").release,
    .release_client_min = @import("vsr_options").release_client_min,
    .config_aof_recovery = @import("vsr_options").config_aof_recovery,
};

// NB: this changes values in `constants.zig`!
pub const tigerbeetle_config = @import("config.zig").configs.test_min;
comptime {
    assert(constants.storage_size_limit_max == tigerbeetle_config.process.storage_size_limit_max);
}

pub const std_options: std.Options = .{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .message_bus, .level = .err },
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
    .message_bus = @import("./message_bus_fuzz.zig"),
    .storage = @import("./storage_fuzz.zig"),
    .vsr_free_set = @import("./vsr/free_set_fuzz.zig"),
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

    @"--": void,
    fuzzer: FuzzersEnum,
    seed: ?u64 = null,
};

pub fn main() !void {
    comptime assert(constants.verify);

    fuzz.limit_ram();

    var gpa_allocator: std.heap.GeneralPurposeAllocator(.{}) = .{};
    // Disable "hint" argument for mmap call, which was observed to cause stack overflow.
    // See https://ziggit.dev/t/stack-probe-puzzle/10291/3 for the full story.
    gpa_allocator.backing_allocator = .{
        .ptr = std.heap.page_allocator.ptr,
        .vtable = &comptime .{
            .alloc = struct {
                fn alloc(
                    ctx: *anyopaque,
                    len: usize,
                    ptr_align: std.mem.Alignment,
                    ret_addr: usize,
                ) ?[*]u8 {
                    @atomicStore(
                        @TypeOf(std.heap.next_mmap_addr_hint),
                        &std.heap.next_mmap_addr_hint,
                        null,
                        .monotonic,
                    );
                    return std.heap.page_allocator.vtable.alloc(ctx, len, ptr_align, ret_addr);
                }
            }.alloc,
            .remap = std.heap.page_allocator.vtable.remap,
            .resize = std.heap.page_allocator.vtable.resize,
            .free = std.heap.page_allocator.vtable.free,
        },
    };
    const gpa = gpa_allocator.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli_args = stdx.flags(&args, CLIArgs);

    switch (cli_args.fuzzer) {
        .smoke => {
            assert(cli_args.seed == null);
            assert(cli_args.events_max == null);
            try main_smoke(gpa);
        },
        else => try main_single(gpa, cli_args),
    }
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
            .message_bus => 10,
            .storage => 1_000,
            .vsr_free_set => 10_000,
            .vsr_multi_batch => 128,
            .vsr_superblock => 3,

            inline .ewah,
            .lsm_segmented_array,
            .lsm_manifest_level,
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
    assert(cli_args.fuzzer != .smoke);

    const seed = cli_args.seed orelse std.crypto.random.int(u64);
    log.info("Fuzz seed = {}", .{seed});

    var timer = try std.time.Timer.start();
    switch (cli_args.fuzzer) {
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
