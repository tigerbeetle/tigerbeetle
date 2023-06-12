const std = @import("std");
const assert = std.debug.assert;
const allocator = std.testing.allocator;

const log = std.log.scoped(.lsm_update_table_microbenchmark);
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const lsm = @import("tree.zig");

const Key = u64;
const Value = struct {
    key: Key,
    tombstone: bool,
};

const growth_factor = constants.lsm_growth_factor;
const manifest_node_size = constants.lsm_manifest_node_size;

const table_count_max = lsm.table_count_max_for_tree(growth_factor, constants.lsm_levels);
const Environment = @import("manifest_level_fuzz.zig").EnvironmentType(table_count_max, manifest_node_size);

const Levels = enum(u8) {
    a = 5,
    b,
};

pub fn main() !void {
    const levels_fields = @typeInfo(Levels).Enum.fields;
    const args = try fuzz.parse_fuzz_args(allocator);
    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();
    var envs: [levels_fields.len]Environment = undefined;

    var index: u32 = 0;
    inline for (levels_fields) |field| {
        const level = @enumToInt(@field(Levels, field.name));
        const table_count = lsm.table_count_max_for_level(growth_factor, level);
        envs[index] = try Environment.init(random);
        log.debug("Inserting {} tables into level {}", .{ table_count, level });
        try envs[index].insert_tables(table_count);
        index += 1;
    }
    const level_a: *const Environment.ManifestLevel = &envs[0].level;
    const level_b: *const Environment.ManifestLevel = &envs[1].level;
    const optimal_table = level_a.compaction_table(level_b);
    log.debug("Optimal table: {}", .{optimal_table});
    for (envs) |*env| {
        try env.deinit();
    }
}
