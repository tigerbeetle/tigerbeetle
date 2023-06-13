const std = @import("std");
const assert = std.debug.assert;
const allocator = std.testing.allocator;

const log = std.log.scoped(.lsm_update_table_microbenchmark);
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const lsm = @import("tree.zig");
const math = std.math;

const Key = u64;
const Value = struct {
    key: Key,
    tombstone: bool,
};

const growth_factor = constants.lsm_growth_factor;
const manifest_node_size = constants.lsm_manifest_node_size;
const lsm_levels = constants.lsm_levels;

const table_count_max = lsm.table_count_max_for_tree(growth_factor, constants.lsm_levels);
const Environment = @import("manifest_level_fuzz.zig").EnvironmentType(table_count_max, manifest_node_size);
const TableInfo = Environment.TableInfo;
const TableInfoCopies = std.BoundedArray(TableInfo, growth_factor);
const TableInfoReferences = std.BoundedArray(*TableInfo, growth_factor);
const KeyRange = Environment.ManifestLevel.KeyRange;

fn level_data_iterator_microbenchmark(env: *Environment, table_references: TableInfoReferences) void {
    const num_tables = table_references.len;
    const manifest_level: *Environment.ManifestLevel = &env.level;
    const level_data_iterator_legacy_start = std.time.nanoTimestamp();
    var key_min = table_references.constSlice()[0].key_min;
    const key_max = table_references.constSlice()[num_tables - 1].key_max;
    var next_table_info = manifest_level.iterator(
        .visible,
        &[_]u64{lsm.snapshot_latest},
        .ascending,
        KeyRange{ .key_min = key_min, .key_max = key_max },
    ).next();

    while (next_table_info) |table_info| {
        key_min = table_info.key_max;
        next_table_info = manifest_level.iterator(
            .visible,
            &[_]u64{lsm.snapshot_latest},
            .ascending,
            KeyRange{ .key_min = key_min, .key_max = key_max },
        ).next();
    }
    const level_data_iterator_legacy_duration = std.time.nanoTimestamp() - level_data_iterator_legacy_start;
    log.debug("Time taken to iterate over {} intersecting tables using legacy LevelDataIterator: {}ns", .{ num_tables, level_data_iterator_legacy_duration });

    const level_data_iterator_start = std.time.nanoTimestamp();
    for (table_references.constSlice()) |_| {}
    const level_data_iterator_duration = std.time.nanoTimestamp() - level_data_iterator_start;
    log.debug("Time taken to iterate over {} intersecting tables using new LevelDataIterator: {}ns", .{ num_tables, level_data_iterator_duration });
}

fn update_table_microbenchmark(env: *Environment, table_copies: TableInfoCopies, table_references: TableInfoReferences) void {
    const manifest_level: *Environment.ManifestLevel = &env.level;
    var update_by_copy_start = std.time.nanoTimestamp();
    for (table_copies.constSlice()) |*table| {
        manifest_level.set_snapshot_max(table.snapshot_min + 1, @intToPtr(*TableInfo, @ptrToInt(table)));
    }

    var update_by_copy_duration = std.time.nanoTimestamp() - update_by_copy_start;
    log.debug("Time taken to update {} intersecting tables by copy: {}ns", .{ table_copies.len, update_by_copy_duration });

    for (table_references.constSlice()) |table| {
        table.snapshot_max = math.maxInt(u64);
        manifest_level.table_count_visible += 1;
    }

    var update_by_ref_start = std.time.nanoTimestamp();
    for (table_references.constSlice()) |table| {
        manifest_level.set_snapshot_max_stable_ptr(table.snapshot_min + 1, table);
    }
    var update_by_ref_duration = std.time.nanoTimestamp() - update_by_ref_start;
    log.debug("Time taken to update {} intersecting tables by reference: {}ns", .{ table_copies.len, update_by_ref_duration });
}

pub fn main() !void {
    const args = try fuzz.parse_fuzz_args(allocator);
    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();
    var fuzz_op_count = std.math.min(
        args.events_max orelse @as(usize, 2e5),
        fuzz.random_int_exponential(random, usize, 1e5),
    );
    while (fuzz_op_count > 0) : (fuzz_op_count -= 1) {
        var env = try Environment.init(random);
        const level = env.random.uintAtMostBiased(u32, lsm_levels - 1);
        const table_count = lsm.table_count_max_for_level(growth_factor, level);

        log.debug("Inserting {} tables into level {}", .{ table_count, level });
        try env.insert_tables(table_count);
        const manifest_level: *Environment.ManifestLevel = &env.level;
        var num_intersecting_tables = env.random.uintAtMostBiased(u32, growth_factor);
        var table_copies: TableInfoCopies = .{ .buffer = undefined };
        var table_references: TableInfoReferences = .{ .buffer = undefined };

        log.debug("# of intersecting tables: {}", .{num_intersecting_tables});

        if (num_intersecting_tables > 0) {
            const start_table_index = env.random.uintAtMostBiased(u32, manifest_level.table_count_visible - 1);
            var it = manifest_level.tables.iterator_from_index(start_table_index, .ascending);
            while (it.next()) |table| {
                table_copies.appendAssumeCapacity(table.*);
                table_references.appendAssumeCapacity(@intToPtr(*TableInfo, @ptrToInt(table)));
                num_intersecting_tables -= 1;
                if (num_intersecting_tables == 0) {
                    break;
                }
            }
            update_table_microbenchmark(&env, table_copies, table_references);
            level_data_iterator_microbenchmark(&env, table_references);
        }

        try env.deinit();
    }
}
