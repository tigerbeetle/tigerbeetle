const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const allocator = fuzz.allocator;

const log = std.log.scoped(.lsm_forest_fuzz);
const lsm = @import("tree.zig");

const Storage = @import("../testing/storage.zig").Storage;
const GridType = @import("../vsr/grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const ForestType = @import("forest.zig").ForestType;
const ScanLookupType = @import("scan_lookup.zig").ScanLookupType;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;
const Direction = @import("../direction.zig").Direction;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const batch_max: u32 = @divFloor(
    constants.message_body_size_max,
    @sizeOf(Thing),
);

/// The testing object.
const Thing = extern struct {
    id: u128,
    // All indexes must be `u64` to avoid conflicting matching values:
    index_1: u64,
    index_2: u64,
    index_3: u64,
    index_4: u64,
    index_5: u64,
    index_6: u64,
    index_7: u64,
    index_8: u64,
    index_9: u64,
    index_10: u64,
    index_11: u64,
    index_12: u64,
    reserved: u64,
    timestamp: u64,

    comptime {
        assert(stdx.no_padding(Thing));
        assert(@sizeOf(Thing) == 128);
        assert(@alignOf(Thing) == 16);
    }

    fn zeroed() Thing {
        return .{
            .id = 0,
            .index_1 = 0,
            .index_2 = 0,
            .index_3 = 0,
            .index_4 = 0,
            .index_5 = 0,
            .index_6 = 0,
            .index_7 = 0,
            .index_8 = 0,
            .index_9 = 0,
            .index_10 = 0,
            .index_11 = 0,
            .index_12 = 0,
            .reserved = 0,
            .timestamp = 0,
        };
    }

    fn get_index(self: *const Thing, index: Index) u64 {
        switch (index) {
            inline else => |comptime_index| {
                return @field(self, @tagName(comptime_index));
            },
        }
    }

    fn set_index(self: *Thing, index: Index, value: u64) void {
        switch (index) {
            inline else => |comptime_index| {
                @field(self, @tagName(comptime_index)) = value;
            },
        }
    }

    fn merge(self: *Thing, other: Thing) void {
        stdx.maybe(stdx.zeroed(std.mem.asBytes(self)));
        assert(!stdx.zeroed(std.mem.asBytes(&other)));
        defer assert(!stdx.zeroed(std.mem.asBytes(self)));

        for (std.enums.values(Index)) |index| {
            const value = other.get_index(index);
            if (value != 0) {
                assert(self.get_index(index) == 0);
                self.set_index(index, value);
            }
        }
    }

    fn randomize(self: Thing, random: std.rand.Random, id: u128, timestamp: u64) Thing {
        assert(self.id == 0);
        assert(self.timestamp == 0);
        assert(id != 0);
        assert(timestamp != 0);

        var other: Thing = self;
        other.id = id;
        other.timestamp = timestamp;
        for (std.enums.values(Index)) |index| {
            const value = other.get_index(index);
            if (value == 0) {
                // Fill the zeroed fields with random values out of the matching prefix.
                other.set_index(
                    index,
                    prefix_combine(std.math.maxInt(u32), random.int(u32)),
                );
            }
        }

        return other;
    }
};

const ThingsGroove = GrooveType(
    Storage,
    Thing,
    .{
        .ids = .{
            .id = 1,
            .index_1 = 2,
            .index_2 = 3,
            .index_3 = 4,
            .index_4 = 5,
            .index_5 = 6,
            .index_6 = 7,
            .index_7 = 8,
            .index_8 = 9,
            .index_9 = 10,
            .index_10 = 11,
            .index_11 = 12,
            .index_12 = 13,
            .timestamp = 14,
        },
        .batch_value_count_max = .{
            .id = batch_max,
            .index_1 = batch_max,
            .index_2 = batch_max,
            .index_3 = batch_max,
            .index_4 = batch_max,
            .index_5 = batch_max,
            .index_6 = batch_max,
            .index_7 = batch_max,
            .index_8 = batch_max,
            .index_9 = batch_max,
            .index_10 = batch_max,
            .index_11 = batch_max,
            .index_12 = batch_max,
            .timestamp = batch_max,
        },
        .ignored = &[_][]const u8{"reserved"},
        .derived = .{},
    },
);

const Forest = ForestType(Storage, .{
    .things = ThingsGroove,
});

const Index = std.meta.FieldEnum(ThingsGroove.IndexTrees);

const ScanLookup = ScanLookupType(
    ThingsGroove,
    ThingsGroove.ScanBuilder.Scan,
    Storage,
);

const QueryCondition = union(enum) {
    field_condition: FieldCondition,
    parenthesis_condition: ParenthesisCondition,
};

const QueryOperator = enum {
    union_set,
    intersection_set,

    fn flip(self: QueryOperator) QueryOperator {
        return switch (self) {
            .union_set => .intersection_set,
            .intersection_set => .union_set,
        };
    }
};

const FieldCondition = struct {
    index: Index,
    value: u64,
};

const ParenthesisCondition = struct {
    operator: QueryOperator,
    operands: []const QueryCondition,
};

const QuerySpec = struct {
    // All matching fields must start with this prefix, to avoid collision.
    prefix: u32,
    // The query condition.
    condition: QueryCondition,
    // Ascending or descending.
    reversed: bool,
    // Matching templates to populate the database.
    // Zeroed fields are not part of the condition and can be filled with random values.
    templates: []const Thing,
    // Number of expected results.
    expected_results: u32,
};

/// This fuzzer generates random arbitrary complex query conditions such as
/// `(a OR b) and (c OR d OR (e AND f AND g))`.
/// It also includes an array of at least one object that matches the condition.
/// Those objects are used as template to populate the database in such a way
/// that the results retrieved by the query can be asserted.
///
/// Some limitations in place:
///
/// - Limited up to the max number of scans defined at `constants.lsm_scans_max`.
///
/// - The next operator must be the opposite of the previous one,
///   avoiding unnecessary use of parenthesis, such as `(a AND b) AND c`.
///   This way, the query generated can be either `a AND b AND c` without
///   precedence or `(a AND b) OR c` flipping the operator.
///
/// - Cannot repeat fields, while `(a=1 OR a=2)` is valid, this limitation avoids
///   always false conditions such as `(a=1 AND a=2)`.
const QuerySpecFuzzer = struct {
    arena: std.mem.Allocator,
    random: std.rand.Random,
    prefix: u32,

    suffix_last: u32 = 0,
    indexes_used: std.EnumSet(Index) = std.EnumSet(Index).initEmpty(),

    /// Always generate more than one query spec, since multiple queries can
    /// test both the positive space (results must match the query) and the
    /// negative space (results from other queries must not match).
    fn generate_fuzz_query_specs(
        arena: std.mem.Allocator,
        random: std.rand.Random,
    ) ![]QuerySpec {
        const query_spec_count = random.intRangeAtMostBiased(usize, 2, 8);
        const query_specs = try arena.alloc(QuerySpec, query_spec_count);
        for (query_specs, 1..) |*query_spec, prefix| {
            var fuzzer = QuerySpecFuzzer{
                .arena = arena,
                .random = random,
                .prefix = @truncate(prefix),
            };
            query_spec.* = try fuzzer.generate_query_spec();
        }
        return query_specs;
    }

    fn generate_query_spec(
        self: *QuerySpecFuzzer,
    ) !QuerySpec {
        const condition = QueryCondition{
            .parenthesis_condition = try self.generate_parenthesys_condition(
                self.random.enumValue(QueryOperator),
                1 + self.random.intRangeLessThanBiased(u32, 0, constants.lsm_scans_max),
            ),
        };
        const templates = try self.get_templates(condition);

        return QuerySpec{
            .prefix = self.prefix,
            .condition = condition,
            .templates = templates,
            .reversed = self.random.boolean(),
            .expected_results = 0,
        };
    }

    fn generate_parenthesys_condition(
        self: *QuerySpecFuzzer,
        operator: QueryOperator,
        field_count: u32,
    ) !ParenthesisCondition {
        assert(field_count > 0);
        assert(field_count <= constants.lsm_scans_max);

        const QueryConditionTag = std.meta.Tag(QueryCondition);
        var operands = std.ArrayListUnmanaged(QueryCondition){};
        var fields_remain: u32 = field_count;
        while (fields_remain > 0) {
            const condition_tag = if (fields_remain > 1)
                self.random.enumValue(QueryConditionTag)
            else
                .field_condition;

            const condition = switch (condition_tag) {
                .field_condition => field_condition: {
                    fields_remain -= 1;
                    break :field_condition QueryCondition{
                        .field_condition = self.generate_field_condition(),
                    };
                },
                .parenthesis_condition => parenthesis_condition: {
                    const field_count_parenthesis = self.random.intRangeAtMostBiased(
                        u32,
                        // Parenthesis conditions must contain at least two fields:
                        2,
                        fields_remain,
                    );
                    fields_remain -= field_count_parenthesis;
                    break :parenthesis_condition QueryCondition{
                        .parenthesis_condition = try self.generate_parenthesys_condition(
                            operator.flip(),
                            field_count_parenthesis,
                        ),
                    };
                },
            };
            try operands.append(self.arena, condition);
        }

        return ParenthesisCondition{
            .operator = operator,
            .operands = try operands.toOwnedSlice(self.arena),
        };
    }

    fn generate_field_condition(
        self: *QuerySpecFuzzer,
    ) FieldCondition {
        self.suffix_last += 1;
        const value: u64 = prefix_combine(self.prefix, self.suffix_last);

        return FieldCondition{
            .index = self.index_random(),
            .value = value,
        };
    }

    fn index_random(self: *QuerySpecFuzzer) Index {
        const index_count = comptime std.enums.values(Index).len;
        comptime assert(index_count >= constants.lsm_scans_max);
        assert(self.indexes_used.count() < index_count);
        while (true) {
            const index = self.random.enumValue(Index);
            if (self.indexes_used.contains(index)) continue;

            self.indexes_used.insert(index);
            return index;
        }
    }

    /// Templates are objects that match the condition, such as:
    /// - The condition `a=1` (from now on represented only as `a` for brevity)
    ///   generates a template with the field `a` set to `1`, so when inserting
    ///   objects based on this template, they're expected to match.
    ///
    /// - The condition `(a OR b)` generates two templates, one with the
    ///   field `a` and another one with the field `b` set, so both of them
    ///   can satisfy the OR clause.
    ///
    /// - The condition `(a AND b)` generates only a single template that
    ///   matches the criteria with both fields `a` and `b` set to the
    ///   corresponding value.
    ///
    /// More complex queries like `(a OR b) AND (c OR d)` need to combine the
    /// templates generated by each individual condition in such a way any of
    /// them can satisfy the OR and AND clauses: {a,c},{a,d},{b,c}, and {b,d}.
    fn get_templates(
        self: *QuerySpecFuzzer,
        condition: QueryCondition,
    ) ![]const Thing {
        var templates: std.ArrayListUnmanaged(Thing) = .{};
        switch (condition) {
            .field_condition => |field_condition| {
                var template = Thing.zeroed();
                template.set_index(field_condition.index, field_condition.value);
                try templates.append(self.arena, template);
            },
            .parenthesis_condition => |parenthesis_condition| {
                const matrix_len = parenthesis_condition.operands.len;
                assert(matrix_len > 0);
                assert(matrix_len <= constants.lsm_scans_max);

                var matrix = [_][]const Thing{undefined} ** constants.lsm_scans_max;
                for (parenthesis_condition.operands, 0..) |operand, i| {
                    matrix[i] = try self.get_templates(operand);
                }

                switch (parenthesis_condition.operator) {
                    .union_set => {
                        for (matrix[0..matrix_len]) |operand_templates| {
                            try templates.appendSlice(self.arena, operand_templates);
                        }
                    },
                    .intersection_set => {
                        var matrix_indexes = [_]u32{0} ** constants.lsm_scans_max;
                        while (matrix_indexes[0] < matrix[0].len) {
                            var template = Thing.zeroed();
                            for (matrix[0..matrix_len], 0..) |operand_templates, i| {
                                template.merge(operand_templates[matrix_indexes[i]]);
                            }
                            try templates.append(self.arena, template);

                            for (0..parenthesis_condition.operands.len) |i| {
                                const reverse_index = parenthesis_condition.operands.len - 1 - i;
                                if (reverse_index == 0 or
                                    matrix_indexes[reverse_index] < matrix[reverse_index].len - 1)
                                {
                                    matrix_indexes[reverse_index] += 1;
                                } else {
                                    matrix_indexes[reverse_index] = 0;
                                    matrix_indexes[reverse_index - 1] += 1;
                                }
                            }
                        }
                    },
                }
            },
        }

        return try templates.toOwnedSlice(self.arena);
    }
};

const Environment = struct {
    const cluster = 0;
    const replica = 0;
    const replica_count = 1;
    const node_count = 1024;

    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
    const forest_options = Forest.GroovesOptions{
        .things = .{
            .prefetch_entries_for_read_max = batch_max,
            .prefetch_entries_for_update_max = batch_max,
            .cache_entries_max = cache_entries_max,
            .tree_options_object = .{ .batch_value_count_limit = batch_max },
            .tree_options_id = .{ .batch_value_count_limit = batch_max },
            .tree_options_index = .{
                .index_1 = .{ .batch_value_count_limit = batch_max },
                .index_2 = .{ .batch_value_count_limit = batch_max },
                .index_3 = .{ .batch_value_count_limit = batch_max },
                .index_4 = .{ .batch_value_count_limit = batch_max },
                .index_5 = .{ .batch_value_count_limit = batch_max },
                .index_6 = .{ .batch_value_count_limit = batch_max },
                .index_7 = .{ .batch_value_count_limit = batch_max },
                .index_8 = .{ .batch_value_count_limit = batch_max },
                .index_9 = .{ .batch_value_count_limit = batch_max },
                .index_10 = .{ .batch_value_count_limit = batch_max },
                .index_11 = .{ .batch_value_count_limit = batch_max },
                .index_12 = .{ .batch_value_count_limit = batch_max },
            },
        },
    };

    const State = enum {
        init,
        superblock_format,
        superblock_open,
        free_set_open,
        forest_init,
        forest_open,
        fuzzing,
        populating,
        scanning,
        forest_compact,
        grid_checkpoint,
        forest_checkpoint,
        superblock_checkpoint,
    };

    random: std.rand.Random,
    state: State,

    storage: *Storage,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context = undefined,
    grid: Grid,
    forest: Forest,
    ticks_remaining: usize,

    op: u64 = 0,
    checkpoint_op: ?u64 = null,
    object_count: u64,

    scan_lookup: ScanLookup = undefined,
    scan_lookup_buffer: []Thing,
    scan_lookup_result: ?[]const Thing = null,

    fn init(
        env: *Environment,
        storage: *Storage,
        random: std.rand.Random,
    ) !void {
        env.* = .{
            .storage = storage,
            .random = random,
            .state = .init,

            .superblock = try SuperBlock.init(allocator, .{
                .storage = env.storage,
                .storage_size_limit = constants.storage_size_limit_max,
            }),

            .grid = try Grid.init(allocator, .{
                .superblock = &env.superblock,
                .missing_blocks_max = 0,
                .missing_tables_max = 0,
            }),

            .scan_lookup_buffer = try allocator.alloc(Thing, batch_max),
            .forest = undefined,
            .checkpoint_op = null,
            .ticks_remaining = std.math.maxInt(usize),
            .object_count = 0,
        };
    }

    fn deinit(env: *Environment) void {
        env.superblock.deinit(allocator);
        env.grid.deinit(allocator);
        allocator.free(env.scan_lookup_buffer);
    }

    pub fn run(
        storage: *Storage,
        random: std.rand.Random,
        repeat: u32,
    ) !void {
        var env: Environment = undefined;
        try env.init(storage, random);
        defer env.deinit();

        env.change_state(.init, .superblock_format);
        env.superblock.format(superblock_format_callback, &env.superblock_context, .{
            .cluster = cluster,
            .release = vsr.Release.minimum,
            .replica = replica,
            .replica_count = replica_count,
        });
        try env.tick_until_state_change(.superblock_format, .superblock_open);

        try env.open();
        defer env.close();

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const query_specs = try QuerySpecFuzzer.generate_fuzz_query_specs(
            arena.allocator(),
            random,
        );

        try env.apply(query_specs, repeat);
    }

    fn apply(
        env: *Environment,
        query_specs: []QuerySpec,
        /// Repeating the same test multiple times is valuable since
        /// it scans the same values after compaction.
        repeat: u32,
    ) !void {
        assert(repeat > 0);
        assert(env.state == .fuzzing);

        for (0..repeat) |_| {
            // Inserting one batch for each query spec.
            for (query_specs) |*query_spec| {
                try env.populate_things(query_spec);
            }

            // Executing each query spec.
            for (query_specs) |*query_spec| {
                try env.query(query_spec);
            }
        }
    }

    fn populate_things(env: *Environment, query_spec: *QuerySpec) !void {
        assert(query_spec.templates.len > 0);

        for (0..batch_max) |_| {
            // Total number of objects inserted.
            env.object_count += 1;

            // Non-match inserted just for creating "noise".
            const noise_probability = 20;
            if (fuzz.chance(env.random, noise_probability)) {
                var dummy = Thing.zeroed();
                env.forest.grooves.things.insert(&dummy.randomize(
                    env.random,
                    env.random.int(u128),
                    env.object_count,
                ));

                continue;
            }

            const template = query_spec.templates[
                env.random.intRangeAtMostBiased(
                    usize,
                    0,
                    query_spec.templates.len - 1,
                )
            ];

            query_spec.expected_results += 1; // Expected objects that match the spec.
            env.forest.grooves.things.insert(&template.randomize(
                env.random,
                prefix_combine(
                    query_spec.prefix,
                    query_spec.expected_results,
                ),
                env.object_count,
            ));
        }

        try env.commit();
    }

    fn query(env: *Environment, query_spec: *QuerySpec) !void {
        assert(query_spec.expected_results > 0);

        const pages = stdx.div_ceil(query_spec.expected_results, batch_max);
        assert(pages > 0);

        var result_count: u32 = 0;
        var timestamp_last: u64 = if (query_spec.reversed)
            std.math.maxInt(u64)
        else
            0;

        for (0..pages) |page| {
            const results = try env.fetch(query_spec, timestamp_last);

            for (results) |result| {
                if (query_spec.reversed)
                    assert(timestamp_last > result.timestamp)
                else
                    assert(timestamp_last < result.timestamp);
                timestamp_last = result.timestamp;

                assert(prefix_validate(query_spec.prefix, result.id));
                result_count += 1;
            }

            if (query_spec.expected_results <= batch_max) {
                assert(results.len == query_spec.expected_results);
            }

            const remaining: u32 = query_spec.expected_results - result_count;
            if (remaining == 0) {
                assert(page == pages - 1);
                assert(results.len + (page * batch_max) == query_spec.expected_results);
            } else {
                assert(results.len == batch_max);
            }
        }

        assert(result_count == query_spec.expected_results);
    }

    fn fetch(
        env: *Environment,
        query_spec: *const QuerySpec,
        timestamp_last: u64,
    ) ![]const Thing {
        assert(env.forest.scan_buffer_pool.scan_buffer_used == 0);
        defer {
            assert(env.forest.scan_buffer_pool.scan_buffer_used > 0);

            env.forest.scan_buffer_pool.reset();
            env.forest.grooves.things.scan_builder.reset();
        }

        const scan = env.scan_from_condition(
            query_spec.condition,
            timestamp_last,
            query_spec.reversed,
        );
        env.scan_lookup = ScanLookup.init(
            &env.forest.grooves.things,
            scan,
        );

        assert(env.scan_lookup_result == null);
        defer env.scan_lookup_result = null;

        env.change_state(.fuzzing, .scanning);
        env.scan_lookup.read(env.scan_lookup_buffer, &scan_lookup_callback);
        try env.tick_until_state_change(.scanning, .fuzzing);

        return env.scan_lookup_result.?;
    }

    fn scan_from_condition(
        env: *Environment,
        condition: QueryCondition,
        timestamp_last: u64, // exclusive
        reversed: bool,
    ) *ThingsGroove.ScanBuilder.Scan {
        const scan_buffer_pool = &env.forest.scan_buffer_pool;
        const things_groove = &env.forest.grooves.things;
        const scan_builder: *ThingsGroove.ScanBuilder = &things_groove.scan_builder;

        switch (condition) {
            .field_condition => |field_condition| {
                const direction: Direction = if (reversed) .descending else .ascending;
                const timestamp_range = if (timestamp_last == 0)
                    TimestampRange.all()
                else if (reversed)
                    TimestampRange.lte(timestamp_last - 1)
                else
                    TimestampRange.gte(timestamp_last + 1);

                return switch (field_condition.index) {
                    inline else => |comptime_index| scan_builder.scan_prefix(
                        comptime_index,
                        scan_buffer_pool.acquire_assume_capacity(),
                        lsm.snapshot_latest,
                        field_condition.value,
                        timestamp_range,
                        direction,
                    ),
                };
            },
            .parenthesis_condition => |parenthesis_condition| {
                assert(parenthesis_condition.operands.len > 0);

                var scans: stdx.BoundedArray(
                    *ThingsGroove.ScanBuilder.Scan,
                    constants.lsm_scans_max,
                ) = .{};
                for (parenthesis_condition.operands) |operand| {
                    const scan = env.scan_from_condition(operand, timestamp_last, reversed);
                    scans.append_assume_capacity(scan);
                }
                assert(scans.count() == parenthesis_condition.operands.len);

                return if (scans.count() == 1)
                    scans.get(0)
                else switch (parenthesis_condition.operator) {
                    .union_set => scan_builder.merge_union(scans.const_slice()),
                    .intersection_set => scan_builder.merge_intersection(scans.const_slice()),
                };
            },
        }
    }

    fn change_state(env: *Environment, current_state: State, next_state: State) void {
        assert(env.state == current_state);
        env.state = next_state;
    }

    fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) !void {
        while (true) {
            if (env.state != current_state) break;

            if (env.ticks_remaining == 0) return error.OutOfTicks;
            env.ticks_remaining -= 1;
            env.storage.tick();
        }
        assert(env.state == next_state);
    }

    fn open(env: *Environment) !void {
        env.superblock.open(superblock_open_callback, &env.superblock_context);
        try env.tick_until_state_change(.superblock_open, .free_set_open);

        env.grid.open(grid_open_callback);
        try env.tick_until_state_change(.free_set_open, .forest_init);

        env.forest = try Forest.init(allocator, &env.grid, .{
            .compaction_block_count = Forest.Options.compaction_block_count_min,
            .node_count = node_count,
        }, forest_options);
        env.change_state(.forest_init, .forest_open);
        env.forest.open(forest_open_callback);

        try env.tick_until_state_change(.forest_open, .fuzzing);
    }

    fn close(env: *Environment) void {
        env.forest.deinit(allocator);
    }

    fn commit(env: *Environment) !void {
        env.op += 1;

        const checkpoint =
            // Can only checkpoint on the last beat of the bar.
            env.op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1 and
            env.op > constants.lsm_batch_multiple;

        env.change_state(.fuzzing, .forest_compact);
        env.forest.compact(forest_compact_callback, env.op);
        try env.tick_until_state_change(.forest_compact, .fuzzing);

        if (checkpoint) {
            assert(env.checkpoint_op == null);
            env.checkpoint_op = env.op - constants.lsm_batch_multiple;

            env.change_state(.fuzzing, .forest_checkpoint);
            env.forest.checkpoint(forest_checkpoint_callback);
            try env.tick_until_state_change(.forest_checkpoint, .grid_checkpoint);

            env.grid.checkpoint(grid_checkpoint_callback);
            try env.tick_until_state_change(.grid_checkpoint, .superblock_checkpoint);

            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(cluster);
                    header.op = env.checkpoint_op.?;
                    header.set_checksum();
                    break :header header;
                },
                .manifest_references = env.forest.manifest_log.checkpoint_references(),
                .free_set_reference = env.grid.free_set_checkpoint.checkpoint_reference(),
                .client_sessions_reference = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .commit_max = env.checkpoint_op.? + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .sync_view = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
                .release = vsr.Release.minimum,
            });
            try env.tick_until_state_change(.superblock_checkpoint, .fuzzing);

            env.checkpoint_op = null;
        }
    }

    fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
        env.change_state(.superblock_format, .superblock_open);
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
        env.change_state(.superblock_open, .free_set_open);
    }

    fn grid_open_callback(grid: *Grid) void {
        const env: *Environment = @fieldParentPtr("grid", grid);
        env.change_state(.free_set_open, .forest_init);
    }

    fn forest_open_callback(forest: *Forest) void {
        const env: *Environment = @fieldParentPtr("forest", forest);
        env.change_state(.forest_open, .fuzzing);
    }

    fn grid_checkpoint_callback(grid: *Grid) void {
        const env: *Environment = @fieldParentPtr("grid", grid);
        assert(env.checkpoint_op != null);
        env.change_state(.grid_checkpoint, .superblock_checkpoint);
    }

    fn forest_checkpoint_callback(forest: *Forest) void {
        const env: *Environment = @fieldParentPtr("forest", forest);
        assert(env.checkpoint_op != null);
        env.change_state(.forest_checkpoint, .grid_checkpoint);
    }

    fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
        env.change_state(.superblock_checkpoint, .fuzzing);
    }

    fn forest_compact_callback(forest: *Forest) void {
        const env: *Environment = @fieldParentPtr("forest", forest);
        env.change_state(.forest_compact, .fuzzing);
    }

    fn scan_lookup_callback(scan_lookup: *ScanLookup, result: []const Thing) void {
        const env: *Environment = @fieldParentPtr("scan_lookup", scan_lookup);
        assert(env.scan_lookup_result == null);
        env.scan_lookup_result = result;
        env.change_state(.scanning, .fuzzing);
    }
};

pub fn main(fuzz_args: fuzz.FuzzArgs) !void {
    var rng = std.rand.DefaultPrng.init(fuzz_args.seed);
    const random = rng.random();

    // Init mocked storage.
    var storage = try Storage.init(
        allocator,
        constants.storage_size_limit_max,
        Storage.Options{
            .seed = random.int(u64),
            .read_latency_min = 0,
            .read_latency_mean = 0,
            .write_latency_min = 0,
            .write_latency_mean = 0,
            .crash_fault_probability = 0,
        },
    );
    defer storage.deinit(allocator);

    const repeat: u32 = @truncate(
        fuzz_args.events_max orelse
            random.intRangeAtMostBiased(u32, 1, 32),
    );

    try Environment.run(
        &storage,
        random,
        repeat,
    );
}

fn prefix_combine(prefix: u32, suffix: u32) u64 {
    return @as(u64, @intCast(prefix)) << 32 |
        @as(u64, @intCast(suffix));
}

fn prefix_validate(prefix: u32, value: u128) bool {
    assert(prefix != 0);
    assert(value != 0);
    assert(value >> 64 == 0); // Asserting it's not a random id.

    const value_64: u64 = @truncate(value);
    return prefix == @as(u32, @truncate(value_64 >> 32));
}
