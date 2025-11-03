const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const fixtures = @import("../testing/fixtures.zig");
const fuzz = @import("../testing/fuzz.zig");
const stdx = @import("stdx");
const vsr = @import("../vsr.zig");
const Ratio = stdx.PRNG.Ratio;
const ratio = stdx.PRNG.ratio;

const log = std.log.scoped(.lsm_scan_fuzz);
const lsm = @import("tree.zig");

const TimeSim = @import("../testing/time.zig").TimeSim;
const Storage = @import("../testing/storage.zig").Storage;
const GridType = @import("../vsr/grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const ForestType = @import("forest.zig").ForestType;
const ScanLookupType = @import("scan_lookup.zig").ScanLookupType;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;
const Direction = @import("../direction.zig").Direction;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);

const batch_objects_max: u32 = @divFloor(
    constants.message_body_size_max,
    @sizeOf(Thing),
);

/// The max number of query specs generated per run.
const query_spec_max = 8;

/// The testing object.
const Thing = extern struct {
    id: u128,
    index_01: u64,
    index_02: u64,
    index_03: u64,
    index_04: u64,
    index_05: u64,
    index_06: u64,
    index_07: u64,
    index_08: u64,
    index_09: u64,
    index_10: u64,
    index_11: u64,
    index_12: u64,
    index_13: u64,
    timestamp: u64,

    fn get_index(thing: *const Thing, index: Index) u64 {
        switch (index) {
            inline else => |comptime_index| {
                return @field(thing, @tagName(comptime_index));
            },
        }
    }

    comptime {
        assert(stdx.no_padding(Thing));
        assert(@sizeOf(Thing) == 128);
        assert(@alignOf(Thing) == 16);
    }
};

const ThingsGroove = GrooveType(
    Storage,
    Thing,
    .{
        .ids = .{
            .id = 1,
            .index_01 = 2,
            .index_02 = 3,
            .index_03 = 4,
            .index_04 = 5,
            .index_05 = 6,
            .index_06 = 7,
            .index_07 = 8,
            .index_08 = 9,
            .index_09 = 10,
            .index_10 = 11,
            .index_11 = 12,
            .index_12 = 13,
            .index_13 = 14,
            .timestamp = 15,
        },
        .batch_value_count_max = .{
            .id = batch_objects_max,
            .index_01 = batch_objects_max,
            .index_02 = batch_objects_max,
            .index_03 = batch_objects_max,
            .index_04 = batch_objects_max,
            .index_05 = batch_objects_max,
            .index_06 = batch_objects_max,
            .index_07 = batch_objects_max,
            .index_08 = batch_objects_max,
            .index_09 = batch_objects_max,
            .index_10 = batch_objects_max,
            .index_11 = batch_objects_max,
            .index_12 = batch_objects_max,
            .index_13 = batch_objects_max,
            .timestamp = batch_objects_max,
        },
        .ignored = &[_][]const u8{},
        .optional = &[_][]const u8{},
        .derived = .{},
        .orphaned_ids = false,
        .objects_cache = true,
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

const Scan = ThingsGroove.ScanBuilder.Scan;

const thing_index_count = std.enums.values(Index).len;
/// The max number of indexes in a query.
const query_scans_max: comptime_int = @min(constants.lsm_scans_max, thing_index_count);
comptime {
    assert(thing_index_count >= query_scans_max);
}

/// The max number of query parts.
/// If `query_scans_max == x`, then we can have at most x fields and x - 1 merge operations.
const query_part_max = (query_scans_max * 2) - 1;

const QueryPart = union(enum) {
    const Field = struct { index: Index, value: u64 };
    const Merge = struct { operator: QueryOperator, operand_count: u8 };

    field: Field,
    merge: Merge,
};

/// The query is represented non-recursively in reverse polish notation as an array of `QueryPart`.
/// Example: `(a OR b) AND (c OR d OR e)` == `[{AND;2}, {OR;2}, {a}, {b}, {OR;3}, {c}, {d}, {e}]`.
const Query = stdx.BoundedArrayType(QueryPart, query_part_max);

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

const QuerySpec = struct {
    query: Query,
    direction: Direction,

    /// Formats the array of `QueryPart`, for debugging purposes.
    /// E.g. "((a OR b) and c)".
    pub fn format(
        self: *const QuerySpec,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        var stack: stdx.BoundedArrayType(QueryPart.Merge, query_scans_max - 1) = .{};
        var print_operator: bool = false;
        for (0..self.query.count()) |index| {
            // Reverse the RPN array in order to print in the natural order.
            const query_part = self.query.get(self.query.count() - index - 1);

            const merge_current: ?*QueryPart.Merge = if (stack.count() > 0) merge_current: {
                const merge = &stack.slice()[stack.count() - 1];
                assert(merge.operand_count > 0);
                if (print_operator) switch (merge.operator) {
                    .union_set => try writer.print(" OR ", .{}),
                    .intersection_set => try writer.print(" AND ", .{}),
                };
                break :merge_current merge;
            } else null;

            switch (query_part) {
                .field => |field| {
                    print_operator = true;
                    try writer.print("{s}", .{std.enums.tagName(Index, field.index).?});

                    if (merge_current) |merge| {
                        merge.operand_count -= 1;
                    }
                },
                .merge => |merge| {
                    print_operator = false;
                    try writer.print("(", .{});
                    stack.push(merge);
                },
            }

            if (merge_current) |merge| {
                if (merge.operand_count == 0) {
                    print_operator = true;
                    try writer.print(")", .{});
                    stack.truncate(stack.count() - 1);
                }
            }
        }

        // Closing the parenthesis from the tail of the stack:
        stdx.maybe(stack.count() > 0);
        while (stack.count() > 0) {
            try writer.print(")", .{});
            stack.truncate(stack.count() - 1);
        }
        assert(stack.count() == 0);
    }

    /// Returns whether the query results should include the specified object.
    fn query_matches(query_spec: *const QuerySpec, thing: *const Thing) bool {
        var matches = stdx.BoundedArrayType(bool, query_part_max){};
        for (query_spec.query.const_slice()) |query_part| {
            const match = switch (query_part) {
                .field => |field| thing.get_index(field.index) == field.value,
                .merge => |merge| switch (merge.operator) {
                    .union_set => match: {
                        var match: bool = false;
                        for (0..merge.operand_count) |_| match = matches.pop().? or match;
                        break :match match;
                    },
                    .intersection_set => match: {
                        var match: bool = true;
                        for (0..merge.operand_count) |_| match = matches.pop().? and match;
                        break :match match;
                    },
                },
            };
            matches.push(match);
        }
        return matches.get(matches.count() - 1);
    }
};

/// This fuzzer generates random arbitrary complex query conditions such as
/// `(a OR b) AND (c OR d OR (e AND f AND g))`.
///
/// Some limitations in place:
///
/// - Limited up to the max number of scans defined at `constants.lsm_scans_max`
///   or the number of indexed fields in `Thing`.
///
/// - The next operator must be the opposite of the previous one,
///   avoiding unnecessary use of parenthesis, such as `(a AND b) AND c`.
///   This way, the query generated can be either `a AND b AND c` without
///   precedence or `(a AND b) OR c` flipping the operator.
///
/// - Cannot repeat fields, while `(a=1 OR a=2)` is valid, this limitation avoids
///   always false conditions such as `(a=1 AND a=2)`.
const QuerySpecFuzzer = struct {
    prng: *stdx.PRNG,
    index_cardinality: [thing_index_count]u64,
    indexes_used: std.EnumSet(Index) = std.EnumSet(Index).initEmpty(),

    fn generate_fuzz_query_specs(
        prng: *stdx.PRNG,
        index_cardinality: [thing_index_count]u64,
    ) [query_spec_max]QuerySpec {
        var query_specs: [query_spec_max]QuerySpec = undefined;
        for (&query_specs) |*query_spec| {
            var fuzzer = QuerySpecFuzzer{
                .prng = prng,
                .index_cardinality = index_cardinality,
            };

            const query_field_max = prng.range_inclusive(u32, 1, query_scans_max);
            const query = fuzzer.generate_query(query_field_max);

            query_spec.* = .{
                .query = query,
                .direction = if (prng.boolean()) .ascending else .descending,
            };
        }
        return query_specs;
    }

    fn generate_query(self: *QuerySpecFuzzer, field_max: u32) Query {
        assert(field_max > 0);
        assert(field_max <= query_scans_max);

        const QueryPartTag = std.meta.Tag(QueryPart);
        const MergeStack = struct {
            index: usize,
            operand_count: u8,
            fields_remain: u32,

            fn nested_merge_field_max(merge_stack: *const @This()) u32 {
                // The query part must have at least two operands, if `operand_count == 0`
                // it can start a nested query part, but at least one field must remain for
                // the next operand.
                return merge_stack.fields_remain - @intFromBool(merge_stack.operand_count == 0);
            }
        };

        var query: Query = .{};
        if (field_max == 1) {
            // Single field queries must have just one part.
            query.push(.{ .field = self.generate_query_field() });
            return query;
        }

        // Multi field queries must start with a merge.
        var stack: stdx.BoundedArrayType(MergeStack, query_scans_max - 1) = .{};
        stack.push(.{
            .index = 0,
            .operand_count = 0,
            .fields_remain = field_max,
        });

        query.push(.{
            .merge = .{
                .operator = self.prng.enum_uniform(QueryOperator),
                .operand_count = 0,
            },
        });

        // Limiting the maximum number of merges upfront produces both simple and complex
        // queries with the same probability.
        // Otherwise, simple queries would be rare or limited to have few fields.
        const merge_max = self.prng.range_inclusive(u32, 1, field_max - 1);

        var field_remain: u32 = field_max;
        while (field_remain > 0) {
            const stack_top: *MergeStack = &stack.slice()[stack.count() - 1];
            const query_part_tag: QueryPartTag = if (stack.count() == merge_max) .field else tag: {
                // Choose randomly between `.field` or `.merge` if there are enough
                // available fields to start a new `.merge`.
                assert(stack_top.fields_remain > 0);
                const nested_merge_field_max = stack_top.nested_merge_field_max();
                stdx.maybe(nested_merge_field_max == 0);
                break :tag if (nested_merge_field_max > 1)
                    self.prng.enum_uniform(QueryPartTag)
                else
                    .field;
            };

            const query_part = switch (query_part_tag) {
                .field => field: {
                    assert(field_remain > 0);
                    field_remain -= 1;

                    assert(stack_top.fields_remain > 0);
                    stack_top.operand_count += 1;
                    stack_top.fields_remain -= 1;

                    if (stack_top.fields_remain == 0) {
                        assert(stack_top.operand_count > 1 or field_max == 1);

                        const parent = &query.slice()[stack_top.index];
                        parent.merge.operand_count = stack_top.operand_count;
                        stack.truncate(stack.count() - 1);
                    }

                    break :field QueryPart{
                        .field = self.generate_query_field(),
                    };
                },
                .merge => merge: {
                    assert(field_remain > 1);
                    const merge_field_remain = self.prng.range_inclusive(
                        u32,
                        // Merge must contain at least two fields, and at most the
                        // number of remaining field for the current merge.
                        2,
                        stack_top.nested_merge_field_max(),
                    );

                    assert(merge_field_remain > 1);
                    assert(field_remain >= merge_field_remain);

                    stack_top.fields_remain -= merge_field_remain;
                    stack_top.operand_count += 1;

                    const parent: *QueryPart.Merge = &query.slice()[stack_top.index].merge;
                    if (stack_top.fields_remain == 0) {
                        assert(stack_top.operand_count > 1);
                        parent.operand_count = stack_top.operand_count;
                        stack.truncate(stack.count() - 1);
                    }

                    stack.push(.{
                        .index = query.count(),
                        .operand_count = 0,
                        .fields_remain = merge_field_remain,
                    });

                    break :merge QueryPart{
                        .merge = .{
                            .operator = parent.operator.flip(),
                            .operand_count = 0,
                        },
                    };
                },
            };

            query.push(query_part);
        }

        assert(stack.count() == 0);

        // Represented in reverse polish notation.
        std.mem.reverse(QueryPart, query.slice());
        return query;
    }

    fn generate_query_field(self: *QuerySpecFuzzer) QueryPart.Field {
        assert(self.indexes_used.count() < thing_index_count);

        const index = while (true) {
            const index = self.prng.enum_uniform(Index);
            if (!self.indexes_used.contains(index)) {
                self.indexes_used.insert(index);
                break index;
            }
        };

        const index_cardinality = self.index_cardinality[@intFromEnum(index)];

        return QueryPart.Field{
            .index = index,
            .value = self.prng.range_inclusive(u64, 1, index_cardinality),
        };
    }
};

const Environment = struct {
    const node_count = 1024;

    // This is the smallest size that set_associative_cache will allow us.
    const cache_entries_max = 2048;
    const forest_options = Forest.GroovesOptions{
        .things = .{
            .prefetch_entries_for_read_max = batch_objects_max,
            .prefetch_entries_for_update_max = batch_objects_max,
            .cache_entries_max = cache_entries_max,
            .tree_options_object = .{ .batch_value_count_limit = batch_objects_max },
            .tree_options_id = .{ .batch_value_count_limit = batch_objects_max },
            .tree_options_index = .{
                .index_01 = .{ .batch_value_count_limit = batch_objects_max },
                .index_02 = .{ .batch_value_count_limit = batch_objects_max },
                .index_03 = .{ .batch_value_count_limit = batch_objects_max },
                .index_04 = .{ .batch_value_count_limit = batch_objects_max },
                .index_05 = .{ .batch_value_count_limit = batch_objects_max },
                .index_06 = .{ .batch_value_count_limit = batch_objects_max },
                .index_07 = .{ .batch_value_count_limit = batch_objects_max },
                .index_08 = .{ .batch_value_count_limit = batch_objects_max },
                .index_09 = .{ .batch_value_count_limit = batch_objects_max },
                .index_10 = .{ .batch_value_count_limit = batch_objects_max },
                .index_11 = .{ .batch_value_count_limit = batch_objects_max },
                .index_12 = .{ .batch_value_count_limit = batch_objects_max },
                .index_13 = .{ .batch_value_count_limit = batch_objects_max },
            },
        },
    };

    const State = enum {
        init,
        forest_init,
        forest_open,
        fuzzing,
        populating,
        scanning,
        forest_compact,
        grid_checkpoint,
        forest_checkpoint,
        superblock_checkpoint,
        grid_checkpoint_durable,
    };

    prng: *stdx.PRNG,
    state: State,

    storage: *Storage,
    time_sim: TimeSim,
    trace: Storage.Tracer,
    superblock: SuperBlock,
    superblock_context: SuperBlock.Context = undefined,
    grid: Grid,
    forest: Forest,
    model: std.ArrayListUnmanaged(Thing), // Ordered by ascending timestamp.
    model_matches: [query_spec_max]std.DynamicBitSetUnmanaged,
    ticks_remaining: usize,

    op: u64 = 0,
    checkpoint_op: ?u64 = null,

    scan_lookup: ScanLookup = undefined,
    scan_lookup_buffer: []Thing,
    scan_lookup_result: ?[]const Thing = null,

    fn init(
        env: *Environment,
        gpa: std.mem.Allocator,
        storage: *Storage,
        prng: *stdx.PRNG,
    ) !void {
        env.time_sim = fixtures.init_time(.{});
        env.trace = try fixtures.init_tracer(gpa, env.time_sim.time(), .{});
        errdefer env.trace.deinit(gpa);

        env.* = .{
            .storage = storage,
            .prng = prng,
            .state = .init,
            .time_sim = env.time_sim,
            .trace = env.trace,

            .superblock = try fixtures.init_superblock(gpa, env.storage, .{}),

            .grid = try fixtures.init_grid(gpa, &env.trace, &env.superblock, .{
                // Grid.mark_checkpoint_not_durable releases the FreeSet checkpoints blocks into
                // FreeSet.blocks_released_prior_checkpoint_durability.
                .blocks_released_prior_checkpoint_durability_max = 0,
            }),
            .forest = undefined,
            .model = .{},
            .model_matches = @splat(.{}),

            .scan_lookup_buffer = try gpa.alloc(Thing, batch_objects_max),
            .checkpoint_op = null,
            .ticks_remaining = std.math.maxInt(usize),
        };
    }

    fn deinit(env: *Environment, gpa: std.mem.Allocator) void {
        for (&env.model_matches) |*matches| matches.deinit(gpa);
        env.model.deinit(gpa);
        env.superblock.deinit(gpa);
        env.grid.deinit(gpa);
        env.trace.deinit(gpa);
        gpa.free(env.scan_lookup_buffer);
    }

    pub fn run(
        gpa: std.mem.Allocator,
        storage: *Storage,
        prng: *stdx.PRNG,
        commits_max: u32,
        query_chance: Ratio,
    ) !void {
        assert(commits_max > 0);
        log.info("commits = {}", .{commits_max});

        var env: Environment = undefined;
        try env.init(gpa, storage, prng);
        defer env.deinit(gpa);

        try env.open(gpa);
        defer env.close(gpa);

        var index_cardinality: [thing_index_count]u64 = undefined;
        for (&index_cardinality) |*cardinality| {
            cardinality.* = 1 +| fuzz.random_int_exponential(env.prng, u64, 32);
        }

        const query_specs = QuerySpecFuzzer.generate_fuzz_query_specs(env.prng, index_cardinality);
        for (&query_specs, 0..) |*query_spec, i| {
            log.info("query_specs[{}]: {} {s}", .{ i, query_spec, @tagName(query_spec.direction) });
        }

        for (0..commits_max) |_| {
            assert(env.state == .fuzzing);

            // Often insert full batches, to fill the database.
            const batch_objects = if (prng.boolean())
                batch_objects_max
            else
                prng.range_inclusive(u32, 1, batch_objects_max);
            try env.model.ensureUnusedCapacity(gpa, batch_objects);
            for (&env.model_matches) |*query_matches| {
                try query_matches.resize(gpa, env.model.items.len + batch_objects, false);
            }

            for (0..batch_objects) |_| {
                // TODO: sometimes update and delete things.
                const thing_index = env.model.items.len;
                const thing = Thing{
                    .id = env.prng.int(u128),
                    .index_01 = env.prng.range_inclusive(u64, 1, index_cardinality[0]),
                    .index_02 = env.prng.range_inclusive(u64, 1, index_cardinality[1]),
                    .index_03 = env.prng.range_inclusive(u64, 1, index_cardinality[2]),
                    .index_04 = env.prng.range_inclusive(u64, 1, index_cardinality[3]),
                    .index_05 = env.prng.range_inclusive(u64, 1, index_cardinality[4]),
                    .index_06 = env.prng.range_inclusive(u64, 1, index_cardinality[5]),
                    .index_07 = env.prng.range_inclusive(u64, 1, index_cardinality[6]),
                    .index_08 = env.prng.range_inclusive(u64, 1, index_cardinality[7]),
                    .index_09 = env.prng.range_inclusive(u64, 1, index_cardinality[8]),
                    .index_10 = env.prng.range_inclusive(u64, 1, index_cardinality[9]),
                    .index_11 = env.prng.range_inclusive(u64, 1, index_cardinality[10]),
                    .index_12 = env.prng.range_inclusive(u64, 1, index_cardinality[11]),
                    .index_13 = env.prng.range_inclusive(u64, 1, index_cardinality[12]),
                    .timestamp = thing_index + 1,
                };

                env.forest.grooves.things.insert(&thing);
                env.model.appendAssumeCapacity(thing);

                for (&query_specs, &env.model_matches) |*query_spec, *query_matches| {
                    query_matches.setValue(thing_index, query_spec.query_matches(&thing));
                }
            }
            try env.commit();

            if (env.prng.chance(query_chance)) {
                for (&query_specs, &env.model_matches) |*query_spec, *query_matches| {
                    const query_results_count = try env.run_query(query_spec, query_matches);
                    assert(query_results_count == query_matches.count()); // Sanity-check.
                }
            }
        }
        for (&query_specs, &env.model_matches) |*query_spec, *query_matches| {
            const query_results_count = try env.run_query(query_spec, query_matches);
            assert(query_results_count == query_matches.count()); // Sanity-check.
        }
    }

    fn run_query(
        env: *Environment,
        query_spec: *const QuerySpec,
        model_matches: *const std.DynamicBitSetUnmanaged,
    ) !u32 {
        assert(model_matches.bit_length >= env.model.items.len);

        var timestamp_previous: u64 = switch (query_spec.direction) {
            .ascending => 0,
            .descending => std.math.maxInt(u64),
        };

        // Execute the query repeatedly with different limits, paging until all objects are scanned.
        var results_count: u32 = 0;
        var model_offset: u32 = 0;
        while (model_offset < env.model.items.len) {
            assert(env.forest.scan_buffer_pool.scan_buffer_used == 0);
            assert(env.scan_lookup_result == null);
            defer {
                env.forest.scan_buffer_pool.reset();
                env.forest.grooves.things.scan_builder.reset();
            }

            const query_results_max: u32 = env.prng.range_inclusive(
                u32,
                1,
                @intCast(env.scan_lookup_buffer.len),
            );
            assert(query_results_max > 0);
            const query_results = results: {
                const scan = env.scan_from_condition(query_spec, timestamp_previous);
                env.scan_lookup = ScanLookup.init(&env.forest.grooves.things, scan);

                const scan_lookup_buffer = env.scan_lookup_buffer[0..query_results_max];
                env.change_state(.fuzzing, .scanning);
                env.scan_lookup.read(scan_lookup_buffer, &scan_lookup_callback);
                try env.tick_until_state_change(.scanning, .fuzzing);

                const query_results = env.scan_lookup_result.?;
                env.scan_lookup_result = null;
                break :results query_results;
            };

            var results_index: u32 = 0;
            while (model_offset < env.model.items.len) {
                defer model_offset += 1;

                const model_index = switch (query_spec.direction) {
                    .ascending => model_offset,
                    .descending => env.model.items.len - model_offset - 1,
                };

                if (model_matches.isSet(model_index)) {
                    assert(results_index < query_results.len);
                    // Positive space:
                    // - Each result is a valid, matching object from the model.
                    // - The results are ordered correctly.
                    const query_result = &query_results[results_index];
                    const model_result = &env.model.items[model_index];
                    assert(stdx.equal_bytes(Thing, model_result, query_result));

                    timestamp_previous = query_result.timestamp;
                    results_index += 1;
                    if (results_index == query_results_max) break;
                } else {
                    maybe(results_index == query_results.len);
                }
            }
            // Negative space: The query didn't miss any matching objects.
            assert(results_index == query_results.len);

            results_count += @intCast(query_results.len);
            assert(results_count <= model_matches.count());
            assert(results_count == model_matches.count() or query_results.len > 0);
        }
        assert(model_matches.count() == results_count);

        return results_count;
    }

    fn scan_from_condition(
        env: *Environment,
        query_spec: *const QuerySpec,
        timestamp_last: u64, // exclusive
    ) *Scan {
        const scan_buffer_pool = &env.forest.scan_buffer_pool;
        const things_groove = &env.forest.grooves.things;
        const scan_builder: *ThingsGroove.ScanBuilder = &things_groove.scan_builder;

        var stack = stdx.BoundedArrayType(*Scan, query_scans_max){};
        for (query_spec.query.const_slice()) |query_part| {
            switch (query_part) {
                .field => |field| {
                    const timestamp_range = if (timestamp_last == 0)
                        TimestampRange.all()
                    else if (query_spec.direction == .descending)
                        TimestampRange.lte(timestamp_last - 1)
                    else
                        TimestampRange.gte(timestamp_last + 1);
                    assert(timestamp_range.min <= timestamp_range.max);

                    const scan = switch (field.index) {
                        inline else => |comptime_index| scan_builder.scan_prefix(
                            comptime_index,
                            scan_buffer_pool.acquire_assume_capacity(),
                            lsm.snapshot_latest,
                            field.value,
                            timestamp_range,
                            query_spec.direction,
                        ),
                    };
                    stack.push(scan);
                },
                .merge => |merge| {
                    assert(merge.operand_count > 1);

                    const scans_to_merge = stack.slice()[stack.count() - merge.operand_count ..];

                    const scan = switch (merge.operator) {
                        .union_set => scan_builder.merge_union(scans_to_merge),
                        .intersection_set => scan_builder.merge_intersection(scans_to_merge),
                    };

                    stack.truncate(stack.count() - merge.operand_count);
                    stack.push(scan);
                },
            }
        }

        assert(stack.count() == 1);
        return stack.get(0);
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
            env.storage.run();
        }
        assert(env.state == next_state);
    }

    fn open(env: *Environment, gpa: std.mem.Allocator) !void {
        fixtures.open_superblock(&env.superblock);
        fixtures.open_grid(&env.grid);

        // The first checkpoint is trivially durable.
        env.grid.free_set.mark_checkpoint_durable();

        env.change_state(.init, .forest_init);
        try env.forest.init(gpa, &env.grid, .{
            .compaction_block_count = Forest.Options.compaction_block_count_min,
            .node_count = node_count,
        }, forest_options);
        env.change_state(.forest_init, .forest_open);
        env.forest.open(forest_open_callback);

        try env.tick_until_state_change(.forest_open, .fuzzing);
    }

    fn close(env: *Environment, gpa: std.mem.Allocator) void {
        env.forest.deinit(gpa);
    }

    fn commit(env: *Environment) !void {
        env.op += 1;

        // TODO Make LSM (and this fuzzer) unaware of VSR's checkpoint schedule.
        const checkpoint = env.op == vsr.Checkpoint.trigger_for_checkpoint(
            vsr.Checkpoint.checkpoint_after(env.superblock.working.vsr_state.checkpoint.header.op),
        );

        env.change_state(.fuzzing, .forest_compact);
        env.forest.compact(forest_compact_callback, env.op);
        try env.tick_until_state_change(.forest_compact, .fuzzing);

        if (checkpoint) {
            assert(env.checkpoint_op == null);
            env.checkpoint_op = env.op - constants.lsm_compaction_ops;

            env.change_state(.fuzzing, .forest_checkpoint);
            env.forest.checkpoint(forest_checkpoint_callback);
            try env.tick_until_state_change(.forest_checkpoint, .grid_checkpoint);

            env.grid.checkpoint(grid_checkpoint_callback);
            try env.tick_until_state_change(.grid_checkpoint, .superblock_checkpoint);

            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(fixtures.cluster);
                    header.op = env.checkpoint_op.?;
                    header.set_checksum();
                    break :header header;
                },
                .view_attributes = null,
                .manifest_references = env.forest.manifest_log.checkpoint_references(),
                .free_set_references = .{
                    .blocks_acquired = env.grid
                        .free_set_checkpoint_blocks_acquired.checkpoint_reference(),
                    .blocks_released = env.grid
                        .free_set_checkpoint_blocks_released.checkpoint_reference(),
                },
                .client_sessions_reference = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .commit_max = env.checkpoint_op.? + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
                .release = vsr.Release.minimum,
            });
            try env.tick_until_state_change(.superblock_checkpoint, .fuzzing);

            // The fuzzer runs in a single process, all checkpoints are trivially durable. Use
            // free_set.mark_checkpoint_durable() instead of grid.mark_checkpoint_durable(); the
            // latter requires passing a callback, which is called synchronously in fuzzers anyway.
            env.grid.mark_checkpoint_not_durable();
            env.grid.free_set.mark_checkpoint_durable();

            env.checkpoint_op = null;
        }
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

pub fn main(gpa: std.mem.Allocator, fuzz_args: fuzz.FuzzArgs) !void {
    var prng = stdx.PRNG.from_seed(fuzz_args.seed);

    var storage = try fixtures.init_storage(gpa, .{
        .seed = prng.int(u64),
        .size = constants.storage_size_limit_default,
    });
    defer storage.deinit(gpa);

    try fixtures.storage_format(gpa, &storage, .{});

    const commits_max: u32 = @intCast(
        fuzz_args.events_max orelse prng.range_inclusive(u32, 1, 1024),
    );

    try Environment.run(gpa, &storage, &prng, commits_max, lerp_query_chance(commits_max));

    log.info("Passed!", .{});
}

// Fuzzer is quadratic in commit_max, so make query probability decay linearly to 10% if
// commit_max ≥ 1_000
fn lerp_query_chance(commits_max: u64) Ratio {
    return ratio(
        // See the comptime test below.
        @max(100 - @divFloor(100 * @as(u64, @min(commits_max, 1_000)), 1_000), 10),
        100,
    );
}

comptime {
    assert(lerp_query_chance(0).numerator == 100);
    assert(lerp_query_chance(10).numerator == 99);
    assert(lerp_query_chance(500).numerator == 50);
    assert(lerp_query_chance(1_000).numerator == 10);
    assert(lerp_query_chance(10_000).numerator == 10);
}
