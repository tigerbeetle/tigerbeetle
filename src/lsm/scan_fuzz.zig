const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const allocator = fuzz.allocator;

const log = std.log.scoped(.lsm_scan_fuzz);
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
    // All indexes must be `u64` to avoid conflicting matching values,
    // the most significant bits are a seed used for assertions.
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
    checksum: u64,
    timestamp: u64,

    comptime {
        assert(stdx.no_padding(Thing));
        assert(@sizeOf(Thing) == 128);
        assert(@alignOf(Thing) == 16);
    }

    /// Initializes a struct with all fields zeroed.
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
            .checksum = 0,
            .timestamp = 0,
        };
    }

    /// Gets the field's value based on the `Index` enum.
    fn get_index(thing: *const Thing, index: Index) u64 {
        switch (index) {
            inline else => |comptime_index| {
                return @field(thing, @tagName(comptime_index));
            },
        }
    }

    /// Sets the field's value based on the `Index` enum.
    fn set_index(thing: *Thing, index: Index, value: u64) void {
        switch (index) {
            inline else => |comptime_index| {
                @field(thing, @tagName(comptime_index)) = value;
            },
        }
    }

    /// Merges all non-zero fields.
    fn merge(template: *Thing, other: Thing) void {
        stdx.maybe(stdx.zeroed(std.mem.asBytes(template)));
        assert(!stdx.zeroed(std.mem.asBytes(&other)));
        defer assert(!stdx.zeroed(std.mem.asBytes(template)));

        for (std.enums.values(Index)) |index| {
            const value = other.get_index(index);
            if (value != 0) {
                assert(template.get_index(index) == 0);
                template.set_index(index, value);
            }
        }
    }

    fn merge_all(things: []const Thing) Thing {
        var result = Thing.zeroed();
        for (things) |thing| result.merge(thing);
        return result;
    }

    /// Creates a struct from a template, setting all zeroed fields with random values and
    /// calculating the checksum of the resulting struct.
    fn from_template(
        template: Thing,
        random: std.rand.Random,
        init: struct { id: u128, timestamp: u64 },
    ) Thing {
        assert(template.id == 0);
        assert(template.timestamp == 0);
        assert(init.id != 0);
        assert(init.timestamp != 0);

        var thing: Thing = template;
        thing.id = init.id;
        thing.timestamp = init.timestamp;
        for (std.enums.values(Index)) |index| {
            const value = thing.get_index(index);
            if (value == 0) {
                // Fill the zeroed fields with random values out of the matching prefix.
                thing.set_index(
                    index,
                    prefix_combine(
                        std.math.maxInt(u32),
                        random.intRangeAtMost(u32, 1, std.math.maxInt(u32)),
                    ),
                );
            }
        }

        thing.checksum = stdx.hash_inline(thing);
        return thing;
    }

    fn checksum_valid(thing: *const Thing) bool {
        assert(thing.id != 0);
        assert(thing.timestamp != 0);
        assert(thing.checksum != 0);

        var copy = thing.*;
        copy.checksum = 0;

        return thing.checksum == stdx.hash_inline(copy);
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
        .ignored = &[_][]const u8{"checksum"},
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

const Scan = ThingsGroove.ScanBuilder.Scan;

/// The max number of indexes in a query.
const index_max: comptime_int = @min(constants.lsm_scans_max, std.enums.values(Index).len);

/// The max number of query parts.
/// If `index_max == x`, then we can have at most x fields and x - 1 merge operations.
const query_part_max = (index_max * 2) - 1;

/// The max number of query specs generated per run.
/// Always generate more than one query spec, since multiple queries can
/// test both the positive space (results must match the query) and the
/// negative space (results from other queries must not match).
const query_spec_max = 8;

const QueryPart = union(enum) {
    const Field = struct { index: Index, value: u64 };
    const Merge = struct { operator: QueryOperator, operand_count: u8 };

    field: Field,
    merge: Merge,
};

/// The query is represented non-recursively in reverse polish notation as an array of `QueryPart`.
/// Example: `(a OR b) AND (c OR d OR e)` == `[{AND;2}, {OR;2}, {a}, {b}, {OR;3}, {c}, {d}, {e}]`.
const Query = stdx.BoundedArray(
    QueryPart,
    query_part_max,
);

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
    // All matching fields must start with this prefix, to avoid collision.
    prefix: u32,
    // The query.
    query: Query,
    // Ascending or descending.
    reversed: bool,
    // Number of expected results.
    expected_results: u32,

    /// Formats the array of `QueryPart`, for debugging purposes.
    /// E.g. "((a OR b) and c)".
    pub fn format(
        self: *const QuerySpec,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        var stack: stdx.BoundedArray(QueryPart.Merge, index_max - 1) = .{};
        var print_operator: bool = false;
        for (0..self.query.count()) |index| {
            // Reverse the RPN array in order to print in the natual order.
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
                    stack.append_assume_capacity(merge);
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
};

/// This fuzzer generates random arbitrary complex query conditions such as
/// `(a OR b) AND (c OR d OR (e AND f AND g))`.
/// It also includes an array of at least one object that matches the condition.
/// Those objects are used as template to populate the database in such a way
/// that the results retrieved by the query can be asserted.
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
    random: std.rand.Random,
    prefix: u32,

    suffix_last: u32 = 0,
    indexes_used: std.EnumSet(Index) = std.EnumSet(Index).initEmpty(),

    fn generate_fuzz_query_specs(
        random: std.rand.Random,
    ) stdx.BoundedArray(QuerySpec, query_spec_max) {
        var query_specs = stdx.BoundedArray(QuerySpec, query_spec_max){};
        const query_spec_count = random.intRangeAtMostBiased(
            usize,
            2,
            query_specs.inner.capacity(),
        );

        log.info("query_spec_count = {}", .{query_spec_count});

        for (0..query_spec_count) |prefix| {
            var fuzzer = QuerySpecFuzzer{
                .random = random,
                .prefix = @intCast(prefix + 1),
            };

            const query_spec = fuzzer.generate_query_spec();
            log.info("query_specs[{}]: {}", .{ prefix, query_spec });

            query_specs.append_assume_capacity(query_spec);
        }

        return query_specs;
    }

    fn generate_query_spec(
        self: *QuerySpecFuzzer,
    ) QuerySpec {
        const field_max = self.random.intRangeAtMostBiased(u32, 1, index_max);
        const query = self.generate_query(field_max);

        return QuerySpec{
            .prefix = self.prefix,
            .query = query,
            .reversed = self.random.boolean(),
            .expected_results = 0,
        };
    }

    fn generate_query(
        self: *QuerySpecFuzzer,
        field_max: u32,
    ) Query {
        assert(field_max > 0);
        assert(field_max <= index_max);

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
            query.append_assume_capacity(.{
                .field = self.generate_query_field(),
            });

            return query;
        }

        // Multi field queries must start with a merge.
        var stack: stdx.BoundedArray(MergeStack, index_max - 1) = .{};
        stack.append_assume_capacity(.{
            .index = 0,
            .operand_count = 0,
            .fields_remain = field_max,
        });

        query.append_assume_capacity(.{
            .merge = .{
                .operator = self.random.enumValue(QueryOperator),
                .operand_count = 0,
            },
        });

        // Limiting the maximum number of merges upfront produces both simple and complex
        // queries with the same probability.
        // Otherwise, simple queries would be rare or limited to have few fields.
        const merge_max = self.random.intRangeAtMostBiased(u32, 1, field_max - 1);

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
                    self.random.enumValue(QueryPartTag)
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
                    const merge_field_remain = self.random.intRangeAtMostBiased(
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

                    stack.append_assume_capacity(.{
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

            query.append_assume_capacity(query_part);
        }

        assert(stack.count() == 0);

        // Represented in reverse polish notation.
        std.mem.reverse(QueryPart, query.slice());
        return query;
    }

    fn generate_query_field(
        self: *QuerySpecFuzzer,
    ) QueryPart.Field {
        self.suffix_last += 1;
        const value: u64 = prefix_combine(self.prefix, self.suffix_last);

        return QueryPart.Field{
            .index = self.index_random(),
            .value = value,
        };
    }

    fn index_random(self: *QuerySpecFuzzer) Index {
        const index_count = comptime std.enums.values(Index).len;
        comptime assert(index_count >= index_max);
        assert(self.indexes_used.count() < index_count);
        while (true) {
            const index = self.random.enumValue(Index);
            if (self.indexes_used.contains(index)) continue;

            self.indexes_used.insert(index);
            return index;
        }
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
        /// Repeating multiple times is valuable since it populates
        /// more data, compacts and scans again on each iteration.
        repeat: u32,
    ) !void {
        assert(repeat > 0);
        log.info("repeat = {}", .{repeat});

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

        var query_specs = QuerySpecFuzzer.generate_fuzz_query_specs(random);
        for (0..repeat) |_| {
            try env.apply(query_specs.slice());
        }
    }

    fn apply(
        env: *Environment,
        query_specs: []QuerySpec,
    ) !void {
        assert(env.state == .fuzzing);

        // Inserting one batch for each query spec.
        for (query_specs) |*query_spec| {
            try env.populate_things(query_spec);
        }

        // Executing each query spec.
        for (query_specs) |*query_spec| {
            log.debug(
                \\prefix: {}
                \\object_count: {}
                \\expected_results: {}
                \\reversed: {}
                \\query: {}
                \\
            , .{
                query_spec.prefix,
                env.object_count,
                query_spec.expected_results,
                query_spec.reversed,
                query_spec,
            });

            try env.run_query(query_spec);
        }
    }

    // TODO: sometimes update and delete things.
    fn populate_things(env: *Environment, query_spec: *QuerySpec) !void {
        for (0..batch_max) |_| {
            // Total number of objects inserted.
            env.object_count += 1;

            // Non-match inserted just for creating "noise".
            const noise_probability = 20;
            if (chance(env.random, noise_probability)) {
                var dummy = Thing.zeroed();
                env.forest.grooves.things.insert(&dummy.from_template(
                    env.random,
                    .{
                        .id = env.random.int(u128),
                        .timestamp = env.object_count,
                    },
                ));

                continue;
            }

            const template = env.template_matching_query(query_spec);
            query_spec.expected_results += 1; // Expected objects that match the spec.
            const thing = template.from_template(
                env.random,
                .{
                    .id = prefix_combine(
                        query_spec.prefix,
                        query_spec.expected_results,
                    ),
                    .timestamp = env.object_count,
                },
            );
            env.forest.grooves.things.insert(&thing);
        }

        try env.commit();
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
    fn template_matching_query(env: *const Environment, query_spec: *const QuerySpec) Thing {
        var stack: [query_part_max]Thing = undefined;
        var stack_top: usize = 0;
        for (query_spec.query.const_slice()) |query_part| {
            switch (query_part) {
                .field => |field| {
                    var thing = Thing.zeroed();
                    thing.set_index(field.index, field.value);
                    stack[stack_top] = thing;
                    stack_top += 1;
                },
                .merge => |merge| {
                    const operands = stack[stack_top - merge.operand_count .. stack_top];
                    const result = switch (merge.operator) {
                        .union_set => union_set: {
                            const index = env.random.uintLessThan(usize, operands.len);
                            break :union_set if (env.random.boolean())
                                operands[index]
                            else
                                // Union `(a OR B)` should also match if the element contains
                                // both `a` and `b`.
                                Thing.merge_all(operands[index..][0..env.random.intRangeAtMost(
                                    usize,
                                    1,
                                    operands.len - index,
                                )]);
                        },
                        // Intersection matches only when the element contains all conditions.
                        .intersection_set => Thing.merge_all(operands),
                    };
                    stack_top -= merge.operand_count;
                    stack[stack_top] = result;
                    stack_top += 1;
                },
            }
        }

        assert(stack_top == 1);
        return stack[0];
    }

    fn run_query(env: *Environment, query_spec: *QuerySpec) !void {
        assert(query_spec.expected_results > 0);

        const pages = stdx.div_ceil(query_spec.expected_results, batch_max);
        assert(pages > 0);

        var result_count: u32 = 0;
        var timestamp_prev: u64 = if (query_spec.reversed)
            std.math.maxInt(u64)
        else
            0;

        for (0..pages) |page| {
            const results = try env.fetch_page(query_spec, timestamp_prev);

            for (results) |result| {
                if (query_spec.reversed)
                    assert(timestamp_prev > result.timestamp)
                else
                    assert(timestamp_prev < result.timestamp);
                timestamp_prev = result.timestamp;

                assert(prefix_validate(query_spec.prefix, result.id));
                assert(result.checksum_valid());
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

    fn fetch_page(
        env: *Environment,
        query_spec: *const QuerySpec,
        timestamp_last: u64, // exclusive
    ) ![]const Thing {
        assert(env.forest.scan_buffer_pool.scan_buffer_used == 0);
        defer {
            assert(env.forest.scan_buffer_pool.scan_buffer_used > 0);

            env.forest.scan_buffer_pool.reset();
            env.forest.grooves.things.scan_builder.reset();
        }

        const scan = env.scan_from_condition(
            &query_spec.query,
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
        query: *const Query,
        timestamp_last: u64, // exclusive
        reversed: bool,
    ) *Scan {
        const scan_buffer_pool = &env.forest.scan_buffer_pool;
        const things_groove = &env.forest.grooves.things;
        const scan_builder: *ThingsGroove.ScanBuilder = &things_groove.scan_builder;

        var stack = stdx.BoundedArray(*Scan, index_max){};
        for (query.const_slice()) |query_part| {
            switch (query_part) {
                .field => |field| {
                    const direction: Direction = if (reversed) .descending else .ascending;
                    const timestamp_range = if (timestamp_last == 0)
                        TimestampRange.all()
                    else if (reversed)
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
                            direction,
                        ),
                    };
                    stack.append_assume_capacity(scan);
                },
                .merge => |merge| {
                    assert(merge.operand_count > 1);

                    const scans_to_merge = stack.slice()[stack.count() - merge.operand_count ..];

                    const scan = switch (merge.operator) {
                        .union_set => scan_builder.merge_union(scans_to_merge),
                        .intersection_set => scan_builder.merge_intersection(scans_to_merge),
                    };

                    stack.truncate(stack.count() - merge.operand_count);
                    stack.append_assume_capacity(scan);
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

    const repeat: u32 = @intCast(
        fuzz_args.events_max orelse
            random.intRangeAtMostBiased(u32, 1, 32),
    );

    try Environment.run(
        &storage,
        random,
        repeat,
    );

    log.info("Passed!", .{});
}

fn prefix_combine(prefix: u32, suffix: u32) u64 {
    assert(prefix != 0);
    assert(suffix != 0);
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

/// Returns true, `p` percent of the time, else false.
fn chance(random: std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThanBiased(u8, 100) < p;
}
