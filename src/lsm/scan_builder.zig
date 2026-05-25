const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const constants = @import("../constants.zig");
const is_unique_key = @import("unique_key.zig").is_unique_key;
const is_composite_key = @import("composite_key.zig").is_composite_key;

const GridType = @import("../vsr/grid.zig").GridType;
const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const ScanMergeUnionType = @import("scan_merge.zig").ScanMergeUnionType;
const ScanMergeIntersectionType = @import("scan_merge.zig").ScanMergeIntersectionType;
const ScanMergeDifferenceType = @import("scan_merge.zig").ScanMergeDifferenceType;
const ScanBufferType = @import("scan_buffer.zig").ScanBufferType;
const ScanState = @import("scan_state.zig").ScanState;

const Direction = @import("../direction.zig").Direction;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;

const Error = @import("scan_buffer.zig").Error;

/// Scans work with asynchronous iterators --- streams.
///
/// Iterator has       `fn next() ?Item`,
/// while a stream has `fn next() Pending!?Item`
///
/// When a stream returns `error.Pending`, this means that it is unknown
/// whether a stream has more items or not, and further IO is required.
const Pending = error{Pending};

/// Multiple `Groove`s can be queried together in the same scan, as long as
/// related objects share the same `timestamp`.
/// For example, indexes from the `Transfers`, `TransfersPending`, and
/// `AccountEvents` Grooves can be used together in the same query, and the
/// timestamps produced by the scan can be used for lookups in any of those
/// Grooves.
pub fn ScanBuilderConfigType(comptime Forest: type) type {
    return struct {
        /// Indicates which Groove the ObjectTree comes from.
        /// Used for queries by `timestamp`.
        object: std.meta.FieldEnum(Forest.Grooves),

        /// Array of Grooves whose secondary indexes can be queried together,
        /// meaning the `timestamp` of related objects is the same across all Grooves.
        /// Index names must not conflict.
        /// There is no support for aliasing or fully qualified names.
        indexes: []const std.meta.FieldEnum(Forest.Grooves),
    };
}

/// ScanBuilder is a helper to create and combine scans using
/// any of the Groove's indexes.
pub fn ScanBuilderType(
    comptime Storage: type,
    comptime Forest: type,
    /// `Groove`s can be joined in the same scan, as long they produce
    /// `timestamps` for the the same objects.
    /// For example, indexes from the Grooves `Transfers` and `TransfersPending` can be
    /// used together in the same query, and the timestamp they produce can be used for
    /// lookups in either Grooves:
    ///
    /// ```
    /// SELECT Transfers WHERE Transfers.code=1 AND TransfersPending.status=posted
    /// ```
    /// or
    /// ```
    /// SELECT TransfersPending WHERE Transfers.user_data_32=1234
    /// ```
    comptime scan_config: ScanBuilderConfigType(Forest),
) type {
    return struct {
        /// Each `ScanTree` consumes memory and I/O, so they are limited by `lsm_scans_max`.
        forest: *Forest,
        scans: [constants.lsm_scans_max]Scan,
        scan_count: u32,

        /// Merging `ScanTree`s does not require additional resources, so `ScanMerge`s are stored
        /// in a separate buffer limited to `lsm_scans_max - 1`.
        /// If `lsm_scans_max = 4`, we can have at most 4 scans and 3 merge operations:
        /// M₁(M₂(S₁, S₂), M₃(S₃, S₄)).
        merges: [constants.lsm_scans_max - 1]Scan,
        merge_count: u32,

        const ScanBuilder = @This();
        const ScanBuffer = ScanBufferType(GridType(Storage));

        pub const Scan = ScanType(Storage, Forest, scan_config);

        pub fn init(forest: *Forest) ScanBuilder {
            return .{
                .forest = forest,
                .scans = undefined,
                .scan_count = 0,
                .merges = undefined,
                .merge_count = 0,
            };
        }

        /// Initializes a Scan over the secondary index specified by `index`,
        /// searching for an exact match in the `CompositeKey`'s prefix.
        /// Produces the criteria equivalent to `WHERE field = $value`.
        /// Results are ordered by `timestamp`.
        pub fn scan_prefix(
            self: *ScanBuilder,
            comptime index: Scan.Indexes,
            buffer: *ScanBuffer,
            snapshot: u64,
            prefix: CompositeKeyPrefixType(index),
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const field = comptime std.enums.nameCast(std.meta.FieldEnum(Scan.Dispatcher), index);
            const scan = self.scan_add(field) catch unreachable;
            const scan_impl = &@field(scan.dispatcher, @tagName(field));
            scan_impl.init(
                self.tree(index),
                buffer,
                snapshot,
                key_from_value(index, prefix, timestamp_range.min),
                key_from_value(index, prefix, timestamp_range.max),
                direction,
            );
            return scan;
        }

        /// Initializes a Scan over a unique key searching for an exact match.
        /// Produces the criteria equivalent to `WHERE id = $value`.
        /// Results are always unique.
        pub fn scan_unique_key(
            self: *ScanBuilder,
            comptime index: Scan.Indexes,
            buffer: *ScanBuffer,
            snapshot: u64,
            value: UniqueKeyType(index),
            direction: Direction,
        ) *Scan {
            comptime assert(is_unique_key(TableValueType(index)));

            const field = comptime std.enums.nameCast(std.meta.FieldEnum(Scan.Dispatcher), index);
            const scan = self.scan_add(field) catch unreachable;
            const scan_impl = &@field(scan.dispatcher, @tagName(field));
            scan_impl.init(
                self.tree(index),
                buffer,
                snapshot,
                value,
                value,
                direction,
            );
            return scan;
        }

        /// Initializes a Scan over a timestamp range.
        /// Produces the criteria equivalent to `WHERE timestamp BETWEEN $min AND $max`.
        /// Results are ordered by `timestamp`.
        pub fn scan_timestamp(
            self: *ScanBuilder,
            buffer: *ScanBuffer,
            snapshot: u64,
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const scan = self.scan_add(.timestamp) catch unreachable;
            const scan_impl = &@field(scan.dispatcher, "timestamp");
            scan_impl.init(
                self.tree(.timestamp),
                buffer,
                snapshot,
                timestamp_range.min,
                timestamp_range.max,
                direction,
            );
            return scan;
        }

        /// Initializes a Scan performing the union operation over multiple scans.
        /// E.g. S₁ ∪ S₂ ∪ Sₙ.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> OR <condition_2> OR <condition_N>`.
        ///
        /// All scans must yield results in the same direction.
        pub fn merge_union(
            self: *ScanBuilder,
            scans: []const *Scan,
        ) *Scan {
            const scan = self.merge_add(.merge_union) catch unreachable;
            const scan_impl = &@field(scan.dispatcher, "merge_union");
            scan_impl.init(scans);
            return scan;
        }

        /// Initializes a Scan performing the intersection operation over multiple scans.
        /// E.g. S₁ ∩ S₂ ∩ Sₙ.
        /// Produces the criteria equivalent to
        /// WHERE <condition_1> AND <condition_2> AND <condition_N>`.
        ///
        /// All scans must yield results in the same direction.
        pub fn merge_intersection(
            self: *ScanBuilder,
            scans: []const *Scan,
        ) *Scan {
            const scan = self.merge_add(.merge_intersection) catch unreachable;
            const scan_impl = &@field(scan.dispatcher, "merge_intersection");
            scan_impl.init(scans);
            return scan;
        }

        /// Initializes a Scan performing the difference (minus) of two scans.
        /// E.g. S₁ - S₂.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> AND NOT <condition_2>`.
        ///
        /// Both scans must yield results in the same direction.
        pub fn merge_difference(
            self: *ScanBuilder,
            scan_a: *Scan,
            scan_b: *Scan,
        ) *Scan {
            _ = scan_b;
            _ = scan_a;
            _ = self;
            stdx.unimplemented("merge_difference not implemented");
        }

        fn scan_add(
            self: *ScanBuilder,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
        ) Error!*Scan {
            if (self.scan_count == self.scans.len) {
                return Error.ScansMaxExceeded;
            }

            const scan = &self.scans[self.scan_count];
            self.scan_count += 1;
            assert(self.scan_count <= self.scans.len);

            scan.* = .{
                .dispatcher = @unionInit(
                    Scan.Dispatcher,
                    @tagName(field),
                    undefined,
                ),
                .assigned = false,
            };

            return scan;
        }

        fn merge_add(
            self: *ScanBuilder,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
        ) Error!*Scan {
            if (self.merge_count == self.merges.len) {
                return Error.ScansMaxExceeded;
            }

            const scan = &self.merges[self.merge_count];
            self.merge_count += 1;
            assert(self.merge_count <= self.merges.len);

            scan.* = .{
                .dispatcher = @unionInit(
                    Scan.Dispatcher,
                    @tagName(field),
                    undefined,
                ),
                .assigned = false,
            };

            return scan;
        }

        fn TableValueType(comptime index: Scan.Indexes) type {
            const IndexTree = Scan.TreeType(index);
            return IndexTree.Table.Value;
        }

        fn CompositeKeyPrefixType(comptime index: Scan.Indexes) type {
            const CompositeKey = TableValueType(index);
            comptime assert(is_composite_key(CompositeKey));
            return @FieldType(CompositeKey, "field");
        }

        fn UniqueKeyType(comptime index: Scan.Indexes) type {
            const UniqueKey = TableValueType(index);
            comptime assert(is_unique_key(UniqueKey));
            return @FieldType(UniqueKey, "field");
        }

        fn key_from_value(
            comptime index: Scan.Indexes,
            prefix: CompositeKeyPrefixType(index),
            timestamp: u64,
        ) TableValueType(index).Key {
            return TableValueType(index).key_from_value(&.{
                .field = prefix,
                .timestamp = timestamp,
            });
        }

        inline fn tree(
            self: *ScanBuilder,
            comptime index: Scan.Indexes,
        ) *Scan.TreeType(index) {
            const groove_field = @field(Scan.index_map, @tagName(index));
            const groove: *Scan.GrooveType(index) = &@field(
                self.forest.grooves,
                @tagName(groove_field),
            );

            return if (index == .timestamp)
                &groove.objects
            else
                &@field(groove.indexes, @tagName(index));
        }
    };
}

/// Common `Scan` interface.
///
/// Allows combining different underlying scans into a single output,
/// for example `(A₁ ∪ A₂) ∩ B₁` produces the criteria equivalent to
/// `WHERE (<condition_a1> OR <condition_a2>) AND <condition_b>`.
pub fn ScanType(
    comptime Storage: type,
    comptime Forest: type,
    comptime scan_config: ScanBuilderConfigType(Forest),
) type {
    return struct {
        const Scan = @This();

        /// This pattern of callback `fn(*Context, *Scan)` with the `Context` holding the function
        /// pointer is well suited for this use case as it allows composing multiple scans from
        /// a pool without requiring the caller to keep track of the reference of the topmost scan.
        /// Example:
        /// ```
        /// var scan1: *Scan = ... // Add some condition
        /// var scan2: *Scan = ... // Add another condition
        /// var scan3: *Scan = merge_union(scan1, scan2); // Merge both scans.
        /// scan3.read(&context); // scan3 will be returned during the callback.
        /// ```
        pub const Callback = *const fn (*Context, *Scan) void;
        pub const Context = struct {
            callback: Callback,
        };

        /// Maps the `Index` -> `Groove` relation.
        const index_map: T: {
            const FieldEnum = std.meta.FieldEnum(Forest.Grooves);
            var indexes: []const std.builtin.Type.StructField = &.{};

            // Timestamp from the ObjectTree:
            indexes = indexes ++ [_]std.builtin.Type.StructField{.{
                .name = "timestamp",
                .type = FieldEnum,
                .is_comptime = true,
                .default_value_ptr = &scan_config.object,
                .alignment = @alignOf(FieldEnum),
            }};

            // Secondary indexes from joined Grooves' IndexTrees:
            for (scan_config.indexes) |*groove_name| {
                const Groove = @FieldType(Forest.Grooves, @tagName(groove_name.*));
                for (std.meta.fields(Groove.IndexTrees)) |field| {
                    indexes = indexes ++ [_]std.builtin.Type.StructField{.{
                        .name = field.name,
                        .type = FieldEnum,
                        .is_comptime = true,
                        .default_value_ptr = groove_name,
                        .alignment = @alignOf(FieldEnum),
                    }};
                }
            }

            break :T @Type(.{ .@"struct" = .{
                .layout = .auto,
                .fields = indexes,
                .decls = &.{},
                .is_tuple = false,
            } });
        } = .{};

        pub const Indexes = std.meta.FieldEnum(@TypeOf(index_map));

        fn GrooveType(comptime index: Indexes) type {
            const groove_from_index: std.meta.FieldEnum(Forest.Grooves) = @field(
                index_map,
                @tagName(index),
            );
            return @FieldType(Forest.Grooves, @tagName(groove_from_index));
        }

        fn TreeType(comptime index: Indexes) type {
            const Groove = GrooveType(index);
            return if (index == .timestamp)
                Groove.ObjectTree
            else
                @FieldType(Groove.IndexTrees, @tagName(index));
        }

        /// Comptime dispatcher for all scan implementations that share the same interface.
        /// Generates a tagged union with an specialized `ScanTreeType` for each queryable field in
        /// the `Groove` (e.g. `timestamp`, `id` if present, and secondary indexes),
        /// plus the supported joins, and a `ScanMergeType` for each merge operation
        /// (e.g. union, intersection, and difference).
        ///
        /// Example:
        /// ```
        /// const Dispatcher = union(enum) {
        ///   .timestamp: ScanTree(...),
        ///   .id: ScanTree(...),
        ///   .code: ScanTree(...),
        ///   .ledger: ScanTree(...),
        ///   // ... All other indexes ...
        ///   .merge_union: ...
        ///   .merge_intersection: ...
        ///   .merge_difference: ...
        /// };
        /// ```
        pub const Dispatcher = T: {
            var type_info = @typeInfo(union(enum) {
                merge_union: ScanMergeUnionType(Storage, Forest, scan_config),
                merge_intersection: ScanMergeIntersectionType(Storage, Forest, scan_config),
                merge_difference: ScanMergeDifferenceType(Storage, Forest, scan_config),
            });

            for (std.enums.values(Indexes)) |index| {
                const Tree = TreeType(index);
                const ScanTree = ScanTreeType(*Context, Tree, Storage);
                type_info.@"union".fields = type_info.@"union".fields ++
                    [_]std.builtin.Type.UnionField{.{
                        .name = @tagName(index),
                        .type = ScanTree,
                        .alignment = @alignOf(ScanTree),
                    }};
            }

            // We need a tagged union for dynamic dispatching.
            type_info.@"union".tag_type = blk: {
                const union_fields = type_info.@"union".fields;
                var tag_fields: [union_fields.len]std.builtin.Type.EnumField =
                    undefined;
                for (&tag_fields, union_fields, 0..) |*tag_field, union_field, i| {
                    tag_field.* = .{
                        .name = union_field.name,
                        .value = i,
                    };
                }

                break :blk @Type(.{ .@"enum" = .{
                    .tag_type = std.math.IntFittingRange(0, tag_fields.len - 1),
                    .fields = &tag_fields,
                    .decls = &.{},
                    .is_exhaustive = true,
                } });
            };

            break :T @Type(type_info);
        };

        dispatcher: Dispatcher,
        assigned: bool,

        pub fn read(scan: *Scan, context: *Context) void {
            @setEvalBranchQuota(10_000);
            switch (scan.dispatcher) {
                inline else => |*scan_impl, tag| {
                    const Impl = @TypeOf(scan_impl.*);
                    const on_read_callback = struct {
                        fn callback(ctx: *Context, ptr: *Impl) void {
                            ctx.callback(ctx, parent(tag, ptr));
                        }
                    }.callback;
                    scan_impl.read(context, on_read_callback);
                },
            }
        }

        pub fn next(scan: *Scan) Pending!?u64 {
            @setEvalBranchQuota(10_000);
            switch (scan.dispatcher) {
                inline .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_merge| return try scan_merge.next(),
                inline else => |*scan_tree, index| {
                    while (try scan_tree.next()) |value| {
                        const ScanTree = @TypeOf(scan_tree.*);
                        if (ScanTree.Tree.Table.tombstone(&value)) {
                            // When iterating over `ScanTreeType`, it can return a tombstone, which
                            // indicates the value was deleted, and must be ignored in the results.
                            continue;
                        }

                        const Groove = GrooveType(comptime std.enums.nameCast(Indexes, index));
                        if (comptime Groove.is_primary_key(index)) {
                            // When iterating over the primary key,
                            // it can return a timestamp zero, which indicates an orphaned id.
                            if (value.timestamp == 0) {
                                assert(index != .timestamp);
                                assert(Groove.config.primary_key_orphaned);
                                continue;
                            }
                        }
                        assert(TimestampRange.valid(value.timestamp));

                        return value.timestamp;
                    }
                    return null;
                },
            }
        }

        pub fn state(scan: *const Scan) ScanState {
            switch (scan.dispatcher) {
                inline else => |*scan_impl| return scan_impl.state,
            }
        }

        pub fn snapshot(scan: *const Scan) u64 {
            return switch (scan.dispatcher) {
                inline else => |*scan_impl| scan_impl.snapshot,
            };
        }

        pub fn probe(scan: *Scan, timestamp: u64) void {
            switch (scan.dispatcher) {
                inline .timestamp,
                .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_impl| scan_impl.probe(timestamp),
                inline else => |*scan_impl, index| {
                    const ScanTree = @TypeOf(scan_impl.*);
                    const Value = ScanTree.Tree.Table.Value;

                    if (comptime is_composite_key(Value)) {
                        const prefix = prefix: {
                            const prefix_lower = Value.key_prefix(scan_impl.key_lower);
                            const prefix_upper = Value.key_prefix(scan_impl.key_upper);
                            assert(prefix_lower == prefix_upper);
                            break :prefix prefix_lower;
                        };
                        scan_impl.probe(Value.key_from_value(&.{
                            .field = prefix,
                            .timestamp = timestamp,
                        }));
                    } else {
                        const Groove = GrooveType(std.enums.nameCast(Indexes, index));
                        comptime assert(is_unique_key(Value));
                        comptime assert(Groove.config.unique_keys.len > 0);

                        // Scans over the unique keys cannot probe for a next timestamp.
                        assert(scan_impl.key_lower == scan_impl.key_upper);
                    }
                },
            }
        }

        /// Returns the direction of the output timestamp values.
        pub fn direction(scan: *const Scan) Direction {
            switch (scan.dispatcher) {
                inline .timestamp,
                .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_impl| return scan_impl.direction,
                inline else => |*scan_impl, index| {
                    const ScanTree = @TypeOf(scan_impl.*);
                    const Value = ScanTree.Tree.Table.Value;
                    if (comptime is_composite_key(Value)) {
                        // Secondary indexes can only produce results sorted by timestamp if
                        // scanning over the same key prefix.
                        assert(Value.key_prefix(scan_impl.key_lower) ==
                            Value.key_prefix(scan_impl.key_upper));
                    } else {
                        const Groove = GrooveType(std.enums.nameCast(Indexes, index));
                        comptime assert(is_unique_key(Value));
                        comptime assert(Groove.config.unique_keys.len > 0);

                        assert(scan_impl.key_lower == scan_impl.key_upper);
                    }

                    return scan_impl.direction;
                },
            }
        }

        inline fn parent(
            comptime field: std.meta.FieldEnum(Dispatcher),
            impl: *@FieldType(Dispatcher, @tagName(field)),
        ) *Scan {
            const dispatcher: *Dispatcher = @alignCast(@fieldParentPtr(
                @tagName(field),
                impl,
            ));
            return @fieldParentPtr("dispatcher", dispatcher);
        }
    };
}
