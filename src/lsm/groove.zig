const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const TableType = @import("table.zig").TableType;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("../vsr/grid.zig").GridType;
const CompositeKeyType = @import("composite_key.zig").CompositeKeyType;
const NodePool = @import("node_pool.zig").NodePoolType(constants.lsm_manifest_node_size, 16);
const CacheMapType = @import("cache_map.zig").CacheMapType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ScanBuilderType = @import("scan_builder.zig").ScanBuilderType;

const snapshot_latest = @import("tree.zig").snapshot_latest;

fn ObjectTreeHelperType(comptime Object: type) type {
    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    return struct {
        inline fn key_from_value(value: *const Object) u64 {
            return value.timestamp & ~@as(u64, tombstone_bit);
        }

        const sentinel_key = std.math.maxInt(u64);
        const tombstone_bit = 1 << (64 - 1);

        inline fn tombstone(value: *const Object) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        inline fn tombstone_from_key(timestamp: u64) Object {
            assert(timestamp & tombstone_bit == 0);

            var value = std.mem.zeroes(Object); // Full zero-initialized Value.
            value.timestamp = timestamp | tombstone_bit;
            return value;
        }
    };
}

const IdTreeValue = extern struct {
    id: u128,
    timestamp: u64,
    padding: u64 = 0,

    comptime {
        // Assert that there is no implicit padding.
        assert(@sizeOf(IdTreeValue) == 32);
        assert(stdx.no_padding(IdTreeValue));
    }

    inline fn key_from_value(value: *const IdTreeValue) u128 {
        return value.id;
    }

    const sentinel_key = std.math.maxInt(u128);
    const tombstone_bit = 1 << (64 - 1);

    inline fn tombstone(value: *const IdTreeValue) bool {
        return (value.timestamp & tombstone_bit) != 0;
    }

    inline fn tombstone_from_key(id: u128) IdTreeValue {
        return .{
            .id = id,
            .timestamp = tombstone_bit,
        };
    }
};

/// Normalizes index tree field types into either u64 or u128 for CompositeKey
fn IndexCompositeKeyType(comptime Field: type) type {
    switch (@typeInfo(Field)) {
        .Void => return void,
        .Enum => |e| {
            return switch (@bitSizeOf(e.tag_type)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u65)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported enum tag for index: " ++ @typeName(e.tag_type)),
            };
        },
        .Int => |i| {
            if (i.signedness != .unsigned) {
                @compileError("Index int type (" ++ @typeName(Field) ++ ") is not unsigned");
            }
            return switch (@bitSizeOf(Field)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u65)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported int type for index: " ++ @typeName(Field)),
            };
        },
        else => @compileError("Index type " ++ @typeName(Field) ++ " is not supported"),
    }
}

comptime {
    assert(IndexCompositeKeyType(void) == void);
    assert(IndexCompositeKeyType(u0) == u64);
    assert(IndexCompositeKeyType(enum(u0) { x }) == u64);

    assert(IndexCompositeKeyType(u1) == u64);
    assert(IndexCompositeKeyType(u16) == u64);
    assert(IndexCompositeKeyType(enum(u16) { x }) == u64);

    assert(IndexCompositeKeyType(u32) == u64);
    assert(IndexCompositeKeyType(u63) == u64);
    assert(IndexCompositeKeyType(u64) == u64);

    assert(IndexCompositeKeyType(enum(u65) { x }) == u128);
    assert(IndexCompositeKeyType(u65) == u128);
    assert(IndexCompositeKeyType(u128) == u128);
}

fn IndexTreeType(
    comptime Storage: type,
    comptime Field: type,
    comptime table_value_count_max: usize,
) type {
    const CompositeKey = CompositeKeyType(IndexCompositeKeyType(Field));
    const Table = TableType(
        CompositeKey.Key,
        CompositeKey,
        CompositeKey.key_from_value,
        CompositeKey.sentinel_key,
        CompositeKey.tombstone,
        CompositeKey.tombstone_from_key,
        table_value_count_max,
        .secondary_index,
    );

    return TreeType(Table, Storage);
}

/// A Groove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GrooveType(
    comptime Storage: type,
    comptime Object: type,
    /// An anonymous struct instance which contains the following:
    ///
    /// - ids: { .tree = u16 }:
    ///     An anonymous struct which maps each of the groove's trees to a stable, forest-unique,
    ///     tree identifier.
    ///
    /// - batch_value_count_max: { .field = usize }:
    ///     An anonymous struct which contains, for each field of `Object`,
    ///     the maximum number of values per table per batch for the corresponding index tree.
    ///
    /// - ignored: [][]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - optional: [][]const u8:
    ///     An array of fields that should *not* index zero values.
    ///
    /// - derived: { .field = *const fn (*const Object) ?DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    ///
    /// - orphaned_ids: bool:
    ///     Whether Groove should store objectless `id`s to prevent their reuse.
    ///     Should be `true` only if the object contains an `id` field.
    ///
    /// - objects_cache: bool:
    ///     Whether Groove should have an ObjectCache.
    ///     Should be `false` only if both `prefetch_entries_for_update_max` and
    ///     `prefetch_entries_for_read_max` are set to 0.
    comptime groove_options: anytype,
) type {
    @setEvalBranchQuota(64_000);

    const has_id = @hasField(Object, "id");
    comptime if (has_id) assert(std.meta.fieldInfo(Object, .id).type == u128);
    comptime if (groove_options.orphaned_ids) assert(has_id);

    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    comptime var index_fields: []const std.builtin.Type.StructField = &.{};

    const primary_field = if (has_id) "id" else "timestamp";
    const PrimaryKey = @TypeOf(@field(@as(Object, undefined), primary_field));

    // Generate index LSM trees from the struct fields.
    for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        //
        // By default, we ignore the "timestamp" field since it's a special identifier.
        // Since the "timestamp" is ignored by default, it shouldn't be provided
        // in groove_options.ignored.
        comptime var ignored =
            mem.eql(u8, field.name, "timestamp") or mem.eql(u8, field.name, "id");
        for (groove_options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            comptime assert(!std.mem.eql(u8, ignored_field_name, "id"));
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }

        if (!ignored) {
            const table_value_count_max = constants.lsm_compaction_ops *
                @field(groove_options.batch_value_count_max, field.name);
            const IndexTree = IndexTreeType(Storage, field.type, table_value_count_max);
            index_fields = index_fields ++ [_]std.builtin.Type.StructField{
                .{
                    .name = field.name,
                    .type = IndexTree,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(IndexTree),
                },
            };
        }
    }

    // Generate IndexTrees for fields derived from the Value in groove_options.
    const derived_fields = std.meta.fields(@TypeOf(groove_options.derived));
    for (derived_fields) |field| {
        // Get the function info for the derived field.
        const derive_func = @field(groove_options.derived, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).Fn;

        // Make sure it has only one argument.
        if (derive_func_info.params.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Make sure the function takes in a reference to the Value:
        const derive_arg = derive_func_info.params[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.type != *const Object) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Get the return value from the derived field as the DerivedType.
        if (derive_func_info.return_type == null) {
            @compileError("expected derive fn to return valid tree index type");
        }

        const derive_return_type = @typeInfo(derive_func_info.return_type.?);
        if (derive_return_type != .Optional) {
            @compileError("expected derive fn to return optional tree index type");
        }

        const DerivedType = derive_return_type.Optional.child;
        const table_value_count_max = constants.lsm_compaction_ops *
            @field(groove_options.batch_value_count_max, field.name);
        const IndexTree = IndexTreeType(Storage, DerivedType, table_value_count_max);

        index_fields = index_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    comptime var index_options_fields: [index_fields.len]std.builtin.Type.StructField = undefined;
    for (index_fields, 0..) |index_field, i| {
        const IndexTree = index_field.type;
        index_options_fields[i] = .{
            .name = index_field.name,
            .type = IndexTree.Options,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(IndexTree.Options),
        };
    }

    // Verify that every tree referenced by "optional" corresponds to an actual field.
    for (groove_options.optional) |field_name| {
        if (!@hasField(Object, field_name)) {
            std.debug.panic("optional: unrecognized field name: {s}", .{field_name});
        }
    }

    const ObjectTreeHelper = ObjectTreeHelperType(Object);

    const _ObjectTree = blk: {
        const table_value_count_max = constants.lsm_compaction_ops *
            groove_options.batch_value_count_max.timestamp;
        const Table = TableType(
            u64, // key = timestamp
            Object,
            ObjectTreeHelper.key_from_value,
            ObjectTreeHelper.sentinel_key,
            ObjectTreeHelper.tombstone,
            ObjectTreeHelper.tombstone_from_key,
            table_value_count_max,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IdTree = if (!has_id) void else blk: {
        const table_value_count_max = constants.lsm_compaction_ops *
            groove_options.batch_value_count_max.id;
        const Table = TableType(
            u128,
            IdTreeValue,
            IdTreeValue.key_from_value,
            IdTreeValue.sentinel_key,
            IdTreeValue.tombstone,
            IdTreeValue.tombstone_from_key,
            table_value_count_max,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IndexTrees = @Type(.{
        .Struct = .{
            .layout = .auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });
    const _IndexTreeOptions = @Type(.{
        .Struct = .{
            .layout = .auto,
            .fields = &index_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const has_scan = index_fields.len > 0;

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(_IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len -
        groove_options.ignored.len -
        // The id/timestamp fields are implicitly ignored since it's the primary key for ObjectTree:
        (@as(usize, 1) + @intFromBool(has_id)) +
        std.meta.fields(@TypeOf(groove_options.derived)).len;

    assert(indexes_count_actual == indexes_count_expect);
    assert(indexes_count_actual == std.meta.fields(_IndexTreeOptions).len);

    const _IndexTreeFieldHelperType = struct {
        fn HelperType(comptime field_name: []const u8) type {
            return struct {
                pub const Index = type: {
                    if (is_derived) {
                        const derived_fn = @typeInfo(@TypeOf(@field(
                            groove_options.derived,
                            field_name,
                        )));
                        assert(derived_fn == .Fn);
                        assert(derived_fn.Fn.return_type != null);

                        const return_type = @typeInfo(derived_fn.Fn.return_type.?);
                        assert(return_type == .Optional);
                        break :type return_type.Optional.child;
                    }

                    break :type @TypeOf(@field(@as(Object, undefined), field_name));
                };
                pub const IndexPrefix = switch (@typeInfo(Index)) {
                    .Void => void,
                    .Int => Index,
                    .Enum => |info| info.tag_type,
                    else => @compileError("Unsupported index type for " ++ field_name),
                };

                const is_derived: bool = is_derived: {
                    for (derived_fields) |derived_field| {
                        if (std.mem.eql(u8, derived_field.name, field_name)) break :is_derived true;
                    }
                    break :is_derived false;
                };

                const allow_zero: bool = allow_zero: {
                    for (groove_options.optional) |optional| {
                        if (std.mem.eql(u8, field_name, optional)) {
                            assert(!is_derived);
                            break :allow_zero false;
                        }
                    }
                    break :allow_zero true;
                };

                inline fn as_prefix(index: Index) IndexPrefix {
                    return switch (@typeInfo(Index)) {
                        .Void => {},
                        .Int => index,
                        .Enum => @intFromEnum(index),
                        else => unreachable,
                    };
                }

                /// Try to extract an index from the object, deriving it when necessary.
                /// Null means the value should not be indexed.
                pub fn index_from_object(object: *const Object) ?IndexPrefix {
                    if (is_derived) {
                        return if (@field(groove_options.derived, field_name)(object)) |value|
                            as_prefix(value)
                        else
                            null;
                    } else {
                        const value = as_prefix(@field(object, field_name));
                        return if (allow_zero or value != 0)
                            value
                        else
                            null;
                    }
                }
            };
        }
    }.HelperType;

    const ObjectsCacheHelpers = struct {
        const tombstone_bit = 1 << (64 - 1);

        inline fn key_from_value(value: *const Object) PrimaryKey {
            if (has_id) {
                return value.id;
            } else {
                return value.timestamp & ~@as(u64, tombstone_bit);
            }
        }

        inline fn hash(key: PrimaryKey) u64 {
            return stdx.hash_inline(key);
        }

        inline fn tombstone_from_key(a: PrimaryKey) Object {
            var obj: Object = undefined;
            if (has_id) {
                obj.id = a;
                obj.timestamp = 0;
            } else {
                obj.timestamp = a;
            }
            obj.timestamp |= tombstone_bit;
            return obj;
        }

        inline fn tombstone(a: *const Object) bool {
            return (a.timestamp & tombstone_bit) != 0;
        }
    };

    const _ObjectsCache = if (groove_options.objects_cache) CacheMapType(
        PrimaryKey,
        Object,
        ObjectsCacheHelpers.key_from_value,
        ObjectsCacheHelpers.hash,
        ObjectsCacheHelpers.tombstone_from_key,
        ObjectsCacheHelpers.tombstone,
    ) else void;

    const TimestampSet = struct {
        const TimestampSet = @This();
        const Found = union(enum) { found: u128, not_found };
        const Map = std.AutoHashMapUnmanaged(u64, Found);

        map: Map,

        fn init(self: *TimestampSet, allocator: mem.Allocator, entries_max: u32) !void {
            self.* = .{
                .map = undefined,
            };

            self.map = .{};
            try self.map.ensureTotalCapacity(allocator, entries_max);
            errdefer self.map.deinit(allocator);
        }

        fn deinit(self: *TimestampSet, allocator: mem.Allocator) void {
            self.map.deinit(allocator);
            self.* = undefined;
        }

        fn reset(self: *TimestampSet) void {
            self.map.clearRetainingCapacity();
        }

        /// Marks the timestamp as "found" or "not found".
        /// Can be called only once per timestamp.
        fn set(self: *TimestampSet, timestamp: u64, value: Found) void {
            self.map.putAssumeCapacityNoClobber(timestamp, value);
        }

        /// Whether the previously enqueued timestamp was found or not.
        fn get(self: *const TimestampSet, timestamp: u64) Found {
            const result = self.map.get(timestamp);
            assert(result != null);

            return result.?;
        }

        fn has(self: *const TimestampSet, timestamp: u64) bool {
            return self.map.contains(timestamp);
        }
    };

    return struct {
        const Groove = @This();

        pub const ObjectTree = _ObjectTree;
        pub const IdTree = _IdTree;
        pub const IndexTrees = _IndexTrees;
        pub const ObjectsCache = _ObjectsCache;
        pub const config = groove_options;

        /// Helper function for interacting with an Index field type.
        pub const IndexTreeFieldHelperType = _IndexTreeFieldHelperType;

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const KeyType = enum {
            /// Either `id` or `timestamp` for objects without the id field.
            /// This is the same key used by the object cache map.
            primary_key,

            /// Lookup by `timestamp` in an object where the object cache map is indexed by `id`.
            /// In this case, the `timestamp` is also indexed to support indirect lookups such as
            /// `exists()` and `get_by_timestamp()`.
            /// Invariant: `has_id` is true.
            timestamp,
        };

        const PrefetchKey = union(enum) {
            id: PrimaryKey,
            timestamp: u64,
        };

        const PrefetchKeys = std.AutoHashMapUnmanaged(
            PrefetchKey,
            struct {
                level: u8,
                lookup_by: KeyType,
            },
        );

        const LookupResult = union(enum) {
            found_object: Object,
            found_orphaned_id,
            not_found,
        };

        pub const ScanBuilder = if (has_scan) ScanBuilderType(Groove, Storage) else void;

        grid: *Grid,
        objects: ObjectTree,
        ids: IdTree,
        indexes: IndexTrees,

        /// Object IDs and timestamps enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_keys: PrefetchKeys,

        /// The snapshot to prefetch from.
        prefetch_snapshot: ?u64 = null,

        /// This is used to accelerate point lookups and is not used for range queries.
        /// It's also where prefetched data is loaded into, so we don't have a different
        /// prefetch cache to our object cache.
        ///
        /// The values cache is only used for the latest snapshot for simplicity.
        /// Earlier snapshots will still be able to utilize the block cache.
        ///
        /// The values cache is updated on every `insert()`/`upsert()`/`remove()` and stores
        /// a duplicate of data that's already in table_mutable. This is done because
        /// keeping table_mutable as an array, and simplifying the compaction path
        /// is faster than trying to amortize and save memory.
        ///
        /// Invariant: if there is an objects_cache then if something is in the mutable
        /// table, it _must_ exist in our object cache.
        /// Otherwise, the ObjectsCache is of type void.
        objects_cache: ObjectsCache,

        timestamps: if (has_id) TimestampSet else void,

        scan_builder: ScanBuilder,

        pub const IndexTreeOptions = _IndexTreeOptions;

        pub const Options = struct {
            /// The maximum number of objects that might be prefetched and not modified by a batch.
            prefetch_entries_for_read_max: u32,
            /// The maximum number of objects that might be prefetched and then modified by a batch.
            prefetch_entries_for_update_max: u32,
            cache_entries_max: u32,

            tree_options_object: ObjectTree.Options,
            tree_options_id: if (has_id) IdTree.Options else void,
            tree_options_index: IndexTreeOptions,
        };

        pub fn init(
            groove: *Groove,
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            options: Options,
        ) !void {
            assert(options.tree_options_object.batch_value_count_limit *
                constants.lsm_compaction_ops <= ObjectTree.Table.value_count_max);

            groove.* = .{
                .grid = grid,

                .objects = undefined,
                .ids = undefined,
                .indexes = undefined,
                .prefetch_keys = undefined,
                .objects_cache = if (ObjectsCache != void) undefined else {},
                .timestamps = undefined,
                .scan_builder = undefined,
            };

            groove.objects_cache = if (ObjectsCache != void) try ObjectsCache.init(allocator, .{
                .cache_value_count_max = options.cache_entries_max,
                // In the worst case, each stash must be able to store
                // batch_value_count_limit per beat (to contain either TableMutable or
                // TableImmutable) as well as the maximum number of prefetches a bar may
                // perform, excluding prefetches already accounted
                // for by batch_value_count_limit.
                .stash_value_count_max = constants.lsm_compaction_ops *
                    (options.tree_options_object.batch_value_count_limit +
                    options.prefetch_entries_for_read_max),

                // Scopes are limited to a single beat, so the maximum number of entries in
                // a single scope is batch_value_count_limit (total – not per beat).
                .scope_value_count_max = options.tree_options_object.batch_value_count_limit,

                .name = ObjectTree.tree_name(),
            }) else {
                // If there are no modifications or point lookups on the Groove then
                // no `objects_cache` is needed.
                assert(options.prefetch_entries_for_read_max == 0);
                assert(options.prefetch_entries_for_update_max == 0);
                {}
            };

            errdefer if (ObjectsCache != void) groove.objects_cache.deinit(allocator);

            // Initialize the object LSM tree.
            try groove.objects.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "timestamp"),
                    .name = ObjectTree.tree_name(),
                },
                options.tree_options_object,
            );
            errdefer groove.objects.deinit(allocator);

            if (has_id) try groove.ids.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "id"),
                    .name = ObjectTree.tree_name() ++ ".id",
                },
                options.tree_options_id,
            );
            errdefer if (has_id) groove.ids.deinit(allocator);

            var index_trees_initialized: usize = 0;
            // Make sure to deinit initialized index LSM trees on error.
            errdefer inline for (std.meta.fields(IndexTrees), 0..) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    const Tree = field.type;
                    const tree: *Tree = &@field(groove.indexes, field.name);
                    tree.deinit(allocator);
                }
            };

            // Initialize index LSM trees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                const Tree = field.type;
                const tree: *Tree = &@field(groove.indexes, field.name);
                try tree.init(
                    allocator,
                    node_pool,
                    grid,
                    .{
                        .id = @field(groove_options.ids, field.name),
                        .name = ObjectTree.tree_name() ++ "." ++ field.name,
                    },
                    @field(options.tree_options_index, field.name),
                );
                index_trees_initialized += 1;
            }

            groove.prefetch_keys = .{};
            try groove.prefetch_keys.ensureTotalCapacity(
                allocator,
                options.prefetch_entries_for_read_max + options.prefetch_entries_for_update_max,
            );
            errdefer groove.prefetch_keys.deinit(allocator);

            if (has_id) try groove.timestamps.init(
                allocator,
                options.prefetch_entries_for_read_max,
            );
            errdefer if (has_id) groove.timestamps.deinit(allocator);

            if (has_scan) try groove.scan_builder.init(allocator);
            errdefer if (has_scan) groove.scan_builder.deinit(allocator);
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);
            if (has_id) groove.ids.deinit(allocator);

            groove.prefetch_keys.deinit(allocator);

            if (ObjectsCache != void) groove.objects_cache.deinit(allocator);
            if (has_id) groove.timestamps.deinit(allocator);
            if (has_scan) groove.scan_builder.deinit(allocator);

            groove.* = undefined;
        }

        pub fn reset(groove: *Groove) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).reset();
            }
            groove.objects.reset();
            if (has_id) groove.ids.reset();

            groove.prefetch_keys.clearRetainingCapacity();

            if (ObjectsCache != void) groove.objects_cache.reset();

            if (has_id) groove.timestamps.reset();
            if (has_scan) groove.scan_builder.reset();

            groove.* = .{
                .grid = groove.grid,
                .objects = groove.objects,
                .ids = groove.ids,
                .indexes = groove.indexes,
                .prefetch_keys = groove.prefetch_keys,
                .prefetch_snapshot = null,
                .objects_cache = groove.objects_cache,
                .timestamps = groove.timestamps,
                .scan_builder = groove.scan_builder,
            };
        }

        pub fn get(groove: *const Groove, key: PrimaryKey) LookupResult {
            if (groove.objects_cache.get(key)) |object| {
                if (object.timestamp == 0) {
                    assert(has_id);
                    assert(groove_options.orphaned_ids);
                    return .found_orphaned_id;
                }

                return .{ .found_object = object.* };
            }

            return .not_found;
        }

        /// Looks up an object by `timestamp`.
        /// Use `get()` for objects that don't have the `id` field.
        /// The timestamp must have been passed to `prefetch_enqueue_by_timestamp`.
        pub fn get_by_timestamp(groove: *const Groove, timestamp: u64) LookupResult {
            // Only applicable to objects with an `id` field.
            // Use `get` if the object is already keyed by timestamp.
            comptime assert(has_id);
            assert(timestamp >= TimestampRange.timestamp_min);
            assert(timestamp <= TimestampRange.timestamp_max);

            return switch (groove.timestamps.get(timestamp)) {
                .found => |id| groove.get(id),
                .not_found => .not_found,
            };
        }

        /// Returns whether an object with this timestamp exists or not.
        /// The timestamp to be checked must have been passed to `prefetch_exists_enqueue`.
        pub fn exists(groove: *const Groove, timestamp: u64) bool {
            // Only applicable to objects with an `id` field.
            // Use `get` if the object is already keyed by timestamp.
            comptime assert(has_id);
            assert(timestamp >= TimestampRange.timestamp_min);
            assert(timestamp <= TimestampRange.timestamp_max);

            return groove.timestamps.get(timestamp) == .found;
        }

        /// Must be called directly before the state machine begins queuing ids for prefetch.
        /// When `snapshot` is null, prefetch from the current snapshot.
        pub fn prefetch_setup(groove: *Groove, snapshot: ?u64) void {
            // We currently don't have anything that uses or tests snapshots. Leave this
            // here as a warning that they're not fully tested yet.
            assert(snapshot == null);

            const snapshot_target = snapshot orelse snapshot_latest;
            assert(snapshot_target <= snapshot_latest);

            groove.prefetch_snapshot = snapshot_target;
            assert(groove.prefetch_keys.count() == 0);

            if (has_id) groove.timestamps.reset();
        }

        /// This must be called by the state machine for every key to be prefetched.
        /// We tolerate duplicate IDs enqueued by the state machine.
        /// For example, if all unique operations require the same two dependencies.
        pub fn prefetch_enqueue(groove: *Groove, key: PrimaryKey) void {
            // No need to check again if the key is already present.
            if (groove.prefetch_keys.contains(.{ .id = key })) return;

            if (has_id) {
                if (!groove.ids.key_range_contains(groove.prefetch_snapshot.?, key)) return;

                if (groove.objects_cache.has(key)) {
                    return;
                }

                groove.prefetch_from_memory_by_id(key);
            } else {
                if (groove.objects_cache.has(key)) {
                    return;
                }

                groove.prefetch_from_memory_by_timestamp(key, .primary_key);
            }
        }

        /// This must be called by the state machine for every timestamp to be checked by `exists`.
        /// The first call to this function may trigger the sorting of the mutable table, which is
        /// likely a no-op since timestamps are strictly increasing and the table should already
        /// be sorted, except for objects that are frequently updated (e.g., accounts).
        /// We tolerate duplicate timestamps enqueued by the state machine.
        pub fn prefetch_enqueue_by_timestamp(
            groove: *Groove,
            timestamp: u64,
        ) void {
            // Only applicable to objects with an `id` field.
            // Use `prefetch_enqueue` if the object is already keyed by timestamp.
            comptime assert(has_id);

            // Instead of asserting, we allow and ignore invalid timestamps (most likely zero),
            // so the prefetch step does not need to verify the data's validity.
            const timestamp_valid = timestamp >= TimestampRange.timestamp_min and
                timestamp <= TimestampRange.timestamp_max;
            if (!timestamp_valid) return;

            // No need to check again if the key is already present or enqueued for prefetching.
            if (groove.timestamps.has(timestamp) or
                groove.prefetch_keys.contains(.{ .timestamp = timestamp })) return;

            // The mutable table needs to be sorted to enable searching by timestamp.
            // The immutable table will be searched by `prefetch_from_memory_by_timestamp`.
            groove.objects.table_mutable.sort();
            if (groove.objects.table_mutable.get(timestamp)) |object| {
                assert(object.timestamp == timestamp);
                groove.timestamps.set(timestamp, .{ .found = object.id });
                return;
            }

            groove.prefetch_from_memory_by_timestamp(timestamp, .timestamp);
        }

        /// This function attempts to prefetch a value for the given id from the IdTree's
        /// table blocks in the grid cache.
        /// If found in the IdTree, we attempt to prefetch a value for the timestamp.
        fn prefetch_from_memory_by_id(groove: *Groove, id: PrimaryKey) void {
            switch (groove.ids.lookup_from_levels_cache(
                groove.prefetch_snapshot.?,
                id,
            )) {
                .negative => {},
                .positive => |id_tree_value| {
                    if (IdTreeValue.tombstone(id_tree_value)) return;

                    if (id_tree_value.timestamp == 0) {
                        assert(groove_options.orphaned_ids);

                        // Zeroed timestamp indicates the object is not present,
                        // and this id cannot be used anymore.
                        groove.objects_cache.upsert(
                            &std.mem.zeroInit(Object, .{
                                .id = id_tree_value.id,
                            }),
                        );
                    } else {
                        groove.prefetch_from_memory_by_timestamp(
                            id_tree_value.timestamp,
                            .primary_key,
                        );
                    }
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .id = id },
                        .{
                            .level = level,
                            .lookup_by = .primary_key,
                        },
                    );
                },
            }
        }

        /// This function attempts to prefetch a value for the timestamp from the ObjectTree's
        /// table blocks in the grid cache.
        fn prefetch_from_memory_by_timestamp(
            groove: *Groove,
            timestamp: u64,
            lookup_by: KeyType,
        ) void {
            assert(timestamp >= TimestampRange.timestamp_min);
            assert(timestamp <= TimestampRange.timestamp_max);

            switch (groove.objects.lookup_from_levels_cache(
                groove.prefetch_snapshot.?,
                timestamp,
            )) {
                .negative => switch (lookup_by) {
                    .primary_key => {},
                    .timestamp => if (has_id)
                        groove.timestamps.set(timestamp, .not_found)
                    else
                        unreachable,
                },
                .positive => |object| {
                    assert(!ObjectTreeHelper.tombstone(object));
                    switch (lookup_by) {
                        .primary_key => groove.objects_cache.upsert(object),
                        .timestamp => if (has_id) {
                            groove.objects_cache.upsert(object);
                            groove.timestamps.set(object.timestamp, .{ .found = object.id });
                        } else unreachable,
                    }
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .timestamp = timestamp },
                        .{
                            .level = level,
                            .lookup_by = lookup_by,
                        },
                    );
                },
            }
        }
        /// Ensure the objects corresponding to all ids enqueued with prefetch_enqueue() are
        /// available in `objects_cache`.
        pub fn prefetch(
            groove: *Groove,
            callback: *const fn (*PrefetchContext) void,
            context: *PrefetchContext,
        ) void {
            context.* = .{
                .groove = groove,
                .callback = callback,
                .snapshot = groove.prefetch_snapshot.?,
                .key_iterator = groove.prefetch_keys.iterator(),
            };
            groove.prefetch_snapshot = null;
            context.start_workers();
        }

        pub const PrefetchContext = struct {
            groove: *Groove,
            callback: *const fn (*PrefetchContext) void,
            snapshot: u64,

            key_iterator: PrefetchKeys.Iterator,
            /// The goal is to fully utilize the disk I/O to ensure the prefetch completes as
            /// quickly as possible, so we run multiple lookups in parallel based on the max
            /// I/O depth of the Grid.
            workers: [Grid.read_iops_max]PrefetchWorker = undefined,
            /// The number of workers that are currently running in parallel.
            workers_pending: u32 = 0,

            next_tick: Grid.NextTick = undefined,

            fn start_workers(context: *PrefetchContext) void {
                assert(context.workers_pending == 0);

                context.groove.grid.trace.start(.{
                    .lookup = .{ .tree = @enumFromInt(context.groove.objects.config.id) },
                });

                // Track an extra "worker" that will finish after the loop.
                // This allows the callback to be called asynchronously on `next_tick`
                // if all workers are finished synchronously.
                context.workers_pending += 1;

                for (&context.workers, 0..) |*worker, index| {
                    assert(context.workers_pending == index + 1);

                    worker.* = .{
                        .index = @intCast(index),
                        .context = context,
                    };

                    context.groove.grid.trace.start(
                        .{ .lookup_worker = .{
                            .index = worker.index,
                            .tree = @enumFromInt(context.groove.objects.config.id),
                        } },
                    );

                    context.workers_pending += 1;
                    worker.lookup_start_next();

                    // If the worker finished synchronously (e.g `workers_pending` decreased),
                    // we don't need to start new ones.
                    if (context.workers_pending == index + 1) break;
                }

                assert(context.workers_pending > 0);
                context.workers_pending -= 1;

                if (context.workers_pending == 0) {
                    // All workers finished synchronously,
                    // calling the callback on `next_tick`.
                    context.groove.grid.on_next_tick(worker_next_tick, &context.next_tick);
                }
            }

            fn worker_next_tick(completion: *Grid.NextTick) void {
                const context: *PrefetchContext = @alignCast(
                    @fieldParentPtr("next_tick", completion),
                );
                assert(context.workers_pending == 0);
                context.finish();
            }

            fn worker_finished(context: *PrefetchContext) void {
                context.workers_pending -= 1;
                if (context.workers_pending == 0) context.finish();
            }

            fn finish(context: *PrefetchContext) void {
                assert(context.workers_pending == 0);

                assert(context.key_iterator.next() == null);
                context.groove.prefetch_keys.clearRetainingCapacity();
                assert(context.groove.prefetch_keys.count() == 0);

                context.groove.grid.trace.stop(.{
                    .lookup = .{ .tree = @enumFromInt(context.groove.objects.config.id) },
                });

                context.callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            // Since lookup contexts are used one at a time, it's safe to access
            // the union's fields and reuse the same memory for all context instances.
            // Can't use extern/packed union as the LookupContexts aren't ABI compliant.
            const LookupContext = union(enum) {
                id: if (has_id) IdTree.LookupContext else void,
                object: ObjectTree.LookupContext,

                pub const Field = std.meta.FieldEnum(LookupContext);
                pub fn FieldType(comptime field: Field) type {
                    return std.meta.fieldInfo(LookupContext, field).type;
                }

                pub inline fn parent(
                    comptime field: Field,
                    completion: *FieldType(field),
                ) *PrefetchWorker {
                    const lookup: *LookupContext = @fieldParentPtr(@tagName(field), completion);
                    return @fieldParentPtr("lookup", lookup);
                }

                pub inline fn get(self: *LookupContext, comptime field: Field) *FieldType(field) {
                    self.* = @unionInit(LookupContext, @tagName(field), undefined);
                    return &@field(self, @tagName(field));
                }
            };

            index: u8,
            context: *PrefetchContext,
            lookup: LookupContext = undefined,
            current: ?struct {
                key: PrefetchKey,
                lookup_by: KeyType,
            } = null,

            fn lookup_start_next(worker: *PrefetchWorker) void {
                assert(worker.current == null);
                const prefetch_entry = worker.context.key_iterator.next() orelse {
                    worker.context.groove.grid.trace.stop(
                        .{ .lookup_worker = .{
                            .index = worker.index,
                            .tree = @enumFromInt(worker.context.groove.objects.config.id),
                        } },
                    );

                    worker.context.worker_finished();
                    return;
                };

                worker.current = .{
                    .key = prefetch_entry.key_ptr.*,
                    .lookup_by = prefetch_entry.value_ptr.lookup_by,
                };

                // prefetch_enqueue() ensures that the tree's cache is checked before queueing the
                // object for prefetching. If not in the LSM tree's cache, the object must be read
                // from disk and added to the auxiliary prefetch_objects hash map.
                switch (prefetch_entry.key_ptr.*) {
                    .id => |id| if (has_id) {
                        worker.context.groove.ids.lookup_from_levels_storage(.{
                            .callback = lookup_id_callback,
                            .context = worker.lookup.get(.id),
                            .snapshot = worker.context.snapshot,
                            .key = id,
                            .level_min = prefetch_entry.value_ptr.level,
                        });
                    } else unreachable,
                    .timestamp => |timestamp| {
                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup.get(.object),
                            .snapshot = worker.context.snapshot,
                            .key = timestamp,
                            .level_min = prefetch_entry.value_ptr.level,
                        });
                    },
                }
            }

            fn lookup_id_callback(
                completion: *IdTree.LookupContext,
                result: ?*const IdTreeValue,
            ) void {
                const worker = LookupContext.parent(.id, completion);
                worker.lookup = undefined;
                assert(worker.current != null);
                assert(worker.current.?.key == .id);
                assert(worker.current.?.lookup_by == .primary_key);

                if (result) |id_tree_value| {
                    if (groove_options.orphaned_ids and
                        id_tree_value.timestamp == 0)
                    {
                        comptime assert(has_id);

                        // Zeroed timestamp indicates the object is not present,
                        // and this id cannot be used anymore.
                        worker.context.groove.objects_cache.upsert(
                            &std.mem.zeroInit(Object, .{
                                .id = id_tree_value.id,
                            }),
                        );
                    } else if (!id_tree_value.tombstone()) {
                        worker.lookup_by_timestamp(id_tree_value.timestamp);
                        return;
                    }
                }

                worker.current = null;
                worker.lookup_start_next();
            }

            fn lookup_by_timestamp(worker: *PrefetchWorker, timestamp: u64) void {
                assert(timestamp >= TimestampRange.timestamp_min);
                assert(timestamp <= TimestampRange.timestamp_max);
                assert(worker.current != null);

                switch (worker.context.groove.objects.lookup_from_levels_cache(
                    worker.context.snapshot,
                    timestamp,
                )) {
                    .negative => {
                        lookup_object_callback(worker.lookup.get(.object), null);
                    },
                    .positive => |value| {
                        lookup_object_callback(worker.lookup.get(.object), value);
                    },
                    .possible => |level_min| {
                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup.get(.object),
                            .snapshot = worker.context.snapshot,
                            .key = timestamp,
                            .level_min = level_min,
                        });
                    },
                }
            }

            fn lookup_object_callback(
                completion: *ObjectTree.LookupContext,
                result: ?*const Object,
            ) void {
                const worker = LookupContext.parent(.object, completion);
                worker.lookup = undefined;

                assert(worker.current != null);
                const entry = worker.current.?;
                worker.current = null;

                if (result) |object| {
                    assert(!ObjectTreeHelper.tombstone(object));
                    switch (entry.key) {
                        .id => |key| {
                            assert((if (has_id) object.id else object.timestamp) == key);
                            assert(entry.lookup_by == .primary_key);
                        },
                        .timestamp => |timestamp| {
                            assert(object.timestamp == timestamp);
                            assert(entry.lookup_by == .primary_key or
                                entry.lookup_by == .timestamp);
                        },
                    }

                    switch (entry.lookup_by) {
                        .primary_key => worker.context.groove.objects_cache.upsert(object),
                        .timestamp => if (has_id) {
                            worker.context.groove.objects_cache.upsert(object);
                            worker.context.groove.timestamps.set(
                                object.timestamp,
                                .{ .found = object.id },
                            );
                        } else unreachable,
                    }
                } else switch (entry.lookup_by) {
                    // If the object wasn't found, it should've been prefetched by timestamp,
                    // or handled by `lookup_id_callback`.
                    .primary_key => assert(!has_id),
                    .timestamp => if (has_id) worker.context.groove.timestamps.set(
                        entry.key.timestamp,
                        .not_found,
                    ) else unreachable,
                }

                worker.lookup_start_next();
            }
        };

        /// Insert the value into the objects tree and associated index trees. It's up to the
        /// caller to ensure it doesn't already exist.
        pub fn insert(groove: *Groove, object: *const Object) void {
            assert(object.timestamp >= TimestampRange.timestamp_min);
            assert(object.timestamp <= TimestampRange.timestamp_max);

            if (ObjectsCache != void) {
                assert(!groove.objects_cache.has(@field(object, primary_field)));
                groove.objects_cache.upsert(object);
            }

            if (has_id) {
                groove.ids.put(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
                groove.ids.key_range_update(object.id);
            }
            groove.objects.put(object);
            groove.objects.key_range_update(object.timestamp);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                if (Helper.index_from_object(object)) |value| {
                    @field(groove.indexes, field.name).put(&.{
                        .timestamp = object.timestamp,
                        .field = value,
                    });
                }
            }
        }

        /// Update the value. Requires the old object to be provided.
        /// Update the object and index trees by diff'ing the old and new values.
        pub fn update(
            groove: *Groove,
            values: struct { old: *const Object, new: *const Object },
        ) void {
            const old = values.old;
            const new = values.new;

            if (ObjectsCache != void) {
                const old_from_cache = groove.objects_cache.get(@field(old, primary_field)).?;
                assert(stdx.equal_bytes(Object, old_from_cache, old));
            }

            // Sanity check to ensure the caller didn't accidentally pass in an alias.
            assert(new != old);

            if (has_id) assert(old.id == new.id);
            assert(old.timestamp == new.timestamp);
            assert(new.timestamp >= TimestampRange.timestamp_min);
            assert(new.timestamp <= TimestampRange.timestamp_max);

            // The ID can't change, so no need to update the ID tree. Update the object tree entry
            // if any of the fields (even ignored) are different. We assume the caller will pass in
            // an object that has changes.
            // Unlike the index trees, the new and old values in the object tree share the same
            // key. Therefore put() is sufficient to overwrite the old value.
            if (constants.verify) {
                const tombstone = ObjectTreeHelper.tombstone;
                const key_from_value = ObjectTreeHelper.key_from_value;

                assert(!stdx.equal_bytes(Object, old, new));
                assert(key_from_value(old) == key_from_value(new));
                assert(!tombstone(old) and !tombstone(new));
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                const old_index = Helper.index_from_object(old);
                const new_index = Helper.index_from_object(new);

                // Only update the indexes that change.
                if (old_index != new_index) {
                    if (old_index) |value| {
                        @field(groove.indexes, field.name).remove(&.{
                            .timestamp = old.timestamp,
                            .field = value,
                        });
                    }
                    if (new_index) |value| {
                        @field(groove.indexes, field.name).put(&.{
                            .timestamp = new.timestamp,
                            .field = value,
                        });
                    }
                }
            }

            // Putting the objects_cache upsert after the index tree updates is critical:
            // We diff the old and new objects, but the old object will be a pointer into the
            // objects_cache. If we upsert first, there's a high chance old.* == new.* (always,
            // unless old comes from the stash) and no secondary indexes will be updated!

            if (ObjectsCache != void) {
                groove.objects_cache.upsert(new);
            }
            groove.objects.put(new);
        }

        /// Asserts that the object with the given PrimaryKey exists.
        pub fn remove(groove: *Groove, key: PrimaryKey) void {
            // TODO: Nothing currently calls or tests this method. The forest fuzzer should be
            // extended to cover it.
            assert(false);

            const object = groove.objects_cache.get(key).?;
            assert(object.timestamp >= TimestampRange.timestamp_min);
            assert(object.timestamp <= TimestampRange.timestamp_max);

            // TODO: should update the timestamp and id range, see `key_range_update`.
            groove.objects.remove(object);
            if (has_id) {
                groove.ids.remove(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
            }

            groove.objects_cache.remove(key);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                if (Helper.index_from_object(object)) |value| {
                    @field(groove.indexes, field.name).remove(&.{
                        .timestamp = object.timestamp,
                        .field = value,
                    });
                }
            }
        }

        /// Insert an id associated with no object.
        /// It's up to the caller to ensure it doesn't already exist.
        pub fn insert_orphaned_id(groove: *Groove, id: u128) void {
            comptime assert(groove_options.orphaned_ids);
            comptime assert(has_id);

            assert(id != 0);
            assert(id != std.math.maxInt(u128));

            // We should not insert an orphaned `id` inside a scope.
            assert(!groove.objects_cache.scope_is_active);
            assert(groove.ids.active_scope == null);
            assert(!groove.objects_cache.has(id));

            groove.objects_cache.upsert(&std.mem.zeroInit(Object, .{ .id = id }));
            groove.ids.put(&.{ .id = id, .timestamp = 0 });
            groove.ids.key_range_update(id);
        }

        pub fn remove_orphaned_id(groove: *Groove, id: u128) void {
            comptime assert(groove_options.orphaned_ids);
            comptime assert(has_id);

            // TODO: Nothing currently calls or tests this method. The forest fuzzer should be
            // extended to cover it.
            assert(false);

            _ = groove;
            _ = id;
        }

        pub fn scope_open(groove: *Groove) void {
            if (ObjectsCache != void) groove.objects_cache.scope_open();

            if (has_id) {
                groove.ids.scope_open();
            }
            groove.objects.scope_open();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_open();
            }
        }

        pub fn scope_close(groove: *Groove, mode: ScopeCloseMode) void {
            if (ObjectsCache != void) groove.objects_cache.scope_close(mode);

            if (has_id) {
                groove.ids.scope_close(mode);
            }
            groove.objects.scope_close(mode);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_close(mode);
            }
        }

        pub fn compact(groove: *Groove, op: u64) void {
            if (has_id) groove.ids.compact();
            groove.objects.compact();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact();
            }

            // Compact the objects_cache on the last beat of the bar, just like the trees do to
            // their mutable tables.
            if (ObjectsCache != void) {
                const compaction_beat = op % constants.lsm_compaction_ops;
                if (compaction_beat == constants.lsm_compaction_ops - 1) {
                    groove.objects_cache.compact();
                }
            }
        }

        pub fn open_commence(groove: *Groove, manifest_log: *ManifestLog) void {
            if (has_id) groove.ids.open_commence(manifest_log);
            groove.objects.open_commence(manifest_log);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_commence(manifest_log);
            }
        }

        pub fn open_complete(groove: *Groove) void {
            if (has_id) groove.ids.open_complete();
            groove.objects.open_complete();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_complete();
            }
        }

        pub fn assert_between_bars(groove: *const Groove) void {
            if (has_id) groove.ids.assert_between_bars();
            groove.objects.assert_between_bars();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).assert_between_bars();
            }
        }
    };
}
