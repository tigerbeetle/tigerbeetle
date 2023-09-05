const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("../vsr/grid.zig").GridType;
const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const Fingerprint = @import("bloom_filter.zig").Fingerprint;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;

const snapshot_latest = @import("tree.zig").snapshot_latest;
const key_fingerprint = @import("tree.zig").key_fingerprint;

fn ObjectTreeHelpers(comptime Object: type) type {
    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    return struct {
        inline fn compare_keys(timestamp_a: u64, timestamp_b: u64) std.math.Order {
            return std.math.order(timestamp_a, timestamp_b);
        }

        inline fn key_from_value(value: *const Object) u64 {
            return value.timestamp & ~@as(u64, tombstone_bit);
        }

        const sentinel_key = std.math.maxInt(u64);
        const tombstone_bit = 1 << (64 - 1);

        inline fn tombstone(value: *const Object) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        inline fn tombstone_from_key(timestamp: u64) Object {
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

    inline fn compare_keys(a: u128, b: u128) std.math.Order {
        return std.math.order(a, b);
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
    comptime value_count_max: usize,
) type {
    const Key = CompositeKey(IndexCompositeKeyType(Field));
    const Table = TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
        value_count_max,
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
    /// - value_count_max: { .field = usize }:
    ///     An anonymous struct which contains, for each field of `Object`,
    ///     the maximum number of values per table for the corresponding index tree.
    ///
    /// - ignored: [][]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - derived: { .field = *const fn (*const Object) ?DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    comptime groove_options: anytype,
) type {
    @setEvalBranchQuota(64000);

    const has_id = @hasField(Object, "id");
    if (has_id) assert(std.meta.fieldInfo(Object, .id).type == u128);

    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    comptime var index_fields: []const std.builtin.Type.StructField = &.{};

    // Generate index LSM trees from the struct fields.
    for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        //
        // By default, we ignore the "timestamp" field since it's a special identifier.
        // Since the "timestamp" is ignored by default, it shouldn't be provided in groove_options.ignored.
        comptime var ignored = mem.eql(u8, field.name, "timestamp") or mem.eql(u8, field.name, "id");
        for (groove_options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            comptime assert(!std.mem.eql(u8, ignored_field_name, "id"));
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }

        if (!ignored) {
            const IndexTree = IndexTreeType(
                Storage,
                field.type,
                @field(groove_options.value_count_max, field.name),
            );
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
        if (derive_func_info.args.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Make sure the function takes in a reference to the Value:
        const derive_arg = derive_func_info.params[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.type != *const Object) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Get the return value from the derived field as the DerivedType.
        const derive_return_type = derive_func_info.return_type orelse {
            @compileError("expected derive fn to return valid tree index type");
        };

        // Create an IndexTree for the DerivedType:
        const tree_name = @typeName(Object) ++ "." ++ field.name;
        const DerivedType = @typeInfo(derive_return_type).Optional.child;
        const IndexTree = IndexTreeType(Storage, DerivedType, tree_name);

        index_fields = index_fields ++ &.{
            .{
                .name = field.name,
                .type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    comptime var index_options_fields: []const std.builtin.Type.StructField = &.{};
    for (index_fields) |index_field| {
        const IndexTree = index_field.type;
        index_options_fields = index_options_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = index_field.name,
                .type = IndexTree.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree.Options),
            },
        };
    }

    const _ObjectTree = blk: {
        const Table = TableType(
            u64, // key = timestamp
            Object,
            ObjectTreeHelpers(Object).compare_keys,
            ObjectTreeHelpers(Object).key_from_value,
            ObjectTreeHelpers(Object).sentinel_key,
            ObjectTreeHelpers(Object).tombstone,
            ObjectTreeHelpers(Object).tombstone_from_key,
            groove_options.value_count_max.timestamp,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IdTree = if (!has_id) void else blk: {
        const Table = TableType(
            u128,
            IdTreeValue,
            IdTreeValue.compare_keys,
            IdTreeValue.key_from_value,
            IdTreeValue.sentinel_key,
            IdTreeValue.tombstone,
            IdTreeValue.tombstone_from_key,
            groove_options.value_count_max.id,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IndexTrees = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });
    const IndexTreeOptions = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(_IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len -
        groove_options.ignored.len -
        // The id/timestamp fields are implicitly ignored since it's the primary key for ObjectTree:
        (@as(usize, 1) + @intFromBool(has_id)) +
        std.meta.fields(@TypeOf(groove_options.derived)).len;

    assert(indexes_count_actual == indexes_count_expect);
    assert(indexes_count_actual == std.meta.fields(IndexTreeOptions).len);

    // Generate a helper function for interacting with an Index field type.
    const IndexTreeFieldHelperType = struct {
        /// Returns true if the field is a derived field.
        fn is_derived(comptime field_name: []const u8) bool {
            comptime var derived = false;
            inline for (derived_fields) |derived_field| {
                derived = derived or std.mem.eql(u8, derived_field.name, field_name);
            }
            return derived;
        }

        /// Gets the index type from the index name (even if the index is derived).
        fn IndexType(comptime field_name: []const u8) type {
            if (!is_derived(field_name)) {
                return @TypeOf(@field(@as(Object, undefined), field_name));
            }

            const derived_fn = @TypeOf(@field(groove_options.derived, field_name));
            return @typeInfo(derived_fn).Fn.return_type.?.Optional.child;
        }

        fn HelperType(comptime field_name: []const u8) type {
            return struct {
                const Index = IndexType(field_name);

                /// Try to extract an index from the object, deriving it when necessary.
                pub fn derive_index(object: *const Object) ?Index {
                    if (comptime is_derived(field_name)) {
                        return @field(groove_options.derived, field_name)(object);
                    } else {
                        return @field(object, field_name);
                    }
                }

                /// Create a Value from the index that can be used in the IndexTree.
                pub fn derive_value(
                    object: *const Object,
                    index: Index,
                ) CompositeKey(IndexCompositeKeyType(Index)).Value {
                    return .{
                        .timestamp = object.timestamp,
                        .field = switch (@typeInfo(Index)) {
                            .Int => index,
                            .Enum => @intFromEnum(index),
                            else => @compileError("Unsupported index type for " ++ field_name),
                        },
                    };
                }
            };
        }
    }.HelperType;

    return struct {
        const Groove = @This();

        pub const ObjectTree = _ObjectTree;
        pub const IdTree = _IdTree;
        pub const IndexTrees = _IndexTrees;
        pub const config = groove_options;

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Groove) void;

        const trees_total = @as(usize, 1) + @intFromBool(has_id) + std.meta.fields(IndexTrees).len;
        const TreesBitSet = std.StaticBitSet(trees_total);

        const primary_field = if (has_id) "id" else "timestamp";
        const PrimaryKey = @TypeOf(@field(@as(Object, undefined), primary_field));

        const PrefetchKeys = std.AutoArrayHashMapUnmanaged(
            union(enum) {
                id: PrimaryKey,
                timestamp: u64,
            },
            struct {
                fingerprint: Fingerprint,
                level: u8,
            },
        );

        const PrefetchObjectsContext = struct {
            pub inline fn hash(_: PrefetchObjectsContext, object: Object) u64 {
                return stdx.hash_inline(@field(object, primary_field));
            }

            pub inline fn eql(_: PrefetchObjectsContext, a: Object, b: Object) bool {
                return @field(a, primary_field) == @field(b, primary_field);
            }
        };
        const PrefetchObjectsAdapter = struct {
            pub inline fn hash(_: PrefetchObjectsAdapter, key: PrimaryKey) u64 {
                return stdx.hash_inline(key);
            }

            pub inline fn eql(_: PrefetchObjectsAdapter, a_key: PrimaryKey, b_object: Object) bool {
                return a_key == @field(b_object, primary_field);
            }
        };
        const PrefetchObjects = std.HashMapUnmanaged(Object, void, PrefetchObjectsContext, 70);

        compacting: ?struct {
            /// Count which tree compactions are in progress.
            pending: TreesBitSet = TreesBitSet.initFull(),
            callback: Callback,
        } = null,

        objects: ObjectTree,
        ids: IdTree,
        indexes: IndexTrees,

        /// Object IDs enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_keys: PrefetchKeys,

        /// The prefetched Objects. This hash map holds the subset of objects in the LSM trees
        /// that are required for the current commit. All get()/put()/remove() operations during
        /// the commit are both passed to the LSM trees and mirrored in this hash map. It is always
        /// sufficient to query this hashmap alone to know the state of the LSM trees.
        prefetch_objects: PrefetchObjects,

        /// The snapshot to prefetch from.
        prefetch_snapshot: ?u64,

        pub const Options = struct {
            /// The maximum number of objects that might be prefetched by a batch.
            prefetch_entries_max: u32,

            tree_options_object: ObjectTree.Options,
            tree_options_id: if (has_id) IdTree.Options else void,
            tree_options_index: IndexTreeOptions,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            options: Options,
        ) !Groove {
            var object_tree = try ObjectTree.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "timestamp"),
                    .name = @typeName(Object),
                },
                options.tree_options_object,
            );
            errdefer object_tree.deinit(allocator);

            var id_tree = if (!has_id) {} else (try IdTree.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "id"),
                    .name = @typeName(Object) ++ ".id",
                },
                options.tree_options_id,
            ));
            errdefer if (has_id) id_tree.deinit(allocator);

            var index_trees_initialized: usize = 0;
            var index_trees: IndexTrees = undefined;

            // Make sure to deinit initialized index LSM trees on error.
            errdefer inline for (std.meta.fields(IndexTrees), 0..) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    @field(index_trees, field.name).deinit(allocator);
                }
            };

            // Initialize index LSM trees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                // No value cache for index trees, since they only do range queries.
                assert(@field(options.tree_options_index, field.name).cache_entries_max == 0);

                @field(index_trees, field.name) = try field.type.init(
                    allocator,
                    node_pool,
                    grid,
                    .{
                        .id = @field(groove_options.ids, field.name),
                        .name = @typeName(Object) ++ "." ++ field.name,
                    },
                    @field(options.tree_options_index, field.name),
                );
                index_trees_initialized += 1;
            }

            var prefetch_keys = PrefetchKeys{};
            try prefetch_keys.ensureTotalCapacity(allocator, options.prefetch_entries_max);
            errdefer prefetch_keys.deinit(allocator);

            var prefetch_objects = PrefetchObjects{};
            try prefetch_objects.ensureTotalCapacity(allocator, options.prefetch_entries_max);
            errdefer prefetch_objects.deinit(allocator);

            return Groove{
                .objects = object_tree,
                .ids = id_tree,
                .indexes = index_trees,

                .prefetch_keys = prefetch_keys,
                .prefetch_objects = prefetch_objects,
                .prefetch_snapshot = null,
            };
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);
            if (has_id) groove.ids.deinit(allocator);

            groove.prefetch_keys.deinit(allocator);
            groove.prefetch_objects.deinit(allocator);

            groove.* = undefined;
        }

        pub fn reset(groove: *Groove) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).reset();
            }
            groove.objects.reset();
            if (has_id) groove.ids.reset();

            groove.prefetch_keys.clearRetainingCapacity();
            groove.prefetch_objects.clearRetainingCapacity();

            groove.* = .{
                .objects = groove.objects,
                .ids = groove.ids,
                .indexes = groove.indexes,
                .prefetch_keys = groove.prefetch_keys,
                .prefetch_objects = groove.prefetch_objects,
                .prefetch_snapshot = null,
            };
        }

        pub fn get(groove: *const Groove, key: PrimaryKey) ?*const Object {
            return groove.prefetch_objects.getKeyPtrAdapted(key, PrefetchObjectsAdapter{});
        }

        /// Must be called directly before the state machine begins queuing ids for prefetch.
        /// When `snapshot` is null, prefetch from the current snapshot.
        pub fn prefetch_setup(groove: *Groove, snapshot: ?u64) void {
            const snapshot_target = snapshot orelse snapshot_latest;
            assert(snapshot_target <= snapshot_latest);

            if (groove.prefetch_snapshot == null) {
                groove.prefetch_objects.clearRetainingCapacity();
            } else {
                // If there is a snapshot already set from the previous prefetch_setup(), then its
                // prefetch() was never called, so there must already be no queued objects or ids.
            }

            groove.prefetch_snapshot = snapshot_target;
            assert(groove.prefetch_objects.count() == 0);
            assert(groove.prefetch_keys.count() == 0);
        }

        /// This must be called by the state machine for every key to be prefetched.
        /// We tolerate duplicate IDs enqueued by the state machine.
        /// For example, if all unique operations require the same two dependencies.
        pub fn prefetch_enqueue(
            groove: *Groove,
            key: PrimaryKey,
        ) void {
            if (has_id) {
                if (!groove.ids.key_range_contains(groove.prefetch_snapshot.?, key)) return;
                groove.prefetch_from_memory_by_id(key);
            } else {
                groove.prefetch_from_memory_by_timestamp(key);
            }
        }

        /// This function attempts to prefetch a value for the given id from the IdTree's
        /// mutable table, immutable table, and the table blocks in the grid cache.
        /// If found in the IdTree, we attempt to prefetch a value for the timestamp.
        /// TODO: We may have to remove this function once Fed's prefetching changes are merged,
        /// since those changes remove lookup_from_memory.
        fn prefetch_from_memory_by_id(groove: *Groove, id: PrimaryKey) void {
            const fingerprint = key_fingerprint(id);
            switch (groove.ids.lookup_from_memory(groove.prefetch_snapshot.?, id, fingerprint)) {
                .negative => {},
                .positive => |id_tree_value| {
                    if (IdTreeValue.tombstone(id_tree_value)) return;
                    groove.prefetch_from_memory_by_timestamp(id_tree_value.timestamp);
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .id = id },
                        .{
                            .level = level,
                            .fingerprint = fingerprint,
                        },
                    );
                },
            }
        }

        /// This function attempts to prefetch a value for the timestamp from the ObjectTree's
        /// mutable table, immutable table, and the table blocks in the grid cache.
        /// TODO: We may have to remove this function once Fed's prefetching changes are merged,
        /// since those changes remove lookup_from_memory.
        fn prefetch_from_memory_by_timestamp(groove: *Groove, timestamp: u64) void {
            const fingerprint = key_fingerprint(timestamp);
            switch (groove.objects.lookup_from_memory(
                groove.prefetch_snapshot.?,
                timestamp,
                fingerprint,
            )) {
                .negative => {},
                .positive => |object| {
                    assert(!ObjectTreeHelpers(Object).tombstone(object));
                    groove.prefetch_objects.putAssumeCapacity(object.*, {});
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .timestamp = timestamp },
                        .{
                            .fingerprint = fingerprint,
                            .level = level,
                        },
                    );
                },
            }
        }

        /// Ensure the objects corresponding to all ids enqueued with prefetch_enqueue() are
        /// available in `prefetch_objects`.
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
            workers_busy: u32 = 0,

            fn start_workers(context: *PrefetchContext) void {
                assert(context.workers_busy == 0);

                // Track an extra "worker" that will finish after the loop.
                //
                // This prevents `context.finish()` from being called within the loop body when
                // every worker finishes synchronously. `context.finish()` calls the user-provided
                // callback which may re-use the memory of this `PrefetchContext`. However, we
                // rely on `context` being well-defined for the loop condition.
                context.workers_busy += 1;

                for (&context.workers) |*worker| {
                    worker.* = .{ .context = context };
                    context.workers_busy += 1;
                    worker.lookup_start_next();
                }

                assert(context.workers_busy >= 1);
                context.worker_finished();
            }

            fn worker_finished(context: *PrefetchContext) void {
                context.workers_busy -= 1;
                if (context.workers_busy == 0) context.finish();
            }

            fn finish(context: *PrefetchContext) void {
                assert(context.workers_busy == 0);

                assert(context.key_iterator.next() == null);
                context.groove.prefetch_keys.clearRetainingCapacity();
                assert(context.groove.prefetch_keys.count() == 0);

                context.callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            // Since lookup contexts are used one at a time, it's safe to access
            // the union's fields and reuse the same memory for all context instances.
            // Can't use extern/packed union as the LookupContextes aren't ABI compliant.
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
                    const lookup = @fieldParentPtr(LookupContext, @tagName(field), completion);
                    return @fieldParentPtr(PrefetchWorker, "lookup", lookup);
                }

                pub inline fn get(self: *LookupContext, comptime field: Field) *FieldType(field) {
                    self.* = @unionInit(LookupContext, @tagName(field), undefined);
                    return &@field(self, @tagName(field));
                }
            };

            context: *PrefetchContext,
            lookup: LookupContext = undefined,

            fn lookup_start_next(worker: *PrefetchWorker) void {
                const prefetch_entry = worker.context.key_iterator.next() orelse {
                    worker.context.worker_finished();
                    return;
                };

                // prefetch_enqueue() ensures that the tree's cache is checked before queueing the
                // object for prefetching. If not in the LSM tree's cache, the object must be read
                // from disk and added to the auxiliary prefetch_objects hash map.
                switch (prefetch_entry.key_ptr.*) {
                    .id => |id| {
                        if (constants.verify) {
                            assert(std.meta.eql(
                                prefetch_entry.value_ptr.fingerprint,
                                key_fingerprint(id),
                            ));
                        }

                        if (has_id) {
                            worker.context.groove.ids.lookup_from_levels_storage(.{
                                .callback = lookup_id_callback,
                                .context = worker.lookup.get(.id),
                                .snapshot = worker.context.snapshot,
                                .key = id,
                                .fingerprint = prefetch_entry.value_ptr.fingerprint,
                                .level_min = prefetch_entry.value_ptr.level,
                            });
                        } else unreachable;
                    },
                    .timestamp => |timestamp| {
                        if (constants.verify) {
                            assert(std.meta.eql(
                                prefetch_entry.value_ptr.fingerprint,
                                key_fingerprint(timestamp),
                            ));
                        }

                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup.get(.object),
                            .snapshot = worker.context.snapshot,
                            .key = timestamp,
                            .fingerprint = prefetch_entry.value_ptr.fingerprint,
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

                if (result) |id_tree_value| {
                    if (!id_tree_value.tombstone()) {
                        worker.lookup_by_timestamp(id_tree_value.timestamp);
                        return;
                    }
                }

                worker.lookup_start_next();
            }

            fn lookup_by_timestamp(worker: *PrefetchWorker, timestamp: u64) void {
                const fingerprint = key_fingerprint(timestamp);
                switch (worker.context.groove.objects.lookup_from_memory(
                    worker.context.snapshot,
                    timestamp,
                    fingerprint,
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
                            .fingerprint = fingerprint,
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

                if (result) |object| {
                    assert(!ObjectTreeHelpers(Object).tombstone(object));
                    worker.context.groove.prefetch_objects.putAssumeCapacityNoClobber(object.*, {});
                } else {
                    // When there is an id tree, the result must be non-null,
                    // as we keep the ID and Object trees in sync.
                    assert(!has_id);
                }

                worker.lookup_start_next();
            }
        };

        pub fn put_no_clobber(groove: *Groove, object: *const Object) void {
            const gop = groove.prefetch_objects.getOrPutAssumeCapacityAdapted(
                @field(object, primary_field),
                PrefetchObjectsAdapter{},
            );
            assert(!gop.found_existing);
            groove.insert(object);
            gop.key_ptr.* = object.*;
        }

        pub fn put(groove: *Groove, object: *const Object) void {
            const gop = groove.prefetch_objects.getOrPutAssumeCapacityAdapted(
                @field(object, primary_field),
                PrefetchObjectsAdapter{},
            );

            if (gop.found_existing) {
                groove.update(gop.key_ptr, object);
            } else {
                groove.insert(object);
            }
            gop.key_ptr.* = object.*;
        }

        /// Insert the value into the objects tree and its fields into the index trees.
        fn insert(groove: *Groove, object: *const Object) void {
            groove.objects.put(object);
            if (has_id) {
                groove.ids.put(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
                groove.ids.key_range_update(object.id);
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive_index(object)) |index| {
                    const index_value = Helper.derive_value(object, index);
                    @field(groove.indexes, field.name).put(&index_value);
                }
            }
        }

        /// Update the object and index trees by diff'ing the old and new values.
        fn update(groove: *Groove, old: *const Object, new: *const Object) void {
            assert(@field(old, primary_field) == @field(new, primary_field));
            assert(old.timestamp == new.timestamp);

            // Update the object tree entry if any of the fields (even ignored) are different.
            if (!std.mem.eql(u8, std.mem.asBytes(old), std.mem.asBytes(new))) {
                // Unlike the index trees, the new and old values in the object tree share the
                // same key. Therefore put() is sufficient to overwrite the old value.
                groove.objects.put(new);
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                const old_index = Helper.derive_index(old);
                const new_index = Helper.derive_index(new);

                // Only update the indexes that change.
                if (!std.meta.eql(old_index, new_index)) {
                    if (old_index) |index| {
                        const old_index_value = Helper.derive_value(old, index);
                        @field(groove.indexes, field.name).remove(&old_index_value);
                    }

                    if (new_index) |index| {
                        const new_index_value = Helper.derive_value(new, index);
                        @field(groove.indexes, field.name).put(&new_index_value);
                    }
                }
            }
        }

        /// Asserts that the object with the given PrimaryKey exists.
        pub fn remove(groove: *Groove, key: PrimaryKey) void {
            const object = groove.prefetch_objects.getKeyPtrAdapted(key, PrefetchObjectsAdapter{}).?;

            groove.objects.remove(object);
            if (has_id) {
                groove.ids.remove(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive_index(object)) |index| {
                    const index_value = Helper.derive_value(object, index);
                    @field(groove.indexes, field.name).remove(&index_value);
                }
            }

            groove.prefetch_objects.removeByPtr(object);
        }

        pub fn open_commence(groove: *Groove, manifest_log: *ManifestLog) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.open_commence(manifest_log);
            groove.objects.open_commence(manifest_log);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_commence(manifest_log);
            }
        }

        pub fn open_complete(groove: *Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.open_complete();
            groove.objects.open_complete();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_complete();
            }
        }

        pub fn compact(groove: *Groove, callback: Callback, op: u64) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.compact(compact_tree_callback(.ids), op);
            groove.objects.compact(compact_tree_callback(.objects), op);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const compact_tree_callback_ = compact_tree_callback(.{ .index = field.name });
                @field(groove.indexes, field.name).compact(compact_tree_callback_, op);
            }

            groove.compacting = .{ .callback = callback };
        }

        fn compact_tree_callback(
            comptime tree_field: TreeField,
        ) *const fn (*TreeFor(tree_field)) void {
            return struct {
                fn tree_callback(tree: *TreeFor(tree_field)) void {
                    // Derive the groove pointer from the tree using the tree_field.
                    const groove = switch (tree_field) {
                        .ids => @fieldParentPtr(Groove, "ids", tree),
                        .objects => @fieldParentPtr(Groove, "objects", tree),
                        .index => |field| blk: {
                            const indexes: *align(@alignOf(IndexTrees)) IndexTrees =
                                @alignCast(@fieldParentPtr(IndexTrees, field, tree));
                            break :blk @fieldParentPtr(Groove, "indexes", indexes);
                        },
                    };

                    assert(groove.compacting.?.pending.isSet(tree_field.offset()));
                    groove.compacting.?.pending.unset(tree_field.offset());

                    groove.compact_callback();
                }
            }.tree_callback;
        }

        fn compact_callback(groove: *Groove) void {
            assert(groove.compacting != null);

            // Guard until all pending sync ops complete.
            if (groove.compacting.?.pending.count() > 0) return;

            const callback = groove.compacting.?.callback;
            groove.compacting = null;
            callback(groove);
        }

        const TreeField = union(enum) {
            ids,
            objects,
            index: []const u8,

            fn offset(field: TreeField) usize {
                switch (field) {
                    .objects => return 0,
                    .ids => return 1,
                    .index => |index_tree_name| {
                        inline for (std.meta.fields(IndexTrees), 0..) |index_tree_field, i| {
                            if (std.mem.eql(u8, index_tree_field.name, index_tree_name)) {
                                return @as(usize, 1) + @intFromBool(has_id) + i;
                            }
                        } else unreachable;
                    },
                }
            }
        };

        /// Returns LSM tree type for the given index field name (or ObjectTree if null).
        fn TreeFor(comptime tree_field: TreeField) type {
            return switch (tree_field) {
                .ids => IdTree,
                .objects => ObjectTree,
                .index => |field| @TypeOf(@field(@as(IndexTrees, undefined), field)),
            };
        }

        pub fn compact_end(groove: *Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.compact_end();
            groove.objects.compact_end();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_end();
            }
        }

        pub fn assert_between_bars(groove: *const Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.assert_between_bars();
            groove.objects.assert_between_bars();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).assert_between_bars();
            }
        }
    };
}

test "Groove" {
    const Transfer = @import("../tigerbeetle.zig").Transfer;
    const Storage = @import("../storage.zig").Storage;

    const Groove = GrooveType(
        Storage,
        Transfer,
        .{
            .ids = .{
                .timestamp = 1,
                .id = 2,
                .debit_account_id = 3,
                .credit_account_id = 4,
                .pending_id = 5,
                .timeout = 6,
                .ledger = 7,
                .code = 8,
                .amount = 9,
            },
            // Doesn't matter for this test.
            .value_count_max = .{
                .timestamp = 1,
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 1,
                .user_data = 1,
                .pending_id = 1,
                .timeout = 1,
                .ledger = 1,
                .code = 1,
                .amount = 1,
            },
            .ignored = [_][]const u8{ "reserved", "user_data", "flags" },
            .derived = .{},
        },
    );

    std.testing.refAllDecls(Groove);
    std.testing.refAllDecls(Groove.PrefetchWorker);
    std.testing.refAllDecls(Groove.PrefetchContext);
}
