const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("grid.zig").GridType;
const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

/// Creates an LSM tree type for the Groove's Object tree (where Value = Account or Transfer).
fn ObjectTreeType(comptime Storage: type, comptime Value: type) type {
    if (!@hasField(Value, "timestamp")) {
        @compileError(@typeName(Value) ++ " must have timestamp field as the key");
    }

    if (@TypeOf(@as(Value, undefined).timestamp) != u64) {
        @compileError(@typeName(Value) ++ " timestamp field must be u64");
    }

    const ValueKeyHelpers = struct {
        fn compare_keys(timestamp_a: u64, timestamp_b: u64) callconv(.Inline) std.math.Order {
            return std.math.order(timestamp_a, timestamp_b);
        }

        fn key_from_value(value: *const Value) callconv(.Inline) u64 {
            return value.timestamp;
        }

        const sentinel_key = std.math.maxInt(u64);
        const tombstone_bit = 1 << (64 - 1);

        fn tombstone(value: *const Value) callconv(.Inline) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        fn tombstone_from_key(timestamp: u64) callconv(.Inline) Value {
            var value = std.mem.zeroes(Value); // Full zero-initialized Value.
            value.timestamp = timestamp | tombstone_bit;
            return value;
        }
    };

    const Table = TableType(
        u64, // key = timestamp
        Value,
        ValueKeyHelpers.compare_keys,
        ValueKeyHelpers.key_from_value,
        ValueKeyHelpers.sentinel_key,
        ValueKeyHelpers.tombstone,
        ValueKeyHelpers.tombstone_from_key,
    );

    const tree_name = @typeName(Value);
    return TreeType(Table, Storage, tree_name);
}

/// Normalizes index tree field types into either u64 or u128 for CompositeKey
fn IndexCompositeKeyType(comptime Field: type) type {
    switch (@typeInfo(Field)) {
        .Enum => |e| {
            return switch (@bitSizeOf(e.tag_type)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u64)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported enum tag for index: " ++ @typeName(e.tag_type)),
            };
        },
        .Int => |i| {
            if (i.signedness != .unsigned) {
                @compileError("Index int type (" ++ @typeName(Field) ++ ") is not unsigned");
            }
            return switch (@bitSizeOf(Field)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u64)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported int type for index: " ++ @typeName(Field)),
            };
        },
        else => @compileError("Index type " ++ @typeName(Field) ++ " is not supported"),
    }
}

comptime {
    assert(IndexCompositeKeyType(u1) == u64);
    assert(IndexCompositeKeyType(u16) == u64);
    assert(IndexCompositeKeyType(enum(u16){x}) == u64);

    assert(IndexCompositeKeyType(u32) == u64);
    assert(IndexCompositeKeyType(u63) == u64);
    assert(IndexCompositeKeyType(u64) == u64);
    
    assert(IndexCompositeKeyType(enum(u65){x}) == u128);
    assert(IndexCompositeKeyType(u65) == u128);
    assert(IndexCompositeKeyType(u128) == u128);
}

fn IndexTreeType(
    comptime Storage: type, 
    comptime Field: type, 
    comptime tree_name: []const u8,
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
    );

    return TreeType(Table, Storage, tree_name);
}

/// A Groove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GrooveType(
    comptime Storage: type,
    comptime Object: type,
    /// An anonymous struct instance which contains the following:
    ///
    /// - ignored: [][]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - derived: { .field = fn (*const Object) ?DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    comptime options: anytype,
) type {
    comptime var index_fields: []const std.builtin.TypeInfo.StructField = &.{};
    
    // Generate index LSM trees from the struct fields.
    inline for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        comptime var ignored = false;
        inline for (options.ignored) |ignored_field_name| {
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }

        if (!ignored) {
            const tree_name = @typeName(Object) ++ "." ++ field.name;
            const IndexTree = IndexTreeType(Storage, field.field_type, tree_name);
            index_fields = index_fields ++ [_]std.builtin.TypeInfo.StructField{
                .{
                    .name = field.name,
                    .field_type = IndexTree,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(IndexTree),
                },
            };
        }
    }

    // Generiate IndexTrees for fields derived from the Value in options.
    const derived_fields = std.meta.fields(@TypeOf(options.derived));
    inline for (derived_fields) |field| {
        // Get the function info for the derived field.
        const derive_func = @field(options.derived, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).Fn;
        
        // Make sure it has only one argument.
        if (derive_func_info.args.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Make sure the function takes in a reference to the Value:
        const derive_arg = derive_func_info.args[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.arg_type != *const Object) {
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
                .field_type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    const ObjectTree = ObjectTreeType(Storage, Object);
    const IndexTrees = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    // Verify no hash collisions between all the trees:
    comptime var hashes: []const u128 = &.{ ObjectTree.hash };

    inline for (std.meta.fields(IndexTrees)) |field| {
        const IndexTree = @TypeOf(@field(@as(IndexTrees, undefined), field.name));
        const hash: []const u128 = &.{ IndexTree.hash };

        assert(std.mem.containsAtLeast(u128, hashes, 0, hash));
        hashes = hashes ++ hash;
    }

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len
        - options.ignored.len
        + std.meta.fields(@TypeOf(options.derived)).len;
    assert(indexes_count_actual == indexes_count_expect);

    // Generate a helper function for interacting with an Index field type.
    const IndexTreeFieldHelperType = struct {
        fn HelperType(comptime field_name: []const u8) type {
            // Check if the index is derived.
            comptime var derived = false;
            inline for (derived_fields) |derived_field| {
                derived = derived or std.mem.eql(u8, derived_field.name, field_name);
            }

            // Get the index value type.
            const Value: type = blk: {
                if (!derived) {
                    break :blk @TypeOf(@field(@as(Object, undefined), field_name));
                }

                const derived_fn = @TypeOf(@field(options.derived, field_name));
                break :blk @typeInfo(derived_fn).Fn.return_type.?.Optional.child;
            };

            return struct {
                pub const Key = IndexCompositeKeyType(Value);

                pub fn derive(object: *const Object) ?Value {
                    if (derived) {
                        return @field(options.derived, field_name)(object);
                    } else {
                        return @field(object, field_name);
                    }
                }

                pub fn to_composite_key(value: Value) Key {
                    return switch (@typeInfo(Value)) {
                        .Enum => @enumToInt(value),
                        .Int => value,
                        else => @compileError("Unsupported index value type"),
                    };
                }
            };
        }
    }.HelperType;

    const Key = ObjectTree.TableType.Key;
    const Value = ObjectTree.TableType.Value;
    const key_from_value = ObjectTree.TableType.key_from_value;

    return struct {
        const Groove = @This();

        const Grid = GridType(Storage);

        const Callback = fn (*Groove) void;
        const JoinOp = enum { compacting, checkpoint };

        join_op: ?JoinOp = null,
        join_pending: usize = 0,
        join_callback: ?Callback = null,

        cache: *ObjectTree.ValueCache,
        objects: ObjectTree,
        indexes: IndexTrees,

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            // The cache size is meant to be computed based on the left over available memory
            // that tigerbeetle was given to allocate from CLI arguments.
            cache_size: u32,
            // In general, the commit count max for a field, depends on the field's object,
            // how many objects might be changed by a batch:
            //   (config.message_size_max - sizeOf(vsr.header))
            // For example, there are at most 8191 transfers in a batch.
            // So commit_count_max=8191 for transfer objects and indexes.
            //
            // However, if a transfer is ever mutated, then this will double commit_count_max
            // since the old index might need to be removed, and the new index inserted.
            //
            // A way to see this is by looking at the state machine. If a transfer is inserted,
            // how many accounts and transfer put/removes will be generated?
            //
            // This also means looking at the state machine operation that will generate the
            // most put/removes in the worst case.
            // For example, create_accounts will put at most 8191 accounts.
            // However, create_transfers will put 2 accounts (8191 * 2) for every transfer, and
            // some of these accounts may exist, requiring a remove/put to update the index.
            //
            // TODO(King) Since this is state machine specific, let's expose `commit_count_max`
            // as an option when creating the groove.
            // Then, in our case, we'll create the Accounts groove with a commit_count_max of 
            // 8191 * 2 (accounts mutated per transfer) * 2 (old/new index value).
            commit_count_max: usize,
        ) !Groove {
            // Cache is dynamically allocated to pass a pointer into the Object tree.
            const cache = try allocator.create(ObjectTree.ValueCache);
            errdefer allocator.destroy(cache);

            cache.* = .{};
            try cache.ensureTotalCapacity(allocator, cache_size);
            errdefer cache.deinit(allocator);

            // Intialize the object LSM tree.
            var object_tree = try ObjectTree.init(
                allocator,
                node_pool,
                grid,
                cache,
                .{
                    .prefetch_count_max = commit_count_max * 2,
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer object_tree.deinit(allocator);

            var index_trees_initialized: usize = 0;
            var index_trees: IndexTrees = undefined;

            // Make sure to deinit initialized index LSM trees on error.
            errdefer inline for (std.meta.fields(IndexTrees)) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    @field(index_trees, field.name).deinit(allocator);
                }
            };

            // Initialize index LSM trees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(index_trees, field.name) = try field.field_type.init(
                    allocator,
                    node_pool,
                    grid,
                    null, // No value cache for index trees.
                    .{
                        .prefetch_count_max = 0,
                        .commit_count_max = commit_count_max,
                    },
                );
                index_trees_initialized += 1;
            }

            return Groove{
                .cache = cache,
                .objects = object_tree,
                .indexes = index_trees,
            };
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            assert(groove.join_op == null);
            assert(groove.join_pending == 0);
            assert(groove.join_callback == null);            

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);
            groove.cache.deinit(allocator);
            
            allocator.destroy(groove.cache);
            groove.* = undefined;
        }

        pub fn get(groove: *Groove, key: Key) ?*const Value {
            return groove.objects.get(key);
        }

        pub fn put(groove: *Groove, value: *const Value) void {
            if (groove.get(key_from_value(value))) |existing_value| {
                groove.update(existing_value, value);
            } else {
                groove.insert(value);
            }
        }

        /// Insert the value into the objects tree and its fields into the index trees.
        fn insert(groove: *Groove, value: *const Value) void {
            groove.objects.put(value);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive(value)) |index| {
                    const composite_key = Helper.to_composite_key(index);
                    @field(groove.indexes, field.name).put(composite_key);
                }
            }
        }

        /// Update the object and index tress by diff'ing the old and new values.
        fn update(groove: *Groove, old: *const Value, new: *const Value) void {
            // Update the object tree entry if any of the fields (even ignored) are different.
            if (!std.mem.eql(u8, std.mem.asBytes(old), std.mem.asBytes(new))) {
                groove.objects.remove(old);
                groove.objects.put(new);
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                const old_index = Helper.derive(old);
                const new_index = Helper.derive(new);

                if (old_index != new_index) {
                    if (old_index) |value| {
                        const old_composite_key = Helper.to_composite_key(value);
                        @field(groove.indexes, field.name).remove(old_composite_key);
                    }

                    if (new_index) |value| {
                        const new_composite_key = Helper.to_composite_key(value);
                        @field(groove.indexes, field.name).put(&.{
                            .field = new_composite_key,
                            .timestamp = new.timestamp,
                        });
                    }
                }
            }
        }

        pub fn remove(groove: *Groove, value: *const Value) void {
            const key = key_from_value(value);
            const existing_value = groove.objects.get(key).?;
            assert(std.mem.eql(u8, std.mem.asBytes(existing_value), std.mem.asBytes(value)));
            
            groove.objects.remove(key);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive(value)) |index| {
                    const composite_key = Helper.to_composite_key(index);
                    @field(groove.indexes, field.name).remove(composite_key);
                }
            }
        }

        /// Maximum number of pending sync callbacks (ObjecTree + IndexTrees).
        const join_pending_max = 1 + std.meta.fields(IndexTrees).len;

        fn JoinType(comptime join_op: JoinOp) type {
            return struct {
                pub fn start(groove: *Groove, join_callback: Callback) void {
                    // Make sure no sync op is currently running.
                    assert(groove.join_op == null);
                    assert(groove.join_pending == 0);
                    assert(groove.join_callback == null);
                    
                    // Start the sync operations
                    groove.join_op = join_op;
                    groove.join_callback = join_callback;
                    groove.join_pending = join_pending_max;
                }

                /// Returns LSM tree type for the given index field name (or ObjectTree if null).
                fn TreeFor(comptime index_field_name: ?[]const u8) type {
                    const index_field = index_field_name orelse return ObjectTree;
                    return @TypeOf(@field(@as(IndexTrees, undefined), index_field));
                }

                pub fn tree_callback(
                    comptime index_field_name: ?[]const u8,
                ) fn (*TreeFor(index_field_name)) void {
                    return struct {
                        fn tree_cb(tree: *TreeFor(index_field_name)) void {
                            // Derive the groove pointer from the tree using the index_field_name.
                            const groove = blk: {
                                const index_field = index_field_name orelse {
                                    break :blk @fieldParentPtr(Groove, "objects", tree);
                                };

                                const indexes = @fieldParentPtr(IndexTrees, index_field, tree);
                                break :blk @fieldParentPtr(Groove, "indexes", indexes);
                            };

                            // Make sure the sync operation is currently running.
                            assert(groove.join_op == join_op);
                            assert(groove.join_callback != null);
                            assert(groove.join_pending <= join_pending_max);
                            
                            // Guard until all pending sync ops complete.
                            groove.join_pending -= 1;
                            if (groove.join_pending > 0) return;

                            const callback = groove.join_callback.?;
                            groove.join_op = null;
                            groove.join_callback = null;
                            callback(groove);
                        }
                    }.tree_cb;
                }
            };
        }

        pub fn compact_io(groove: *Groove, op: u64, callback: Callback) void {
            // Start a compacting join operation.
            const Join = JoinType(.compacting);
            Join.start(groove, callback);
            
            // Compact the ObjectTree.
            groove.objects.compact_io(op, Join.tree_callback(null));

            // Compact the IndexTrees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_io(op, Join.tree_callback(field.name));
            }
        }

        pub fn compact_cpu(groove: *Groove) void {
            // Make sure a compacting join operation is running
            assert(groove.join_op == .compacting);
            assert(groove.join_pending <= join_pending_max);
            assert(groove.join_callback != null);

            groove.objects.compact_cpu();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_cpu();
            }
        }

        pub fn checkpoint(groove: *Groove, callback: fn (*Groove) void) void {
            // Start a checkpoint join operation.
            const Join = JoinType(.checkpoint);
            Join.start(groove, callback);
            
            // Checkpoint the ObjectTree.
            groove.objects.checkpoint(Join.tree_callback(null));

            // Checkpoint the IndexTrees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).checkpoint(Join.tree_callback(field.name));
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
            .ignored = [_][]const u8{ "reserved", "user_data", "flags" },
            .derived = .{},
        },
    );

    _ = Groove.init;
    _ = Groove.deinit;

    _ = Groove.get;
    _ = Groove.put;
    _ = Groove.remove;

    _ = Groove.compact_io;
    _ = Groove.compact_cpu;
    _ = Groove.checkpoint;
}
