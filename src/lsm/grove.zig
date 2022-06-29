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

/// Creates an LSM tree type for the Grove's Object tree (where Value = Account or Transfer).
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

        fn tombstone(value: *const Value) callconv(.Inline) bool {
            @panic("TODO: check if value is tombstone")
        }

        fn tombstone_from_key(timestamp: u64) callconv(.Inline) Value {
            @panic("TODO: return tombstone value from timestamp");
        }
    };

    const Table = TableType(
        Storage,
        Key,
        Value,
        ValueKeyHelpers.compare_keys,
        ValueKeyHelpers.key_from_value,
        ValueKeyHelpers.sentinel_key,
        ValueKeyHelpers.tombstone,
        ValueKeyHelpers.tombstone_from_key,
    );

    return TreeType(Table);
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

// TODO(King) CompositeKey accepts only a u128 or u64 type.
// If the field is a u32 (e.g. ledger) or u16 (e.g. flags, code) then upgrade it to a u64 type.

// TODO(King) If this is an object tree, then the Key is a u64 timestamp, not a CompositeKey.
// The Value will then be the Object itself.
// i.e. The CompositeKey is only used for indexes.
//
// For an object tree:
//     Key is the u64 timestamp (e.g. account/transfer.timestamp, when the object was created).
//     Value is the Object itself.
//     compare_keys() compares timestamps directly as u64s.
//     key_from_value() returns object.timestamp.
//     sentinel_key should be math.maxInt(u64).
//
// TODO(King) Since an object tree can already find objects by timestamp, we do not need to
// create an index on the timestamp field of each object as this would be redundant.
// e.g. timestamp->timestamp.

fn IndexTreeType(comptime Storage: type, comptime Field: type) type {
    const Key = CompositeKey(IndexCompositeKeyType(Field));
    const Table = TableType(
        Storage,
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    );
    return TreeType(Table);
}

/// A Grove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GroveType(
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
    comptime var index_fields: []const builtin.TypeInfo.StructField = &.{};
    
    // Generate index LSM trees from the struct fields
    inline for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options
        var ignore = false;
        inline for (options.ignored) |ignored_field_name| {
            ignore = ignore or std.mem.eql(u8, field.name, ignored_field_name);
        }

        // TODO(King) When constructing a tree type, also tell the tree it's comptime "hash" that
        // can be used to identify the tree uniquely in our on disk structures (e.g. ManifestLog).
        // This needs to be stable, even as we add more trees down the line, or reorder structs.
        // So let's go with a Blake3 hash on "grove_name" + "tree_name", then truncate to u128.
        // The motivation for Blake3 is only that we use it elsewhere. We could also use Sha256 if
        // there's any comptime issue with Blake3.
        // e.g. Hash.update("accounts")+Hash.update("id") and Hash("transfers")+Hash("id")

        // TODO(King) Assert that no tree hash in the whole forest collides with that of another.

        if (!ignored) {
            const IndexTree = IndexTreeType(Storage, field.field_type);
            index_fields = index_fields ++ [_]const builtin.TypeInfo.StructField{
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
            @compileError("expected derive fn to take in *const " ++ @typeName(Struct));
        }

        // Make sure the function takes in a reference to the Value.
        const derive_arg = derive_func_info.args[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.arg_type != *const Struct) {
           @compileError("expected derive fn to take in *const " ++ @typeName(Struct));
        }

        // Get the return value from the derived field as the DerivedType.
        const derive_return_type = derive_func_info.return_type orelse {
            @compileError("expected derive fn to return valid tree index type");
        };

        // Create an IndexTree for the DerivedType.
        const DerivedType = @typeInfo(derive_return_type).Optional.child;
        const IndexTree = IndexTreeType(Storage, DerivedType);
        index_fields = index_fields ++ [_]const builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    const Grid = GridType(Storage);
    const ObjectTree = ObjectTreeType(Storage, Object);
    const IndexTrees = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_field_types,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    /// Generate a helper function for interacting with an Index field type
    const IndexTreeFieldHelper = struct {
        fn Helper(comptime field_name: []const u8) type {
            // Check if the index is derived.
            comptime var derived = false;
            inline for (derived_fields) |derived_field| {
                derived = derived or std.mem.eql(u8, derived_field.name, field_name);
            }

            // Get the index value type.
            const Value = blk: {
                if (!derived) {
                    break @TypeOf(@field(@as(Object, undefined), field_name));
                }

                const derived_fn = @TypeOf(@field(options.derived, field_name));
                break :blk @typeInfo(derive_fn).Fn.return_type.?.Optional.child;
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

                pub fn from_composite_key(key: IndexKeyType) Value {
                    return switch (@typeInfo(Value)) {
                        .Enum => |e| @intToEnum(Value, @intCast(e.tag_type, key)),
                        .Int => @intCast(Value, key),
                        else => @compileError("Unsupported index value type"),
                    };
                }
            };
        }
    }.Helper;

    const Key = ObjectTree.Table.Key;
    const Value = ObjecTree.Table.Value;
    const compare_keys = ObjectTree.Table.compare_keys;
    const key_from_value = ObjectTree.Table.key_from_value;

    return struct {
        const Grove = @This();

        cache: ObjecTree.ValueCache,
        objects: ObjectTree,
        indexes: IndexTrees,

        pub fn init(
            grove: *Grove,
            allocator: mem.Allocator, 
            grid: *Grid,
            node_pool: *NodePool,
            // The cache size is meant to be computed based on the left over available memory
            // that tigerbeetle was given to allocate from CLI arguments.
            cache_size: usize,
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
            // as an option when creating the grove.
            // Then, in our case, we'll create the Accounts grove with a commit_count_max of 
            // 8191 * 2 (accounts mutated per transfer) * 2 (old/new index value).
            commit_count_max: usize,
        ) !void {
            // Initialize the value cache for the obejct LSM tree
            grove.cache = .{};
            try grove.cache.ensureTotalCapacity(allocator, cache_size);
            errdefer grove.cache.deinit(allocator);
            
            // Intialize the object LSM tree
            grove.objects = try ObjectTree.init(
                allocator,
                grid,
                node_pool,
                &grove.cache,
                .{
                    .prefetch_count_max = commit_count_max * 2,
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer grove.objects.deinit(allocator);

            var index_trees_initialized: usize = 0;
            var index_trees: IndexTrees = undefined;

            // Make sure to deinit initialized index LSM trees on error
            errdefer inline for (std.meta.fields(IndexTrees)) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    @field(index_trees, field.name).deinit(allocator);
                }
            }

            // Initialize index LSM trees
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(indexes, field.name) = try field.field_type.init(
                    allocator,
                    grid,
                    node_pool,
                    null,
                    .{
                        .prefetch_count_max = commit_count_max * 2,
                        .commit_count_max = commit_count_max,
                    },
                );
                index_trees_initialized += 1;
            }
        }

        pub fn deinit(grove: *Grove, allocator: mem.Allocator) void {
            grove.cache.deinit(allocator);
            grove.objects.deinit(allocator);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(grove.indexes, field.name).deinit(allocator);
            }
        }

        pub fn get(grove: *Grove, key: Key) ?*const Value {
            return grove.objects.get(key);
        }

        pub fn put(grove: *Grove, value: Value) void {
            if (grove.get(key_from_value(&value))) |existing_value| {
                grove.update(existing_value, &value);
            } else {
                grove.insert(&value);
            }
        }

        /// Insert the value into the objects tree and its fields into the index trees.
        fn insert(grove: *Grove, value: *const Value) void {
            grove.objects.put(value.*);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelper(field.name);

                if (Helper.derive(value)) |index| {
                    const composite_key = Helper.to_composite_key(index);
                    @field(grove.indexes, field.name).put(composite_key);
                }
            }
        }

        /// Update the object and index tress by diff'ing the old and new values.
        fn update(grove: *Grove, old: *const Value, new: *const Value) void {
            // Update the object tree entry if any of the indexes changed.
            var updated = false;
            defer if (updated) {
                grove.objects.remove(key_from_value(old));
                grove.objects.put(new.*);
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelper(field.name);

                // Derive the old and new index values
                if (Helper.derive(old)) |old_index| {
                    const new_index = Helper.derive(new).?;

                    // Update the index values if they're not the same (diff).
                    if (old_index != new_index) {
                        const index_tree = &@field(grove.indexes, field.name);
                        updated = true;

                        const old_composite_key = Helper.to_composite_key(old_index);
                        index_tree.remove(old_composite_key);

                        const new_composite_key = Helper.to_composite_key(new_index);
                        index_tree.put(.{
                            .field = new_composite_key,
                            .timestamp = new.timestamp, // TODO(King) Is this correct?
                        });
                    }
                }
            }
        }

        pub fn remove(grove: *Grove, value: Value) void {
            grove.objects.remove(key_from_value(value));

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelper(field.name);

                if (Helper.derive(value)) |index| {
                    const composite_key = Helper.to_composite_key(index);
                    @field(grove.indexes, field.name).remove(composite_key);
                }
            }
        }

        pub fn compact_io(grove: *Grove, callback: fn (*Grove) void) void {
            @panic("TODO: compact entire grove");
        }

        pub fn compact_cpu(grove: *Grove) void {
            @panic("TODO: compact entire grove");
        }

        pub fn checkpoint(grove: *Grove, callback: fn (*Grove) void) void {
            // The ManifestLog is in fact specific to each tree.
            // So we need to call checkpoint on each tree to flush its manifest log.
            @panic("TODO: checkpoint entire grove");
        }
    };
}
