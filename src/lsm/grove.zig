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

// TODO(King) Rename to IndexTreeType, since everything is in the LSM namespace.
// TODO(King) Use a similar helper to construct an ObjectTreeType?
fn LSMTreeType(
    comptime Storage: type, 
    comptime Struct: type,
) type {
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
    const Key = CompositeKey(Struct);
    const Table = TableType(
        Storage,
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
    );
    return TreeType(Table);
}

/// A Grove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GroveType(
    comptime Storage: type,
    comptime Struct: type,
    comptime derived_fields_config: anytype,
) type {
    comptime var index_fields: []const builtin.TypeInfo.StructField = &.{};
    
    // Generate index LSM trees from the struct fields
    inline for (std.meta.fields(Struct)) |field| {
        // TODO(King) When constructing a tree type, also tell the tree it's comptime "hash" that
        // can be used to identify the tree uniquely in our on disk structures (e.g. ManifestLog).
        // This needs to be stable, even as we add more trees down the line, or reorder structs.
        // So let's go with a Blake3 hash on "grove_name" + "tree_name", then truncate to u128.
        // The motivation for Blake3 is only that we use it elsewhere. We could also use Sha256 if
        // there's any comptime issue with Blake3.
        // e.g. Hash("accounts")+Hash("id") and Hash("transfers")+Hash("id")

        // TODO(King) Assert that no tree hash in the whole forest collides with that of another.

        const lsm_tree_type = LSMTreeType(Storage, field.field_type);

        index_fields = index_fields ++ [_]const builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = lsm_tree_type,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(lsm_tree_type),
            },
        };
    }

    // Generiate index LSM streems from the derived fields
    const derived_fields = std.meta.fields(@TypeOf(derived_fields_config));
    inline for (derived_fields) |field| {
        const derive_func = @field(derived_fields_config, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).Fn;
        
        if (derive_func_info.args.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Struct));
        }

        const derive_arg = derive_func_info.args[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.arg_type != *const Struct) {
           @compileError("expected derive fn to take in *const " ++ @typeName(Struct));
        }

        const derive_return_type = derive_func_info.return_type orelse {
            @compileError("expected derive fn to return valid tree index type");
        };

        const derive_type = @typeInfo(derive_return_type).Optional.child;
        const lsm_tree_type = LSMTreeType(Storage, derive_type);

        index_fields = index_fields ++ [_]const builtin.TypeInfo.StructField{
            .{
                .name = field.name,
                .field_type = lsm_tree_type,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(lsm_tree_type),
            },
        };
    }

    const Grid = GridType(Storage);
    const Objects = LSMTreeType(Storage, Struct);
    const Indexes = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_field_types,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    return struct {
        const Grove = @This();

        objects_cache: Objects.ValueCache,
        objects: Objects,
        indexes: Indexes,

        pub fn init(
            allocator: mem.Allocator, 
            grid: *Grid,
            node_pool: *NodePool,
        ) !Grove {
            // Initialize the object cache for the obejct LSM tree
            var objects_cache = Objects.ValueCache{};
            try objects_cache.ensureTotalCapacity(allocator, @panic("TODO: cache size"));
            errdefer objects_cache.deinit(allocator);

            // Initialize the object LSM tree
            // TODO: pass objects_cache by pointer?
            const commit_count_max = @panic("TODO: commit count max");
            var objects = try Objects.init(
                allocator,
                grid,
                node_pool,
                &objects_cache,
                .{
                    .prefetch_count_max = commit_count_max * 2,
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer objects.deinit(allocator);

            var indexes_initialized: usize = 0;
            var indexes: Indexes = undefined;

            // Make sure to deinit initialized index LSM trees on error
            errdefer inline for (std.meta.fields(Indexes)) |field, field_index| {
                if (indexes_initialized >= field_index + 1) {
                    @field(indexes, field.name).deinit(allocator);
                }
            }

            // Initialize index LSM trees
            inline for (std.meta.fields(Indexes)) |field| {
                const lsm_tree_type = field.field_type;
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
                const commit_count_max = @panic("TODO: commit count max for " ++ field.name);

                @field(indexes, field.name) = try lsm_tree_type.init(
                    allocator,
                    grid,
                    node_pool,
                    null,
                    .{
                        .prefetch_count_max = commit_count_max * 2,
                        .commit_count_max = commit_count_max,
                    },
                );

                indexes_initialized += 1;
            }

            return Grove{
                .objects_cache = objects_cache,
                .objects = objects,
                .indexes = indexes,
            };
        }

        pub fn deinit(grove: *Grove, allocator: mem.Allocator) void {
            grove.objects_cache.deinit(allocator);
            grove.objects.deinit(allocator);

            inline for (std.meta.fields(Indexes)) |field| {
                @field(grove.indexes, field.name).deinit(allocator);
            }
        }

        pub fn get(grove: *Grove, key: Objects.Key) ?*const Objects.Value {
            return grove.objects.get(key);
        }

        pub fn put(grove: *Grove, value: Objects.Value) void {
            // TODO(King) Check if the object already exists, and if it does, diff the indexes.
            // For example, when value_old.debits_pending=400 and value_new.debits_pending=450:
            // If value_new.debits_pending == value_old.debits_pending -> do nothing (no put/remove)
            // Otherwise remove(old field value) then put(new field value).
            grove.objects.put(value);

            // Insert the value's fields into the index LSM trees.
            // For derived indexes, look up what to insert using the value + derived field config.
            inline for (std.meta.fields(Indexes)) |field| {
                comptime var is_derived = false;
                inline for (derived_fields) |derived_field| {
                    is_derived = is_derived or std.mem.eql(u8, derived_field.name, field.name);
                }

                if (is_derived) {
                    if (@field(derived_fields_config, field.name)(&value)) |derived_value| {
                        @field(grove.indexes, field.name).put(derived_value);
                    }
                } else {
                    @field(grove.indexes, field.name).put(@field(value, field.name));
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
