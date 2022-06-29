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

fn LSMTreeType(
    comptime Storage: type, 
    comptime Struct: type,
) type {
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

        pub fn get(grove: *Grove, key: Objects.Key) ?*const Objecst.Value {
            return grove.objects.get(key);
        }

        pub fn put(grove: *Grove, value: Objects.Value) void {
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
    };
}