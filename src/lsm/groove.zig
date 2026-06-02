const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx");
const constants = @import("../constants.zig");

const TableType = @import("table.zig").TableType;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("../vsr/grid.zig").GridType;
const UniqueKeyType = @import("unique_key.zig").UniqueKeyType;
const CompositeKeyType = @import("composite_key.zig").CompositeKeyType;
const NodePool = @import("node_pool.zig").NodePoolType(constants.lsm_manifest_node_size, 16);
const CacheMapType = @import("cache_map.zig").CacheMapType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ScanBuilderType = @import("scan_builder.zig").ScanBuilderType;

const ScratchMemory = @import("scratch_memory.zig").ScratchMemory;

const snapshot_latest = @import("tree.zig").snapshot_latest;

fn ObjectTreeHelperType(comptime Object: type) type {
    assert(@hasField(Object, "timestamp"));
    assert(@FieldType(Object, "timestamp") == u64);

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

/// LSM Tree for unique keys.
fn UniqueKeyTreeType(
    comptime Storage: type,
    comptime Field: type,
    comptime table_value_count_max: usize,
) type {
    const UniqueKey = UniqueKeyType(IndexType(Field));
    const Table = TableType(
        UniqueKey.Key,
        UniqueKey,
        UniqueKey.key_from_value,
        UniqueKey.sentinel_key,
        UniqueKey.tombstone,
        UniqueKey.tombstone_from_key,
        table_value_count_max,
        .general,
    );

    return TreeType(Table, Storage);
}

/// LSM Tree for secondary indexes.
fn IndexTreeType(
    comptime Storage: type,
    comptime Field: type,
    comptime table_value_count_max: usize,
) type {
    const CompositeKey = CompositeKeyType(IndexType(Field));
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

/// Normalizes index tree field types into void, u64 or u128 for UniqueKey and CompositeKey.
fn IndexType(comptime Field: type) type {
    switch (@typeInfo(Field)) {
        .void => return void,
        .@"enum" => |e| {
            return switch (@bitSizeOf(e.tag_type)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u65)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported enum tag for index: " ++ @typeName(e.tag_type)),
            };
        },
        .int => |i| {
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
    assert(IndexType(void) == void);
    assert(IndexType(u0) == u64);
    assert(IndexType(enum(u0) { x }) == u64);

    assert(IndexType(u1) == u64);
    assert(IndexType(u16) == u64);
    assert(IndexType(enum(u16) { x }) == u64);

    assert(IndexType(u32) == u64);
    assert(IndexType(u63) == u64);
    assert(IndexType(u64) == u64);

    assert(IndexType(enum(u65) { x }) == u128);
    assert(IndexType(u65) == u128);
    assert(IndexType(u128) == u128);
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
    ///     An anonymous struct which contains, for each indexed field, unique key,
    ///     or derived index of `Object`, the maximum number of values per table
    ///     per batch for the corresponding LSM tree.
    ///
    /// - primary_key: [:0]const u8
    ///     The name of the Groove's primary key. It should be either `timestamp` or one of
    ///     the `unique_keys` (as long as it is not an optional index).
    ///     Lookups and the object cache are keyed by this field.
    ///
    /// - primary_key_orphaned: bool
    ///     Whether the Groove should store objectless primary keys to prevent their reuse.
    ///     Can only be set to `true` if `primary_key` is not the Object's timestamp.
    ///
    /// - unique_keys: [][:0]const u8:
    ///     An array of fields that are unique keys (i.e. `id`).
    ///     The `timestamp` field is always the object's identifier and should not be specified
    ///     as a unique key.
    ///
    /// - ignored: [][:0]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - optional: [][:0]const u8:
    ///     An array of fields that should *not* index zero values.
    ///     Unique keys can be optional, except for the primary key.
    ///
    ///
    /// - derived: { .field = *const fn (*const Object) ?DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    ///     Derived indexes can be made optional by returning `null`.
    ///
    /// - objects_cache: bool:
    ///     Whether Groove should have an ObjectCache.
    ///     Should be `false` only if both `prefetch_entries_for_update_max` and
    ///     `prefetch_entries_for_read_max` are set to 0.
    comptime groove_options: anytype,
) type {
    @setEvalBranchQuota(64_000);

    // `groove_options` must be `anytype` because of groove-specific fields.
    // Keep this validation in sync with the documentation above.
    const GrooveOptions = @TypeOf(groove_options);
    assert(@hasField(GrooveOptions, "ids"));
    assert(@hasField(GrooveOptions, "batch_value_count_max"));
    assert(@hasField(GrooveOptions, "primary_key"));
    assert(@hasField(GrooveOptions, "primary_key_orphaned"));
    assert(@hasField(GrooveOptions, "unique_keys"));
    assert(@hasField(GrooveOptions, "ignored"));
    assert(@hasField(GrooveOptions, "optional"));
    assert(@hasField(GrooveOptions, "derived"));
    assert(@hasField(GrooveOptions, "objects_cache"));
    assert(std.meta.fields(GrooveOptions).len == 9);

    assert(@hasField(Object, "timestamp"));
    assert(@FieldType(Object, "timestamp") == u64);
    assert(@hasField(Object, groove_options.primary_key));
    const PrimaryKey = @FieldType(Object, groove_options.primary_key);

    const _is_primary_key = struct {
        /// Checks if an identifier is the Object's primary key.
        /// It accepts enums, enum literals, or strings containing the identifier.
        inline fn is_primary_key(identifier: anytype) bool {
            const ObjectField = std.meta.FieldEnum(Object);
            const primary_key: ObjectField = comptime @field(
                ObjectField,
                groove_options.primary_key,
            );

            const Identifier = @TypeOf(identifier);
            if (Identifier == ObjectField) {
                return identifier == primary_key;
            }

            if (@typeInfo(Identifier) == .@"enum" or
                @typeInfo(Identifier) == .enum_literal)
            {
                // Allow checking for unknown fields, as we can use
                // tags coming from derived indexes.
                const object_field: ?ObjectField = switch (identifier) {
                    inline else => |tag| if (@hasField(ObjectField, @tagName(tag)))
                        @field(ObjectField, @tagName(tag))
                    else
                        null,
                };

                return object_field == primary_key;
            }

            // Assuming it's a string.
            return std.mem.eql(u8, @tagName(primary_key), identifier);
        }
    }.is_primary_key;

    if (_is_primary_key(.timestamp)) {
        assert(!groove_options.primary_key_orphaned);
        maybe(groove_options.unique_keys.len == 0);
    } else {
        assert(groove_options.unique_keys.len > 0);
        maybe(groove_options.primary_key_orphaned);

        // Verify if the primary key is also a unique key.
        for (groove_options.unique_keys) |field_name| {
            comptime assert(!std.mem.eql(u8, field_name, "timestamp"));
            if (std.mem.eql(u8, field_name, groove_options.primary_key)) {
                break;
            }
        } else @compileError(
            "primary_key: not defined as a unique key " ++ groove_options.primary_key,
        );
    }

    // Verify that every entry referenced by "ignored" corresponds to an actual field.
    for (groove_options.ignored) |field_name| {
        comptime assert(!std.mem.eql(u8, field_name, "timestamp"));
        comptime assert(!std.mem.eql(u8, field_name, groove_options.primary_key));
        if (!@hasField(Object, field_name)) {
            @compileError("ignore: unrecognized field name " ++ field_name);
        }
    }

    // Verify that every entry referenced by "optional" corresponds to an actual field.
    for (groove_options.optional) |field_name| {
        comptime assert(!std.mem.eql(u8, field_name, "timestamp"));
        if (!@hasField(Object, field_name)) {
            @compileError("optional: unrecognized field name " ++ field_name);
        }
    }

    // Verify the unique keys.
    for (groove_options.unique_keys) |field_name| {
        comptime assert(!std.mem.eql(u8, field_name, "timestamp"));
        if (!@hasField(Object, field_name) and
            !@hasField(@TypeOf(groove_options.derived), field_name))
        {
            @compileError("unique_keys: unrecognized field name " ++ field_name);
        }

        // Neither the "timestamp" or any unique key should be provided
        // in groove_options.ignored.
        comptime var ignored = false;
        for (groove_options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            ignored = ignored or std.mem.eql(u8, field_name, ignored_field_name);
        }
        comptime assert(!ignored);

        // Unique keys can be optional (i.e. pending_id).
        comptime var optional = false;
        for (groove_options.optional) |optional_field_name| {
            comptime assert(!std.mem.eql(u8, optional_field_name, "timestamp"));
            optional = optional or std.mem.eql(u8, field_name, optional_field_name);
        }
        comptime maybe(optional);
    }

    comptime var index_fields: []const std.builtin.Type.StructField = &.{};

    // Generate index LSM trees from the struct fields.
    for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        // By default, we ignore the "timestamp" and the unique keys.
        comptime var ignored = mem.eql(u8, field.name, "timestamp");
        for (groove_options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }
        if (ignored) continue;

        comptime var unique_key: bool = false;
        for (groove_options.unique_keys) |unique_key_name| {
            unique_key = unique_key or std.mem.eql(u8, field.name, unique_key_name);
        }

        const table_value_count_max = constants.lsm_compaction_ops *
            @field(groove_options.batch_value_count_max, field.name);
        const IndexTree = if (unique_key)
            UniqueKeyTreeType(Storage, field.type, table_value_count_max)
        else
            IndexTreeType(Storage, field.type, table_value_count_max);

        index_fields = index_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = IndexTree,
                .default_value_ptr = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    // Generate IndexTrees for fields derived from the Value in groove_options.
    const derived_fields = std.meta.fields(@TypeOf(groove_options.derived));
    for (derived_fields) |field| {
        // Get the function info for the derived field.
        const derive_func = @field(groove_options.derived, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).@"fn";

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
        if (derive_return_type != .optional) {
            @compileError("expected derive fn to return optional tree index type");
        }

        comptime var unique_key: bool = false;
        for (groove_options.unique_keys) |unique_key_name| {
            unique_key = unique_key or std.mem.eql(u8, field.name, unique_key_name);
        }

        const DerivedType = derive_return_type.optional.child;
        const table_value_count_max = constants.lsm_compaction_ops *
            @field(groove_options.batch_value_count_max, field.name);
        const IndexTree = if (unique_key)
            UniqueKeyTreeType(Storage, DerivedType, table_value_count_max)
        else
            IndexTreeType(Storage, DerivedType, table_value_count_max);
        index_fields = index_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = IndexTree,
                .default_value_ptr = null,
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
            .default_value_ptr = null,
            .is_comptime = false,
            .alignment = @alignOf(IndexTree.Options),
        };
    }

    const ObjectTreeHelper = ObjectTreeHelperType(Object);

    const _ObjectTree = T: {
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
        break :T TreeType(Table, Storage);
    };

    const _IndexTrees = @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });
    const _IndexTreeOptions = @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &index_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const has_scan = index_fields.len > 0;

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(_IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len +
        std.meta.fields(@TypeOf(groove_options.derived)).len -
        groove_options.ignored.len -
        // The timestamp field is implicitly ignored since it's the primary key for ObjectTree:
        @as(usize, 1);
    assert(indexes_count_actual == indexes_count_expect);
    assert(indexes_count_actual == std.meta.fields(_IndexTreeOptions).len);

    const _IndexHelperType = struct {
        fn HelperType(comptime field_name: []const u8) type {
            return struct {
                pub const Type = type: {
                    if (is_derived) {
                        const derived_fn = @typeInfo(@TypeOf(@field(
                            groove_options.derived,
                            field_name,
                        )));
                        assert(derived_fn == .@"fn");
                        assert(derived_fn.@"fn".return_type != null);

                        const return_type = @typeInfo(derived_fn.@"fn".return_type.?);
                        assert(return_type == .optional);
                        break :type return_type.optional.child;
                    }

                    break :type @FieldType(Object, field_name);
                };

                pub const IndexPrefix = IndexType(Type);

                pub const is_unique_key: bool = is_unique: {
                    for (groove_options.unique_keys) |unique_key| {
                        if (std.mem.eql(u8, unique_key, field_name)) break :is_unique true;
                    }
                    break :is_unique false;
                };

                pub const is_derived: bool = is_derived: {
                    for (derived_fields) |derived_field| {
                        if (std.mem.eql(u8, derived_field.name, field_name)) break :is_derived true;
                    }
                    break :is_derived false;
                };

                pub const allow_zero: bool = allow_zero: {
                    for (groove_options.optional) |optional| {
                        if (std.mem.eql(u8, field_name, optional)) {
                            assert(!is_derived);
                            break :allow_zero false;
                        }
                    }
                    break :allow_zero true;
                };

                inline fn as_prefix(index: Type) IndexPrefix {
                    return switch (@typeInfo(Type)) {
                        .void => {},
                        .int => index,
                        .@"enum" => @intFromEnum(index),
                        else => unreachable,
                    };
                }

                /// Try to extract an index from the object, deriving it when necessary.
                /// Null means the value should not be indexed.
                pub fn index_from_object(object: *const Object) ?IndexPrefix {
                    if (is_derived) {
                        return if (get(object)) |value|
                            as_prefix(value)
                        else
                            null;
                    }

                    const value: IndexPrefix = as_prefix(get(object).?);
                    if (!allow_zero and value == 0) {
                        return null;
                    }
                    return value;
                }

                /// Extracts the value of the index,
                /// either by accessing the field or by calling the derived index function.
                pub inline fn get(object: *const Object) ?Type {
                    if (is_derived) {
                        const function = @field(groove_options.derived, field_name);
                        return function(object);
                    }

                    return @field(object, field_name);
                }
            };
        }
    }.HelperType;

    const ObjectsCacheHelpers = struct {
        const tombstone_bit = 1 << (64 - 1);

        inline fn key_from_value(value: *const Object) PrimaryKey {
            return if (comptime _is_primary_key(.timestamp))
                value.timestamp & ~@as(u64, tombstone_bit)
            else
                @field(value, groove_options.primary_key);
        }

        inline fn hash(key: PrimaryKey) u64 {
            return stdx.hash_inline(key);
        }

        inline fn tombstone_from_key(a: PrimaryKey) Object {
            var obj: Object = undefined;
            if (comptime _is_primary_key(.timestamp)) {
                obj.timestamp = a;
            } else {
                @field(obj, groove_options.primary_key) = a;
                obj.timestamp = 0;
            }
            obj.timestamp |= tombstone_bit;
            return obj;
        }

        inline fn tombstone(a: *const Object) bool {
            return (a.timestamp & tombstone_bit) != 0;
        }
    };

    const _ObjectsCache: type = if (groove_options.objects_cache) CacheMapType(
        PrimaryKey,
        Object,
        ObjectsCacheHelpers.key_from_value,
        ObjectsCacheHelpers.hash,
        ObjectsCacheHelpers.tombstone_from_key,
        ObjectsCacheHelpers.tombstone,
    ) else void;

    return struct {
        const Groove = @This();

        pub const ObjectTree = _ObjectTree;
        pub const IndexTrees = _IndexTrees;
        pub const ObjectsCache = _ObjectsCache;
        pub const config = groove_options;

        pub const is_primary_key = _is_primary_key;

        /// Helper function for interacting with an Index field type.
        pub const IndexHelperType = _IndexHelperType;

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        /// Union containing all of the Object's unique keys.
        pub const UniqueKey: type = T: {
            const Tag = stdx.EnumType(.{"timestamp"} ++ groove_options.unique_keys);
            break :T stdx.EnumUnionType(
                Tag,
                struct {
                    fn Type(comptime variant: Tag) type {
                        const IndexHelper = IndexHelperType(@tagName(variant));
                        return IndexHelper.Type;
                    }
                }.Type,
            );
        };

        const PrefetchKeys = std.ArrayHashMapUnmanaged(UniqueKey, union(enum) {
            /// The key was enqueued for prefetching.
            /// Invariant: Only possible during `prefetch_enqueue`.
            enqueued,
            /// The key must be prefetched from storage.
            /// Invariant: Only possible before the prefetch callback runs.
            prefetching: struct {
                level: u8,
                /// When prefetching by unique keys, the Object's timestamp might
                /// already be known, so we don't need to scan the secondary tree.
                /// Invariant: when prefetching by `timestamp`, it is never null.
                timestamp_hint: ?u64,
            },
            /// The key enqueued for prefetching was found, and the corresponding
            /// object with this primary key is present in the object cache.
            found: PrimaryKey,
            /// The key enqueued for prefetching exists, but there is no corresponding
            /// object associated with it.
            /// Invariant: this is only possible when prefetching by the primary key on
            /// Grooves that allow orphaned keys.
            found_orphaned,
            /// The key enqueued for prefetching was not found.
            not_found,
        }, struct {
            pub fn hash(ctx: @This(), key: UniqueKey) u32 {
                _ = ctx;

                const hash_u64: u64 = stdx.hash_inline(switch (key) {
                    inline else => |value, tag| value: {
                        if (groove_options.unique_keys.len == 0) {
                            comptime assert(tag == .timestamp);
                            break :value value;
                        }

                        // The hash takes the tag as the most significant bytes
                        // and the value as the least significant bytes.
                        // Ensures it is the smallest aligned integer possible.
                        const Tag = std.meta.Tag(std.meta.Tag(UniqueKey));
                        comptime assert(@sizeOf(Tag) > 0);

                        const Value = @TypeOf(value);
                        const size_bytes = comptime std.mem.alignForward(
                            usize,
                            @sizeOf(Tag) + @sizeOf(Value),
                            @alignOf(Value),
                        );
                        comptime assert(size_bytes >= @sizeOf(Value) + @sizeOf(Tag));

                        const Int = std.meta.Int(.unsigned, size_bytes * 8);
                        comptime assert(stdx.no_padding(Int));

                        const tag_shift = @bitSizeOf(Int) - @bitSizeOf(Tag);
                        break :value (@as(Int, @intFromEnum(tag)) << tag_shift) |
                            @as(Int, value);
                    },
                });

                // Combines both halves of the `u64` hash into `u32`
                // so the upper bits also contribute.
                return @as(u32, @truncate(hash_u64)) ^
                    @as(u32, @truncate(hash_u64 >> 32));
            }

            pub fn eql(ctx: @This(), a: UniqueKey, b: UniqueKey, b_index: usize) bool {
                _ = b_index;
                _ = ctx;
                switch (a) {
                    inline else => |value, tag| {
                        // Disable runtime safety: we do not have to check the tag of `b` when
                        // accessing the field, since it has already been checked against `a`.
                        @setRuntimeSafety(builtin.mode != .ReleaseSafe);
                        return b == tag and
                            value == @field(b, @tagName(tag));
                    },
                }
            }
        }, store_hash: {
            // Do not store the hash, as the `eql` function is very cheap.
            break :store_hash false;
        });

        const ObjectCacheResult = if (groove_options.primary_key_orphaned) union(enum) {
            found_object: Object,
            found_orphaned,
            not_found,
        } else union(enum) {
            found_object: Object,
            not_found,
        };

        pub const ScanBuilder = if (has_scan) ScanBuilderType(Groove, Storage) else void;

        grid: *Grid,
        objects: ObjectTree,
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

        scan_builder: ScanBuilder,

        pub const IndexTreeOptions = _IndexTreeOptions;

        pub const Options = struct {
            /// The maximum number of objects that might be prefetched and not modified by a batch.
            prefetch_entries_for_read_max: u32,
            /// The maximum number of objects that might be prefetched and then modified by a batch.
            prefetch_entries_for_update_max: u32,
            cache_entries_max: u32,

            tree_options_object: ObjectTree.Options,
            tree_options_index: IndexTreeOptions,
        };

        pub fn init(
            groove: *Groove,
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            radix_buffer: *ScratchMemory,
            options: Options,
        ) !void {
            assert(options.tree_options_object.batch_value_count_limit *
                constants.lsm_compaction_ops <= ObjectTree.Table.value_count_max);
            assert(radix_buffer.state == .free);

            groove.* = .{
                .grid = grid,

                .objects = undefined,
                .indexes = undefined,
                .prefetch_keys = undefined,
                .objects_cache = if (ObjectsCache != void) undefined else {},
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
                radix_buffer,
                .{
                    .id = @field(groove_options.ids, "timestamp"),
                    .name = ObjectTree.tree_name(),
                },
                options.tree_options_object,
            );
            errdefer groove.objects.deinit(allocator);

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
                    radix_buffer,
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

            if (has_scan) try groove.scan_builder.init(allocator);
            errdefer if (has_scan) groove.scan_builder.deinit(allocator);
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);

            groove.prefetch_keys.deinit(allocator);

            if (ObjectsCache != void) groove.objects_cache.deinit(allocator);
            if (has_scan) groove.scan_builder.deinit(allocator);

            groove.* = undefined;
        }

        pub fn reset(groove: *Groove) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).reset();
            }
            groove.objects.reset();

            groove.prefetch_keys.clearRetainingCapacity();

            if (ObjectsCache != void) groove.objects_cache.reset();

            if (has_scan) groove.scan_builder.reset();

            groove.* = .{
                .grid = groove.grid,
                .objects = groove.objects,
                .indexes = groove.indexes,
                .prefetch_keys = groove.prefetch_keys,
                .prefetch_snapshot = null,
                .objects_cache = groove.objects_cache,
                .scan_builder = groove.scan_builder,
            };
        }

        /// Gets the object from the object cache.
        pub fn get(groove: *const Groove, key: PrimaryKey) ObjectCacheResult {
            switch (groove.objects_cache.get_or_tombstone(key)) {
                .found => |object| {
                    // Orphaned primary key.
                    if (object.timestamp == 0) {
                        if (!groove_options.primary_key_orphaned) unreachable;
                        if (is_primary_key(.timestamp)) unreachable;
                        comptime assert(groove_options.primary_key_orphaned);
                        comptime assert(!is_primary_key(.timestamp));

                        if (constants.verify) {
                            const prefetch_key = groove.prefetch_keys.get(@unionInit(
                                UniqueKey,
                                groove_options.primary_key,
                                key,
                            ));
                            assert(prefetch_key == null or
                                prefetch_key.? == .found_orphaned or
                                prefetch_key.? == .not_found // not found and then failed.
                            );
                        }

                        return .found_orphaned;
                    }
                    assert(TimestampRange.valid(object.timestamp));

                    if (constants.verify) {
                        const prefetch_key = groove.prefetch_keys.get(@unionInit(
                            UniqueKey,
                            groove_options.primary_key,
                            key,
                        ));
                        assert(prefetch_key == null or
                            prefetch_key.? == .found or
                            prefetch_key.? == .not_found // not found and then inserted.
                        );
                    }

                    return .{ .found_object = object.* };
                },
                .tombstone => {
                    if (constants.verify) {
                        const prefetch_key = groove.prefetch_keys.get(@unionInit(
                            UniqueKey,
                            groove_options.primary_key,
                            key,
                        ));
                        assert(prefetch_key == null or
                            prefetch_key.? == .not_found or
                            prefetch_key.? == .found // found and then deleted.
                        );
                    }

                    return .not_found;
                },
                .not_found => {
                    if (constants.verify) {
                        const prefetch_key = groove.prefetch_keys.get(
                            @unionInit(UniqueKey, groove_options.primary_key, key),
                        );
                        assert(prefetch_key == null or prefetch_key.? == .not_found);
                    }

                    return .not_found;
                },
            }
        }

        /// Indirect lookup by one of the unique keys.
        /// The key must have been passed to `prefetch_enqueue`.
        /// Use `get()` for direct lookups by the primary key.
        pub fn indirect_lookup(groove: *const Groove, key: UniqueKey) ?Object {
            comptime assert(groove_options.unique_keys.len > 0);
            assert(!is_primary_key(std.meta.activeTag(key)));

            const prefetch_status = groove.prefetch_keys.get(key);
            assert(prefetch_status != null);

            switch (prefetch_status.?) {
                .enqueued, .prefetching => unreachable,
                .found => |primary_key| {
                    const object: ?*Object = groove.objects_cache.get(primary_key);
                    assert(object != null);
                    assert(object.?.timestamp != 0);
                    switch (key) {
                        inline else => |value, field| {
                            const IndexHelper = IndexHelperType(@tagName(field));
                            assert(IndexHelper.get(object.?).? == value);
                        },
                    }

                    return object.?.*;
                },
                .not_found => return null,
                .found_orphaned => unreachable, // Only primary keys can be orphaned.
            }
        }

        /// Must be called directly before the state machine begins queuing ids for prefetch.
        pub fn prefetch_setup(groove: *Groove, snapshot_target: u64) void {
            assert(snapshot_target < snapshot_latest);

            groove.prefetch_snapshot = snapshot_target;
            groove.prefetch_keys.clearRetainingCapacity();
        }

        /// This must be called by the state machine for every lookup by unique keys.
        /// Prefetching by the primary key can skip the mutable table by directly checking the
        /// object cache. However, when prefetching by other unique keys, the first call to this
        /// function may trigger sorting of the mutable table, to enable searching.
        /// We tolerate duplicate unique keys enqueued by the state machine.
        pub fn prefetch_enqueue(
            groove: *Groove,
            key: UniqueKey,
        ) void {
            const entry = groove.prefetch_keys.getOrPutAssumeCapacity(key);
            // No need to check again if the key is already enqueued for prefetching.
            if (entry.found_existing) {
                assert(entry.value_ptr.* != .enqueued);
                return;
            }
            entry.value_ptr.* = .enqueued;
            defer assert(entry.value_ptr.* != .enqueued);

            const timestamp_hint: ?u64 = switch (key) {
                inline else => |value, field| timestamp: {
                    if (field == .timestamp) {
                        // Instead of asserting, we allow and ignore invalid timestamps,
                        // so the prefetch step does not need to verify the data's validity.
                        if (!TimestampRange.valid(value)) {
                            entry.value_ptr.* = .not_found;
                            return;
                        }

                        if (comptime is_primary_key(.timestamp)) {
                            if (groove.objects_cache.has(value)) {
                                entry.value_ptr.* = .{
                                    .found = value,
                                };
                                return;
                            }
                        }

                        if (!groove.objects.key_range_contains(
                            groove.prefetch_snapshot.?,
                            value,
                        )) {
                            entry.value_ptr.* = .not_found;
                            return;
                        }

                        break :timestamp value;
                    }
                    comptime assert(field != .timestamp);

                    // Instead of asserting, we allow and ignore zeroes keys (most likely zero),
                    // so the prefetch step does not need to verify the data's validity.
                    if (value == 0) {
                        entry.value_ptr.* = .not_found;
                        return;
                    }

                    if (comptime is_primary_key(field)) {
                        switch (groove.objects_cache.get_or_tombstone(value)) {
                            .found => |object| {
                                if (groove_options.primary_key_orphaned) {
                                    if (object.timestamp == 0) {
                                        entry.value_ptr.* = .found_orphaned;
                                        return;
                                    }
                                }
                                assert(TimestampRange.valid(object.timestamp));

                                entry.value_ptr.* = .{
                                    .found = value,
                                };
                                return;
                            },
                            .tombstone => {
                                // Tombstone found in the object cache, the key was deleted.
                                entry.value_ptr.* = .not_found;
                                return;
                            },
                            .not_found => {},
                        }
                    }

                    const Tree = @FieldType(IndexTrees, @tagName(field));
                    const tree: *Tree = &@field(groove.indexes, @tagName(field));
                    if (!tree.key_range_contains(groove.prefetch_snapshot.?, value)) {
                        entry.value_ptr.* = .not_found;
                        return;
                    }

                    if (comptime !is_primary_key(field)) {
                        // Lookup by the primary key skip the mutable
                        // table by checking the object cache.
                        // When searching by other unique keys, the mutable
                        // table needs to be sorted and binary-searched.
                        tree.table_mutable.sort();
                        if (tree.table_mutable.get(value)) |tree_value| {
                            if (tree_value.tombstone()) {
                                entry.value_ptr.* = .not_found;
                                return;
                            }

                            // Timestamp cannot be zero,
                            // as orphaned objects are only expected for primary keys.
                            assert(TimestampRange.valid(tree_value.timestamp));
                            assert(tree_value.field == value);
                            break :timestamp tree_value.timestamp;
                        }
                    }
                    break :timestamp null;
                },
            };

            if (timestamp_hint) |timestamp| {
                assert(TimestampRange.valid(timestamp));

                if (!is_primary_key(std.meta.activeTag(key))) {
                    if (comptime is_primary_key(.timestamp)) {
                        if (groove.objects_cache.has(timestamp)) {
                            entry.value_ptr.* = .{
                                .found = timestamp,
                            };
                            return;
                        }
                    } else {
                        // Lookup by the primary key skip the mutable
                        // table by checking the object cache.
                        // When searching by other unique keys, the mutable
                        // table needs to be sorted and binary-searched.
                        switch (groove.sort_and_search_table_mutable(key, timestamp)) {
                            .found => |primary_key| {
                                entry.value_ptr.* = .{
                                    .found = primary_key,
                                };
                                return;
                            },
                            .tombstone => {
                                entry.value_ptr.* = .not_found;
                                return;
                            },
                            .not_found => {},
                        }
                    }
                }

                // We can still use the timestamp hint if it is present in
                // the secondary tree's mutable table, but not in the object table.
                groove.prefetch_from_memory_by_timestamp(.{
                    .entry = entry,
                    .timestamp_hint = timestamp,
                });
                return;
            }
            assert(timestamp_hint == null);
            assert(key != .timestamp);

            // Not found in the mutable table.
            groove.prefetch_from_memory_by_unique_key(entry);
        }

        /// This function attempts to prefetch a value for the given unique key from the
        /// secondary tree table blocks in the grid cache.
        /// If found in the secondary tree, we attempt to prefetch a value for the timestamp.
        fn prefetch_from_memory_by_unique_key(
            groove: *Groove,
            entry: PrefetchKeys.GetOrPutResult,
        ) void {
            assert(!entry.found_existing);
            assert(entry.value_ptr.* == .enqueued);
            defer assert(entry.value_ptr.* != .enqueued);

            const key: UniqueKey = entry.key_ptr.*;

            switch (key) {
                inline else => |value, field| {
                    // Timestamp is handled by `prefetch_from_memory_by_timestamp`.
                    if (field == .timestamp) unreachable;
                    comptime assert(field != .timestamp);

                    const Tree = @FieldType(IndexTrees, @tagName(field));
                    const tree: *Tree = &@field(groove.indexes, @tagName(field));

                    switch (tree.lookup_from_levels_cache(
                        groove.prefetch_snapshot.?,
                        value,
                    )) {
                        .negative => {
                            entry.value_ptr.* = .not_found;
                        },
                        .positive => |tree_value| {
                            assert(!Tree.Value.tombstone(tree_value));

                            if (tree_value.timestamp == 0) {
                                if (!groove_options.primary_key_orphaned) unreachable;
                                if (!is_primary_key(field)) unreachable;
                                comptime assert(groove_options.primary_key_orphaned);
                                comptime assert(is_primary_key(field));

                                // Zeroed timestamp indicates the object is not present,
                                // and this id cannot be used anymore.
                                entry.value_ptr.* = .found_orphaned;
                                groove.insert_orphaned_object(value);
                                return;
                            }
                            assert(TimestampRange.valid(tree_value.timestamp));

                            if (comptime !is_primary_key(field)) {
                                // Lookup by the primary key skip the mutable
                                // table by checking the object cache.
                                // When searching by other unique keys, the mutable
                                // table needs to be sorted and binary-searched.
                                switch (groove.sort_and_search_table_mutable(
                                    key,
                                    tree_value.timestamp,
                                )) {
                                    .found => |primary_key| {
                                        entry.value_ptr.* = .{
                                            .found = primary_key,
                                        };
                                        return;
                                    },
                                    .tombstone => {
                                        entry.value_ptr.* = .not_found;
                                        return;
                                    },
                                    .not_found => {},
                                }
                            }

                            // We can still use the timestamp hint if it is present in
                            // the secondary tree's mutable table, but not in the object table.
                            groove.prefetch_from_memory_by_timestamp(.{
                                .entry = entry,
                                .timestamp_hint = tree_value.timestamp,
                            });
                        },
                        .possible => |level| {
                            entry.value_ptr.* = .{
                                .prefetching = .{
                                    .level = level,
                                    .timestamp_hint = null,
                                },
                            };
                        },
                    }
                },
            }
        }

        /// This function attempts to prefetch a value for the timestamp from the ObjectTree's
        /// table blocks in the grid cache.
        fn prefetch_from_memory_by_timestamp(
            groove: *Groove,
            options: struct {
                entry: PrefetchKeys.GetOrPutResult,
                timestamp_hint: u64,
            },
        ) void {
            assert(!options.entry.found_existing);
            assert(options.entry.value_ptr.* == .enqueued);
            defer assert(options.entry.value_ptr.* != .enqueued);

            assert(TimestampRange.valid(options.timestamp_hint));

            const key: UniqueKey = options.entry.key_ptr.*;
            if (key == .timestamp) assert(key.timestamp == options.timestamp_hint);

            switch (groove.objects.lookup_from_levels_cache(
                groove.prefetch_snapshot.?,
                options.timestamp_hint,
            )) {
                .negative => {
                    options.entry.value_ptr.* = .not_found;
                },
                .positive => |object| {
                    assert(!ObjectTreeHelper.tombstone(object));
                    assert(object.timestamp == options.timestamp_hint);

                    switch (key) {
                        inline else => |value, field| {
                            const IndexHelper = IndexHelperType(@tagName(field));
                            assert(IndexHelper.get(object).? == value);
                        },
                    }
                    groove.objects_cache.upsert(object);
                    options.entry.value_ptr.* = .{
                        .found = @field(object, groove_options.primary_key),
                    };
                },
                .possible => |level| {
                    options.entry.value_ptr.* = .{
                        .prefetching = .{
                            .level = level,
                            .timestamp_hint = options.timestamp_hint,
                        },
                    };
                },
            }
        }

        /// Performs an indirect lookup by timestamp from the mutable table.
        /// This path is only applicable when searching by unique keys
        /// other than the primary key.
        /// Invariant: The object cache always contains the primary keys
        /// present in the mutable table, so it must be found by
        /// `objects_cache.get()`.
        fn sort_and_search_table_mutable(
            groove: *Groove,
            key: UniqueKey,
            timestamp: u64,
        ) union(enum) {
            found: PrimaryKey,
            tombstone,
            not_found,
        } {
            assert(!is_primary_key(std.meta.activeTag(key)));

            // The mutable table needs to be sorted to enable searching.
            // If not found, the immutable table and other LSM levels will be searched.
            groove.objects.table_mutable.sort();
            if (groove.objects.table_mutable.get(
                timestamp,
            )) |object| {
                if (ObjectTreeHelper.tombstone(object)) {
                    assert(ObjectTreeHelper.key_from_value(object) == timestamp);
                    return .tombstone;
                }
                assert(!ObjectTreeHelper.tombstone(object));
                assert(object.timestamp == timestamp);

                switch (key) {
                    inline else => |value, field| {
                        const IndexHelper = IndexHelperType(@tagName(field));
                        assert(IndexHelper.get(object).? == value);
                    },
                }

                const primary_key: PrimaryKey = @field(
                    object,
                    groove_options.primary_key,
                );
                assert(groove.objects_cache.has(primary_key));
                return .{ .found = primary_key };
            }

            return .not_found;
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
                if (constants.verify) {
                    // Validate that all keys have been prefetched
                    // and are consistent with the object cache.
                    var it = context.groove.prefetch_keys.iterator();
                    while (it.next()) |entry| {
                        switch (entry.value_ptr.*) {
                            .enqueued, .prefetching => unreachable,
                            .found => |primary_key| {
                                assert(primary_key != 0);
                                if (is_primary_key(std.meta.activeTag(entry.key_ptr.*))) {
                                    const value: PrimaryKey = @field(
                                        entry.key_ptr.*,
                                        groove_options.primary_key,
                                    );
                                    assert(primary_key == value);
                                }

                                const object: ?*Object = context.groove.objects_cache.get(
                                    primary_key,
                                );
                                assert(object != null);
                                assert(object.?.timestamp != 0);
                                switch (entry.key_ptr.*) {
                                    inline else => |value, field| {
                                        const IndexHelper = IndexHelperType(@tagName(field));
                                        assert(IndexHelper.get(object.?).? == value);
                                    },
                                }
                            },
                            .not_found => {
                                if (is_primary_key(std.meta.activeTag(entry.key_ptr.*))) {
                                    const primary_key: PrimaryKey = @field(
                                        entry.key_ptr.*,
                                        groove_options.primary_key,
                                    );
                                    assert(!context.groove.objects_cache.has(primary_key));
                                }
                            },
                            .found_orphaned => {
                                assert(groove_options.primary_key_orphaned);
                                assert(is_primary_key(std.meta.activeTag(entry.key_ptr.*)));

                                const primary_key: PrimaryKey = @field(
                                    entry.key_ptr.*,
                                    groove_options.primary_key,
                                );
                                const object: ?*Object = context.groove.objects_cache.get(
                                    primary_key,
                                );
                                assert(object != null);
                                assert(object.?.timestamp == 0);
                            },
                        }
                    }
                }
                context.groove.grid.trace.stop(.{
                    .lookup = .{ .tree = @enumFromInt(context.groove.objects.config.id) },
                });

                context.callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            index: u8,
            context: *PrefetchContext,
            lookup: LookupContext = .null,
            current: ?PrefetchKeys.Entry = null,

            const LookupContext = T: {
                const Tag = stdx.EnumType(.{ "null", "object" } ++ groove_options.unique_keys);
                break :T stdx.EnumUnionType(
                    Tag,
                    struct {
                        fn Type(comptime variant: Tag) type {
                            if (variant == .null) return void;
                            if (variant == .object) return ObjectTree.LookupContext;
                            return @FieldType(IndexTrees, @tagName(variant)).LookupContext;
                        }
                    }.Type,
                );
            };

            const Field = std.meta.FieldEnum(LookupContext);
            fn FieldType(comptime field: Field) type {
                return @FieldType(LookupContext, @tagName(field));
            }

            inline fn worker_from_completion(
                comptime field: Field,
                completion: *FieldType(field),
            ) *PrefetchWorker {
                const lookup: *LookupContext = @fieldParentPtr(@tagName(field), completion);
                assert(lookup.* ==
                    comptime std.enums.nameCast(std.meta.Tag(LookupContext), field));

                return @fieldParentPtr("lookup", lookup);
            }

            inline fn lookup_context(
                self: *PrefetchWorker,
                comptime field: Field,
            ) *FieldType(field) {
                assert(self.lookup == .null);
                self.lookup = @unionInit(
                    LookupContext,
                    @tagName(field),
                    undefined,
                );
                return &@field(self.lookup, @tagName(field));
            }

            fn lookup_start_next(worker: *PrefetchWorker) void {
                assert(worker.current == null);
                worker.current = prefetch_entry: {
                    while (worker.context.key_iterator.next()) |entry| {
                        switch (entry.value_ptr.*) {
                            .enqueued => unreachable,
                            .prefetching => break :prefetch_entry entry,
                            .found, .not_found, .found_orphaned => continue, // Already prefetched.
                        }
                    }

                    worker.context.groove.grid.trace.stop(
                        .{ .lookup_worker = .{
                            .index = worker.index,
                            .tree = @enumFromInt(worker.context.groove.objects.config.id),
                        } },
                    );
                    worker.context.worker_finished();
                    return;
                };
                assert(worker.current.?.value_ptr.* == .prefetching);
                // prefetch_enqueue() ensures that the tree's cache is checked before queueing the
                // object for prefetching. If not in the LSM tree's cache, the object must be read
                // from disk and added to the auxiliary prefetch_objects hash map.
                if (worker.current.?.value_ptr.prefetching.timestamp_hint) |timestamp| {
                    if (worker.current.?.key_ptr.* == .timestamp) {
                        assert(worker.current.?.key_ptr.timestamp == timestamp);
                    }
                    worker.context.groove.objects.lookup_from_levels_storage(.{
                        .callback = lookup_object_callback,
                        .context = worker.lookup_context(.object),
                        .snapshot = worker.context.snapshot,
                        .key = timestamp,
                        .level_min = worker.current.?.value_ptr.prefetching.level,
                    });
                    return;
                }
                assert(worker.current.?.value_ptr.prefetching.timestamp_hint == null);

                // The code below is specific to handling unique indexes,
                // it does not apply to grooves with only the `timestamp` as PrefetchKey.
                if (groove_options.unique_keys.len == 0) unreachable;
                comptime assert(groove_options.unique_keys.len > 0);

                switch (worker.current.?.key_ptr.*) {
                    // Timestamp is handled by the `timestamp_hint` branch above.
                    .timestamp => unreachable,
                    inline else => |value, field| {
                        const Tree = @FieldType(IndexTrees, @tagName(field));
                        const callback = LookupByUniqueKeyCallbackType(Tree, field);

                        const tree: *Tree = &@field(
                            worker.context.groove.indexes,
                            @tagName(field),
                        );
                        tree.lookup_from_levels_storage(.{
                            .callback = callback,
                            .context = worker.lookup_context(comptime std.enums.nameCast(
                                Field,
                                @tagName(field),
                            )),
                            .snapshot = worker.context.snapshot,
                            .key = value,
                            .level_min = worker.current.?.value_ptr.prefetching.level,
                        });
                    },
                }
            }

            fn LookupByUniqueKeyCallbackType(
                comptime Tree: type,
                comptime field: std.meta.Tag(UniqueKey),
            ) fn (*Tree.LookupContext, ?*const Tree.Value) void {
                return struct {
                    fn callback(
                        completion: *Tree.LookupContext,
                        result: ?*const Tree.Value,
                    ) void {
                        const worker: *PrefetchWorker = worker_from_completion(
                            comptime std.enums.nameCast(Field, @tagName(field)),
                            completion,
                        );
                        assert(worker.current != null);
                        assert(worker.lookup ==
                            comptime std.enums.nameCast(std.meta.Tag(LookupContext), field));

                        worker.lookup = .null;

                        const entry = worker.current.?;
                        assert(entry.key_ptr.* != .timestamp);
                        assert(entry.value_ptr.* == .prefetching);

                        const tree_value = result orelse {
                            entry.value_ptr.* = .not_found;
                            worker.current = null;
                            worker.lookup_start_next();
                            return;
                        };

                        // Zeroed timestamp indicates the object is not present,
                        // and this id cannot be used anymore.
                        // Only primary keys can be orphaned.
                        if (tree_value.timestamp == 0) {
                            if (!groove_options.primary_key_orphaned) unreachable;
                            if (!is_primary_key(field)) unreachable;
                            comptime assert(groove_options.primary_key_orphaned);
                            comptime assert(is_primary_key(field));

                            worker.context.groove.insert_orphaned_object(tree_value.field);
                            entry.value_ptr.* = .found_orphaned;

                            worker.current = null;
                            worker.lookup_start_next();
                            return;
                        }

                        if (tree_value.tombstone()) {
                            entry.value_ptr.* = .not_found;

                            worker.current = null;
                            worker.lookup_start_next();
                            return;
                        }
                        assert(!tree_value.tombstone());
                        assert(TimestampRange.valid(tree_value.timestamp));

                        if (!is_primary_key(std.meta.activeTag(entry.key_ptr.*))) {
                            // Lookup by the primary key skip the mutable
                            // table by checking the object cache.
                            // When searching by other unique keys, the mutable
                            // table needs to be sorted and binary-searched.
                            switch (worker.context.groove.sort_and_search_table_mutable(
                                entry.key_ptr.*,
                                tree_value.timestamp,
                            )) {
                                .found => |primary_key| {
                                    entry.value_ptr.* = .{
                                        .found = primary_key,
                                    };

                                    worker.current = null;
                                    worker.lookup_start_next();
                                    return;
                                },
                                .tombstone => {
                                    entry.value_ptr.* = .not_found;

                                    worker.current = null;
                                    worker.lookup_start_next();
                                    return;
                                },
                                .not_found => {},
                            }
                        }

                        assert(worker.current.?.value_ptr.* == .prefetching);
                        worker.lookup_by_timestamp(tree_value.timestamp);
                    }
                }.callback;
            }

            fn lookup_by_timestamp(worker: *PrefetchWorker, timestamp: u64) void {
                assert(TimestampRange.valid(timestamp));
                assert(worker.current != null);
                assert(worker.current.?.value_ptr.* == .prefetching);

                switch (worker.context.groove.objects.lookup_from_levels_cache(
                    worker.context.snapshot,
                    timestamp,
                )) {
                    .negative => {
                        lookup_object_callback(worker.lookup_context(.object), null);
                    },
                    .positive => |value| {
                        assert(!ObjectTree.Table.tombstone(value));
                        assert(value.timestamp == timestamp);

                        lookup_object_callback(worker.lookup_context(.object), value);
                    },
                    .possible => |level_min| {
                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup_context(.object),
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
                const worker: *PrefetchWorker = worker_from_completion(.object, completion);
                assert(worker.lookup == .object);
                worker.lookup = .null;

                assert(worker.current != null);
                assert(worker.current.?.value_ptr.* == .prefetching);

                const entry = worker.current.?;
                worker.current = null;

                if (result) |object| {
                    assert(!ObjectTreeHelper.tombstone(object));
                    switch (entry.key_ptr.*) {
                        inline else => |value, field| {
                            const IndexHelper = IndexHelperType(@tagName(field));
                            assert(IndexHelper.get(object).? == value);
                        },
                    }
                    worker.context.groove.objects_cache.upsert(object);
                    entry.value_ptr.* = .{
                        .found = @field(object, groove_options.primary_key),
                    };
                } else {
                    entry.value_ptr.* = .not_found;
                }

                worker.lookup_start_next();
            }
        };

        /// Insert the value into the objects tree and associated index trees. It's up to the
        /// caller to ensure it doesn't already exist.
        pub fn insert(groove: *Groove, object: *const Object) void {
            assert(TimestampRange.valid(object.timestamp));

            if (ObjectsCache != void) {
                const primary_key = @field(object, groove_options.primary_key);
                assert(!groove.objects_cache.has(primary_key));
                groove.objects_cache.upsert(object);
            }

            groove.objects.put(object);
            groove.objects.key_range_update(object.timestamp);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const IndexHelper = IndexHelperType(field.name);
                if (IndexHelper.index_from_object(object)) |value| {
                    const Tree = field.type;
                    const tree: *Tree = &@field(groove.indexes, field.name);
                    tree.put(&.{
                        .timestamp = object.timestamp,
                        .field = value,
                    });

                    if (IndexHelper.is_unique_key) {
                        tree.key_range_update(value);
                    }
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
                const primary_key = @field(old, groove_options.primary_key);
                const old_from_cache = groove.objects_cache.get(primary_key).?;
                assert(stdx.equal_bytes(Object, old_from_cache, old));
            }

            // Sanity check to ensure the caller didn't accidentally pass in an alias.
            assert(new != old);

            assert(old.timestamp == new.timestamp);
            assert(TimestampRange.valid(new.timestamp));

            // We assume the caller will pass in an object that has changes.
            // Unlike the index trees, the new and old values in the object
            // tree share the same key.
            // Therefore put() is sufficient to overwrite the old value.
            {
                const tombstone = ObjectTreeHelper.tombstone;
                const key_from_value = ObjectTreeHelper.key_from_value;

                // Update the object tree entry if any of the fields (even ignored)
                // are different.
                assert(!stdx.equal_bytes(Object, old, new));
                assert(key_from_value(old) == key_from_value(new));
                assert(!tombstone(old) and !tombstone(new));
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const IndexHelper = IndexHelperType(field.name);
                const old_index = IndexHelper.index_from_object(old);
                const new_index = IndexHelper.index_from_object(new);

                if (IndexHelper.is_unique_key) {
                    // Invariant: Unique keys cannot change.
                    assert(old_index == new_index);
                    continue;
                }
                comptime assert(!IndexHelper.is_unique_key);
                comptime maybe(IndexHelper.is_derived);

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

        pub fn remove(groove: *Groove, key: PrimaryKey) void {
            assert(ObjectsCache != void);

            // TODO: Nothing currently calls or tests this method.
            // The forest fuzzer should be extended to cover it.
            comptime assert(constants.verify);

            // The object must have been prefetched beforehand,
            // to ensure we only delete existing keys.
            const object: *const Object = groove.objects_cache.get(key).?;
            assert(TimestampRange.valid(object.timestamp));

            // TODO: should update the timestamp range,
            // see `key_range_update`.
            groove.objects.remove(object);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const IndexHelper = IndexHelperType(field.name);
                if (IndexHelper.index_from_object(object)) |value| {
                    const IndexTree = @FieldType(IndexTrees, field.name);
                    const tree: *IndexTree = &@field(groove.indexes, field.name);
                    tree.remove(&.{
                        .timestamp = object.timestamp,
                        .field = value,
                    });

                    if (IndexHelper.is_unique_key) {
                        // TODO: should update the unique keys range,
                        // see `key_range_update`.
                    }
                }
            }

            // Remove from the cache last: `object` is a pointer into the cache,
            // so removing it first would invalidate the pointer used above.
            groove.objects_cache.remove(key);
        }

        /// Insert a primary key associated with no object.
        /// It's up to the caller to ensure it doesn't already exist.
        pub fn insert_orphaned_primary_key(groove: *Groove, key: PrimaryKey) void {
            comptime assert(groove_options.primary_key_orphaned);
            comptime assert(!is_primary_key(.timestamp));

            assert(key != 0);
            assert(key != std.math.maxInt(PrimaryKey));

            const tree_key = &@field(groove.indexes, groove_options.primary_key);
            // We should not insert an orphaned `id` inside a scope.
            assert(!groove.objects_cache.scope_is_active);
            assert(!groove.objects_cache.has(key));
            assert(tree_key.active_scope == null);

            tree_key.put(&.{ .field = key, .timestamp = 0 });
            tree_key.key_range_update(key);

            groove.insert_orphaned_object(key);
        }

        /// We need to "tag" the object cache with a zeroed object,
        /// otherwise orphaned keys living in the mutable table would not
        /// be findable, or worse, we would have to sort the mutable table
        /// to search them, discarding many negative lookup optimizations.
        fn insert_orphaned_object(groove: *Groove, key: PrimaryKey) void {
            comptime assert(groove_options.primary_key_orphaned);
            comptime assert(!is_primary_key(.timestamp));

            assert(key != 0);
            assert(key != std.math.maxInt(PrimaryKey));

            var orphaned: Object = std.mem.zeroInit(Object, .{});
            @field(orphaned, groove_options.primary_key) = key;
            groove.objects_cache.upsert(&orphaned);
        }

        pub fn remove_orphaned_primary_key(groove: *Groove, id: u128) void {
            comptime assert(groove_options.primary_key_orphaned);
            comptime assert(!is_primary_key(.timestamp));

            // TODO: Nothing currently calls or tests this method. The forest fuzzer should be
            // extended to cover it.
            comptime assert(false);

            _ = groove;
            _ = id;
        }

        pub fn scope_open(groove: *Groove) void {
            if (ObjectsCache != void) groove.objects_cache.scope_open();
            groove.objects.scope_open();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_open();
            }
        }

        pub fn scope_close(groove: *Groove, mode: ScopeCloseMode) void {
            if (ObjectsCache != void) groove.objects_cache.scope_close(mode);
            groove.objects.scope_close(mode);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_close(mode);
            }
        }

        pub fn compact(groove: *Groove, op: u64) void {
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

                const Trace = @TypeOf(groove.grid.trace.*);
                const GrooveMetric = @FieldType(
                    @FieldType(Trace.EventMetric, "lsm_object_cache_entries"),
                    "groove",
                );
                const maybe_groove_metric = comptime std.meta.stringToEnum(
                    GrooveMetric,
                    ObjectTree.tree_name(),
                );

                if (comptime maybe_groove_metric) |groove_metric| {
                    groove.grid.trace.gauge(
                        .{ .lsm_object_cache_entries = .{
                            .groove = groove_metric,
                        } },
                        groove.objects_cache.cache_entries(),
                    );
                    groove.grid.trace.gauge(
                        .{ .lsm_object_cache_entries_max = .{
                            .groove = groove_metric,
                        } },
                        groove.objects_cache.cache_entries_max(),
                    );
                }
            }
        }

        pub fn open_commence(groove: *Groove, manifest_log: *ManifestLog) void {
            groove.objects.open_commence(manifest_log);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_commence(manifest_log);
            }
        }

        pub fn open_complete(groove: *Groove) void {
            groove.objects.open_complete();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_complete();
            }
        }

        pub fn assert_between_bars(groove: *const Groove) void {
            groove.objects.assert_between_bars();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).assert_between_bars();
            }
        }
    };
}
