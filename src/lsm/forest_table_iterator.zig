//! Iterate over every TableInfo in the forest.
//!
//! The underlying level iterator is stable across ManifestLevel mutation and
//! Forest.reset()/Forest.open(). (This is necessary for the scrubber, which is long-running).
//!
//! Stability invariants:
//! - Tables inserted after the iterator starts *may* be iterated.
//! - Tables inserted before the iterator starts *will* be iterated (unless they are removed).
//!
//! This iterator is conceptually simple, but it is a complex implementation due to the
//! metaprogramming necessary to generalize over the different concrete Tree types, and the
//! stability requirements.
//!
//! Pseudocode for this iterator:
//!
//!   for level in lsm_levels→0:
//!     for tree in forest.trees:
//!       for table in tree.manifest.levels[level]:
//!         yield table
//!
const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

pub fn ForestTableIteratorType(comptime Forest: type) type {
    // struct { (Tree.name) → TreeTableIteratorType(Tree) }
    const TreeTableIterators = iterator: {
        var tree_types: []const type = &[_]type{};
        var tree_names: []const []const u8 = &[_][]const u8{};
        for (std.meta.fields(Forest.Grooves)) |groove_field| {
            const Groove = groove_field.field_type;

            tree_types = tree_types ++ &[_]type{ Groove.ObjectTree };
            tree_names = tree_names ++ &[_][]const u8{ groove_field.name };

            if (Groove.IdTree != void) {
                tree_types = tree_types ++ &[_]type{ Groove.IdTree };
                tree_names = tree_names ++ &[_][]const u8{ groove_field.name ++ ".id" };
            }

            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                tree_types = tree_types ++ &[_]type{ tree_field.field_type };
                tree_names =
                    tree_names ++ &[_][]const u8{ groove_field.name ++ "." ++ tree_field.name };
            }
        }
        assert(tree_names.len == tree_types.len);

        const StructField = std.builtin.TypeInfo.StructField;
        var fields: []const StructField = &[_]StructField{};

        for (tree_types) |Tree, i| {
            // TODO(Zig) Inline these when compiler is fixed. (Doesn't work as of 0.9.1).
            const TreeTableIterator = TreeTableIteratorType(Tree);
            const iterator_default: TreeTableIterator = .{};
            fields = fields ++ &[_]StructField{.{
                .name = tree_names[i],
                .field_type = TreeTableIterator,
                .default_value = iterator_default,
                .is_comptime = false,
                .alignment = @alignOf(TreeTableIteratorType(Tree)),
            }};
        }

        break :iterator @Type(.{ .Struct = .{
            .layout = .Auto,
            .fields = fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    };

    //const TreeTableIterators = types: {
    //    const StructField = std.builtin.TypeInfo.StructField;
    //
    //    var fields: []const StructField = &[_]StructField{};
    //    for (std.meta.fields(Forest.Grooves)) |groove_field| {
    //        const Groove = groove_field.field_type;
    //        types = types ++ &[_]type{ Groove.ObjectTree };
    //        if (Groove.IdTree != void) {
    //            types = types ++ &[_]type{ Groove.IdTree };
    //        }
    //        for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
    //            types = types ++ &[_]type{ tree_field.field_type };
    //        }
    //    }
    //    break :types types;
    //};
    //
    //
    //// struct { (Tree.name) → TreeTableIteratorType(Tree) }
    //const TreeTableIterators = blk: {
    //
    //
    //
    //    const StructField = std.builtin.TypeInfo.StructField;
    //    var fields: []const StructField = &[_]StructField{};
    //    for (tree_types) |Tree, i| {
    //        fields = fields ++ &[_]StructField{.{
    //            .name = 
    //            .field_type = TreeTableIteratorType(Tree),
    //            .default_value = .{},
    //            .is_comptime = false,
    //            .alignment = @alignOf(TreeTableIteratorType(Tree)),
    //        }};
    //    }
    //
    //    break :blk @Type(.{ .Struct = .{
    //        .layout = .Auto,
    //        .fields = fields,
    //        .decls = &.{},
    //        .is_tuple = false,
    //    } });
    //};

    return struct {
        const ForestTableIterator = @This();

        /// This holds Manifest.TableInfo's data except for keys, so that it is not parameterized
        /// over the Tree/Table.
        pub const TableInfo = struct {
            checksum: u128,
            address: u64,
            flags: u64,
            snapshot_min: u64,
            snapshot_max: u64,// = math.maxInt(u64), TODO why did I write this?
        };

        level: ?u8 = constants.lsm_levels - 1,
        trees: TreeTableIterators = .{},

        pub fn next(iterator: *ForestTableIterator, forest: *Forest) ?TableInfo {
            while (iterator.level) |level| {
                inline for (std.meta.fields(Forest.Grooves)) |groove_field| {
                    const Groove = groove_field.field_type;
                    const groove = &@field(forest.grooves, groove_field.name);

                    if (iterator.next_from_tree(
                        groove_field.name,
                        Groove.ObjectTree,
                        &groove.objects,
                    )) |block| {
                        return block;
                    }

                    if (Groove.IdTree != void) {
                        if (iterator.next_from_tree(
                            groove_field.name ++ ".id",
                            Groove.IdTree,
                            &groove.ids,
                        )) |block| {
                            return block;
                        }
                    }

                    inline for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                        const IndexTree = tree_field.field_type;
                        if (iterator.next_from_tree(
                            groove_field.name ++ "." ++ tree_field.name,
                            IndexTree,
                            &@field(groove.indexes, tree_field.name),
                        )) |block| {
                            return block;
                        }
                    }
                }

                if (level == 0) {
                    iterator.level = null;
                } else {
                    iterator.level = level - 1;
                }
            }

            // Sanity-check, since all of this code generation is tricky to follow.
            inline for (std.meta.fields(TreeTableIterators)) |field| {
                const tree_iterator = @field(iterator.trees, field.name);
                assert(tree_iterator.done);
                assert(tree_iterator.level == 0);
            }
            return null;
        }

        fn next_from_tree(
            iterator: *ForestTableIterator,
            comptime iterator_field: []const u8,
            comptime Tree: type,
            tree: *const Tree,
        ) ?TableInfo {
            const tree_iterator = &@field(iterator.trees, iterator_field);
            if (tree_iterator.level < iterator.level.?) {
                //std.debug.print("{s}: A={}<{}\n", .{iterator_field, tree_iterator.level, iterator.level});
                return null;
            }

            if (tree_iterator.next(tree)) |table| {
                //std.debug.print("{s}: B={}<{}: {}\n", .{iterator_field, tree_iterator.level, iterator.level, table});
                return TableInfo{
                    .checksum = table.checksum,
                    .address = table.address,
                    .flags = table.flags,
                    .snapshot_min = table.snapshot_min,
                    .snapshot_max = table.snapshot_max,
                };
            } else {
                assert(tree_iterator.done);
                assert(tree_iterator.level == 0);
                return null;
            }
        }
    };
}

/// Iterate over every table in a tree (i.e. every table in every ManifestLevel).
/// The iterator is stable across ManifestLevel mutation and Manifest.reset()/Manifest.open().
/// TODO(Unified Manifest): If ManifestLevel isn't generic anymore, pass in the ManifestLevel so
/// this isn't generic either.
fn TreeTableIteratorType(comptime Tree: type) type {
    return struct {
        const TreeTableIterator = @This();

        level: u8 = constants.lsm_levels - 1,
        done: bool = false,

        position: ?struct {
            /// Corresponds to `ManifestLevel.generation`.
            /// Used to detect when a ManifestLevel is mutated.
            generation: u32,
            /// Used to recover the position in the manifest level after TableInfo inserts/removes.
            previous: Tree.Manifest.TableInfo,
            /// Only valid for the same level+generation that created it.
            iterator: Tree.Manifest.Level.Tables.Iterator,
        } = null,

        fn next(iterator: *TreeTableIterator, tree: *const Tree) ?*const Tree.Manifest.TableInfo {
            if (iterator.done) return null;

            while (true) : ({
                if (iterator.level > 0) {
                    iterator.level -= 1;
                    iterator.position = null;
                } else {
                    iterator.done = true;
                    return null;
                }
            }) {
                assert(tree.manifest.manifest_log.opened);

                const level: *const Tree.Manifest.Level = &tree.manifest.levels[iterator.level];

                var table_iterator = tables: {
                    if (iterator.position) |position| {
                        if (position.generation == level.generation) {
                            break :tables position.iterator;
                        } else {
                            // The ManifestLevel was mutated since the last iteration, so our
                            // cursor/ManifestLevel.Iterator is invalid.
                            break :tables level.tables.iterator_from_cursor(
                                level.tables.search(.{
                                    .key_max = position.previous.key_max,
                                    .snapshot_min = position.previous.snapshot_min,
                                }),
                                .ascending,
                            );
                        }
                    } else {
                        break :tables level.tables.iterator_from_cursor(
                            level.tables.first(),
                            .ascending,
                        );
                    }
                };

                const table = table_iterator.next() orelse continue;

                iterator.position = .{
                    .generation = level.generation,
                    .previous = table.*,
                    .iterator = table_iterator,
                };

                return table;
            }
        }
    };
}
