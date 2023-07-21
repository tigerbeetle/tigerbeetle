//! Iterate over every table index block address/checksum in the forest.
//!
//! The underlying level iterator is stable across ManifestLevel mutation and
//! Forest.reset()/Forest.open(). (This is necessary for the scrubber, which is long-running).
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

pub fn ForestTableIteratorType(comptime Forest: type) {
    // List of every TreeType(…) in the Forest.
    const tree_types = types: {
        var types: []const type = &[_]type{};
        for (std.meta.fields(Forest.Grooves)) |groove_field| {
            const Groove = groove_field.field_type;
            types = types ++ &[_]type{ Groove.ObjectTree };
            if (Groove.IdTree != void) {
                types = types ++ &[_]type{ Groove.IdTree };
            }
            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                types = types ++ &[_]type{ tree_field.field_type };
            }
        }
        break :types types;
    };

    // struct { (Tree.name) → TreeTableIteratorType(Tree) }
    const TreeTableIterators = blk: {
        const StructField = std.builtin.Type.StructField;
        var fields: []const StructField = &[_]StructField{};
        for (tree_types) |Tree| {
            fields = fields ++ &[_]StructField{.{
                .name = Tree.name,
                .field_type = TreeTableIteratorType(Tree),
                .default_value = .{},
                .is_comptime = false,
                .alignment = @alignOf(TreeTableIteratorType(Tree)),
            }};
        }

        break :blk @Type(.{ .Struct = .{
            .layout = .Auto,
            .fields = fields,
            .decls = &.{},
            .is_tuple = false,
        } });
    };

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

        level: u8 = constants.lsm_levels,
        trees: TreeTableIterators = .{},

        pub fn next(iterator: *ForestTableIterator, forest: *Forest) ?TableInfo {
            // TODO or >=?
            while (iterator.level > 0) : (iterator.level -= 1) {
                inline for (std.meta.fields(Forest.Grooves)) |groove_field| {
                    const Groove = groove_field.field_type;
                    const groove = &@field(forest, groove_field.name);

                    if (iterator.next_from_tree(Groove.ObjectTree, &groove.object_tree)) |block| {
                        return block;
                    }

                    if (Groove.IdTree != void) {
                        if (iterator.next_from_tree(Groove.IdTree, &groove.id_tree)) |block| {
                            return block;
                        }
                    }

                    for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                        const IndexTree = tree_field.field_type;
                        if (iterator.next_from_tree(
                            IndexTree,
                            &@field(groove.index_trees, tree_field.name),
                        )) |block| {
                            return block;
                        }
                    }
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
            comptime Tree: type,
            tree: *const Tree,
        ) ?TableInfo {
            const tree_iterator = &@field(iterator.trees, Tree.name);
            if (tree_iterator.level < iterator.level) return null;

            if (tree_iterator.next(tree)) |table| {
                return .{
                    .checksum = table.checksum,
                    .address = table.address,
                    .flags = table.flags,
                    .snapshot_min = snapshot_min,
                    .snapshot_max = snapshot_max,
                };
            } else {
                assert(tree_iterator.done);
                assert(tree_iterator.level == 0);
                return null;
            }
        }
    };
};

/// Iterate over every table in a tree (i.e. every table in every ManifestLevel).
/// The iterator is stable across ManifestLevel mutation and Manifest.reset()/Manifest.open().
fn TreeTableIteratorType(comptime Tree: type) type {
    return struct {
        const TreeTableIterator = @This();

        level: u8 = constants.lsm_levels,
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

        fn next(iterator: *TreeTableIterator, tree: *const Tree) ?Tree.Manifest.TableInfo {
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
                const level = &tree.manifest.levels[iterator.level];

                const table_iterator = tables: {
                    if (iterator.position) |position| {
                        if (position.generation == level.generation) {
                            break :tables position.iterator;
                        } else {
                            // The ManifestLevel was mutated since the last iteration, so our
                            // cursor/ManifestLevel.Iterator is invalid.
                            break :tables iterator.tables.iterator_from_cursor(
                                level.tables.search(
                                    iterator.level,
                                    position.previous,
                                ) orelse continue,
                                .ascending,
                            );
                        }
                    } else {
                        break :tables iterator.tables.iterator_from_cursor(
                            level.tables.first(),
                            .ascending,
                        );
                    }
                };

                const table = table_iterator.next() orelse continue;

                iterator.position = .{
                    .generation = level.generation,
                    .previous = table,
                    .iterator = table_iterator,
                };

                return table;
            }
        }
    };
}
