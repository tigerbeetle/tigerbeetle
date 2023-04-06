//! SuperBlock.Manifest maintains an on-disk registry of all ManifestLog blocks.

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.superblock_manifest);
const mem = std.mem;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

// TODO Compute & use the upper bound of manifest blocks (per tree) to size the trailer zone.

/// SuperBlock.Manifest schema:
/// │ [manifest.count]u128 │ Tree id (for the owner of the ManifestLog)
/// │ [manifest.count]u128 │ ManifestLog block checksum
/// │ [manifest.count]u64  │ ManifestLog block address
/// Entries are ordered from oldest to newest.
pub const Manifest = struct {
    // This is a struct-of-arrays of `BlockReference`s.
    // Only the first `manifest.count` entries are valid.
    // Entries are ordered from oldest to newest.
    trees: []u128,
    checksums: []u128,
    addresses: []u64,

    count: u32,
    count_max: u32,

    /// A map from table address to the manifest block and entry that is the latest extent version.
    /// Used to determine whether a table should be dropped in a compaction.
    /// Shared by all trees and sized to accommodate all possible tables.
    tables: Tables,

    /// A set of block addresses that have free entries.
    /// Used to determine whether a block should be compacted.
    /// Note: Some of these block addresses may yet to be appended to the manifest through a flush.
    /// This enables us to track fragmentation even in unflushed blocks.
    compaction_set: std.AutoHashMapUnmanaged(u64, void),

    pub const Tables = std.AutoHashMapUnmanaged(TableExtentKey, TableExtent);

    pub const TableExtentKey = struct {
        tree_hash: u128,
        level: u7,
        table: u64,
    };

    pub const TableExtent = struct {
        block: u64, // ManifestLog block address.
        entry: u32, // Index within the ManifestLog Label/TableInfo arrays.
    };

    pub fn init(
        allocator: mem.Allocator,
        manifest_block_count_max: u32,
        // The maximum number of tables in a tree when fully loaded.
        // The same number is the limit for all tables across the forest since most trees are small.
        forest_table_count_max: u32,
    ) !Manifest {
        // TODO Assert relation between manifest_block_count_max and forest_table_count_max.

        const trees = try allocator.alloc(u128, manifest_block_count_max);
        errdefer allocator.free(trees);

        const checksums = try allocator.alloc(u128, manifest_block_count_max);
        errdefer allocator.free(checksums);

        const addresses = try allocator.alloc(u64, manifest_block_count_max);
        errdefer allocator.free(addresses);

        var tables = Tables{};
        try tables.ensureTotalCapacity(allocator, forest_table_count_max);
        errdefer tables.deinit(allocator);

        var compaction_set = std.AutoHashMapUnmanaged(u64, void){};
        try compaction_set.ensureTotalCapacity(allocator, manifest_block_count_max);
        errdefer compaction_set.deinit(allocator);

        mem.set(u128, trees, 0);
        mem.set(u128, checksums, 0);
        mem.set(u64, addresses, 0);

        return Manifest{
            .trees = trees,
            .checksums = checksums,
            .addresses = addresses,
            .count = 0,
            .count_max = manifest_block_count_max,
            .tables = tables,
            .compaction_set = compaction_set,
        };
    }

    pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
        allocator.free(manifest.trees);
        allocator.free(manifest.checksums);
        allocator.free(manifest.addresses);
        manifest.tables.deinit(allocator);
        manifest.compaction_set.deinit(allocator);
    }

    pub fn encode(manifest: *const Manifest, target: []align(@alignOf(u128)) u8) u64 {
        if (constants.verify) manifest.verify();

        assert(target.len > 0);
        assert(target.len % @sizeOf(u128) == 0);

        var size: u64 = 0;

        const trees = target[size..][0 .. manifest.count * @sizeOf(u128)];
        stdx.copy_disjoint(
            .exact,
            u128,
            mem.bytesAsSlice(u128, trees),
            manifest.trees[0..manifest.count],
        );
        size += trees.len;

        const checksums = target[size..][0 .. manifest.count * @sizeOf(u128)];
        stdx.copy_disjoint(
            .exact,
            u128,
            mem.bytesAsSlice(u128, checksums),
            manifest.checksums[0..manifest.count],
        );
        size += checksums.len;

        const addresses = target[size..][0 .. manifest.count * @sizeOf(u64)];
        stdx.copy_disjoint(
            .exact,
            u64,
            mem.bytesAsSlice(u64, addresses),
            manifest.addresses[0..manifest.count],
        );
        size += addresses.len;

        mem.set(u8, target[size..], 0);

        assert(@divExact(size, BlockReferenceSize) == manifest.count);

        return size;
    }

    pub fn decode(manifest: *Manifest, source: []align(@alignOf(u128)) const u8) void {
        assert(manifest.count == 0);
        assert(manifest.tables.count() == 0);
        assert(manifest.compaction_set.count() == 0);

        manifest.count = @intCast(u32, @divExact(source.len, BlockReferenceSize));
        assert(manifest.count <= manifest.count_max);

        var size: u64 = 0;

        const trees = source[size..][0 .. manifest.count * @sizeOf(u128)];
        stdx.copy_disjoint(
            .exact,
            u128,
            manifest.trees[0..manifest.count],
            mem.bytesAsSlice(u128, trees),
        );
        size += trees.len;

        const checksums = source[size..][0 .. manifest.count * @sizeOf(u128)];
        stdx.copy_disjoint(
            .exact,
            u128,
            manifest.checksums[0..manifest.count],
            mem.bytesAsSlice(u128, checksums),
        );
        size += checksums.len;

        const addresses = source[size..][0 .. manifest.count * @sizeOf(u64)];
        stdx.copy_disjoint(
            .exact,
            u64,
            manifest.addresses[0..manifest.count],
            mem.bytesAsSlice(u64, addresses),
        );
        size += addresses.len;

        assert(size == source.len);
        assert(@divExact(size, BlockReferenceSize) == manifest.count);

        mem.set(u128, manifest.trees[manifest.count..], 0);
        mem.set(u128, manifest.checksums[manifest.count..], 0);
        mem.set(u64, manifest.addresses[manifest.count..], 0);

        if (constants.verify) manifest.verify();
    }

    /// Addresses must be unique across all appends, or remove() must be called first.
    /// Warning: The caller is responsible for ensuring that concurrent tree compactions call
    /// append() in a deterministic order with respect to each other.
    pub fn append(manifest: *Manifest, tree: u128, checksum: u128, address: u64) void {
        assert(address > 0);
        assert(manifest.index_for_address(address) == null);

        if (manifest.count == manifest.count_max) {
            @panic("superblock manifest: out of space");
        }

        manifest.trees[manifest.count] = tree;
        manifest.checksums[manifest.count] = checksum;
        manifest.addresses[manifest.count] = address;
        manifest.count += 1;

        // A newly appended manifest block may already be queued for compaction.
        // For example, if a table is inserted and then removed before the block was flushed.
        // TODO Optimize ManifestLog.close_block() to compact blocks internally.

        log.debug("append: tree={} checksum={} address={} manifest.blocks={}/{}", .{
            tree,
            checksum,
            address,
            manifest.count,
            manifest.count_max,
        });

        if (constants.verify) {
            const index = manifest.index_for_address(address).?;
            assert(index == manifest.count - 1);
            manifest.verify_index_tree_checksum_address(index, tree, checksum, address);
            manifest.verify();
        }
    }

    pub fn remove(manifest: *Manifest, tree: u128, checksum: u128, address: u64) void {
        assert(address > 0);

        const index = manifest.index_for_address(address).?;
        assert(index < manifest.count);
        manifest.verify_index_tree_checksum_address(index, tree, checksum, address);

        const tail = manifest.count - (index + 1);
        stdx.copy_left(.inexact, u128, manifest.trees[index..], manifest.trees[index + 1 ..][0..tail]);
        stdx.copy_left(.inexact, u128, manifest.checksums[index..], manifest.checksums[index + 1 ..][0..tail]);
        stdx.copy_left(.inexact, u64, manifest.addresses[index..], manifest.addresses[index + 1 ..][0..tail]);
        manifest.count -= 1;

        manifest.trees[manifest.count] = 0;
        manifest.checksums[manifest.count] = 0;
        manifest.addresses[manifest.count] = 0;

        _ = manifest.compaction_set.remove(address);

        log.debug("remove: tree={} checksum={} address={} manifest.blocks={}/{}", .{
            tree,
            checksum,
            address,
            manifest.count,
            manifest.count_max,
        });

        if (constants.verify) {
            assert(manifest.index_for_address(address) == null);
            manifest.verify();
        }
    }

    fn index_for_address(manifest: *const Manifest, address: u64) ?u32 {
        assert(address > 0);

        var index: u32 = 0;
        while (index < manifest.count) : (index += 1) {
            if (manifest.addresses[index] == address) return index;
        }

        return null;
    }

    pub fn queue_for_compaction(manifest: *Manifest, address: u64) void {
        assert(address > 0);

        manifest.compaction_set.putAssumeCapacity(address, {});
    }

    pub fn queued_for_compaction(manifest: *const Manifest, address: u64) bool {
        assert(address > 0);

        return manifest.compaction_set.contains(address);
    }

    pub fn oldest_block_queued_for_compaction(
        manifest: *const Manifest,
        tree: u128,
    ) ?BlockReference {
        var index: u32 = 0;
        while (index < manifest.count) : (index += 1) {
            if (manifest.trees[index] != tree) continue;
            if (!manifest.queued_for_compaction(manifest.addresses[index])) continue;

            return BlockReference{
                .tree = manifest.trees[index],
                .checksum = manifest.checksums[index],
                .address = manifest.addresses[index],
            };
        }

        return null;
    }

    /// Inserts the table extent if it does not yet exist, and returns true.
    /// Otherwise, returns false.
    pub fn insert_table_extent(manifest: *Manifest, tree_hash: u128, level: u7, table: u64, block: u64, entry: u32) bool {
        assert(table > 0);
        assert(block > 0);

        var extent = manifest.tables.getOrPutAssumeCapacity(.{
            .tree_hash = tree_hash,
            .level = level,
            .table = table,
        });
        if (extent.found_existing) return false;

        extent.value_ptr.* = .{
            .block = block,
            .entry = entry,
        };

        return true;
    }

    /// Inserts or updates the table extent, and returns the previous block address if any.
    /// The table extent must be updated immediately when appending, without delay.
    /// Otherwise, ManifestLog.compact() may append a stale version over the latest.
    pub fn update_table_extent(manifest: *Manifest, tree_hash: u128, level: u7, table: u64, block: u64, entry: u32) ?u64 {
        assert(table > 0);
        assert(block > 0);

        var extent = manifest.tables.getOrPutAssumeCapacity(.{
            .tree_hash = tree_hash,
            .level = level,
            .table = table,
        });
        const previous_block = if (extent.found_existing) extent.value_ptr.block else null;

        extent.value_ptr.* = .{
            .block = block,
            .entry = entry,
        };

        return previous_block;
    }

    /// Removes the table extent if { block, entry } is the latest version, and returns true.
    /// Otherwise, returns false.
    pub fn remove_table_extent(manifest: *Manifest, tree_hash: u128, level: u7, table: u64, block: u64, entry: u32) bool {
        assert(table > 0);
        assert(block > 0);

        const extent = manifest.tables.getPtr(.{
            .tree_hash = tree_hash,
            .level = level,
            .table = table,
        }).?;
        if (extent.block == block and extent.entry == entry) {
            assert(manifest.tables.remove(.{
                .tree_hash = tree_hash,
                .level = level,
                .table = table,
            }));

            return true;
        } else {
            return false;
        }
    }

    /// Reference to a ManifestLog block.
    pub const BlockReference = struct {
        tree: u128,
        checksum: u128,
        address: u64,
    };

    /// The size of an encoded BlockReference on disk, not the size of the BlockReference struct.
    pub const BlockReferenceSize = @sizeOf(u128) + @sizeOf(u128) + @sizeOf(u64);

    pub const IteratorReverse = struct {
        manifest: *const Manifest,
        tree: u128,
        count: u32,

        pub fn next(it: *IteratorReverse) ?BlockReference {
            assert(it.count <= it.manifest.count);

            while (it.count > 0) {
                it.count -= 1;

                if (it.manifest.trees[it.count] == it.tree) {
                    assert(it.manifest.addresses[it.count] > 0);

                    return BlockReference{
                        .tree = it.manifest.trees[it.count],
                        .checksum = it.manifest.checksums[it.count],
                        .address = it.manifest.addresses[it.count],
                    };
                }
            }
            return null;
        }
    };

    /// Return all block references for a given tree in reverse order, latest-appended-first-out.
    /// Using a reverse iterator is an optimization to avoid redundant updates to tree manifests.
    pub fn iterator_reverse(manifest: *const Manifest, tree: u128) IteratorReverse {
        return IteratorReverse{
            .manifest = manifest,
            .tree = tree,
            .count = manifest.count,
        };
    }

    pub fn verify(manifest: *const Manifest) void {
        assert(manifest.count <= manifest.count_max);

        assert(manifest.trees.len == manifest.count_max);
        assert(manifest.checksums.len == manifest.count_max);
        assert(manifest.addresses.len == manifest.count_max);

        for (manifest.trees[manifest.count..]) |tree| assert(tree == 0);
        for (manifest.checksums[manifest.count..]) |checksum| assert(checksum == 0);
        for (manifest.addresses[manifest.count..]) |address| assert(address == 0);

        for (manifest.addresses[0..manifest.count]) |address| assert(address > 0);
    }

    pub fn verify_index_tree_checksum_address(
        manifest: *const Manifest,
        index: u32,
        tree: u128,
        checksum: u128,
        address: u64,
    ) void {
        assert(index < manifest.count);
        assert(address > 0);

        assert(manifest.trees[index] == tree);
        assert(manifest.checksums[index] == checksum);
        assert(manifest.addresses[index] == address);
    }
};

fn test_iterator_reverse(
    manifest: *Manifest,
    tree: u128,
    expect: []const Manifest.BlockReference,
) !void {
    const expectEqualSlices = std.testing.expectEqualSlices;

    var reverse: [3]Manifest.BlockReference = undefined;
    var reverse_count: usize = 0;

    var it = manifest.iterator_reverse(tree);
    while (it.next()) |block| {
        reverse[reverse_count] = block;
        reverse_count += 1;
    }

    try expectEqualSlices(Manifest.BlockReference, expect, reverse[0..reverse_count]);
}

fn test_codec(manifest: *Manifest) !void {
    const testing = std.testing;
    const expectEqual = testing.expectEqual;
    const expectEqualSlices = testing.expectEqualSlices;

    var target_a: [32]u128 = undefined;
    const size_a = manifest.encode(mem.sliceAsBytes(&target_a));
    try expectEqual(
        @as(u64, manifest.count * (@sizeOf(u128) + @sizeOf(u128) + @sizeOf(u64))),
        size_a,
    );

    // The decoded instance must match the original instance:
    var decoded = try Manifest.init(testing.allocator, manifest.count_max, 8);
    defer decoded.deinit(testing.allocator);

    decoded.decode(mem.sliceAsBytes(&target_a)[0..size_a]);

    try expectEqualSlices(u128, manifest.trees, decoded.trees);
    try expectEqualSlices(u128, manifest.checksums, decoded.checksums);
    try expectEqualSlices(u64, manifest.addresses, decoded.addresses);
    try expectEqual(manifest.count_max, decoded.count_max);
    try expectEqual(manifest.count, decoded.count);

    // The decoded instance must encode correctly:
    var target_b: [32]u128 = undefined;
    const size_b = decoded.encode(mem.sliceAsBytes(&target_b));
    try expectEqual(size_a, size_b);
    try expectEqualSlices(
        u8,
        mem.sliceAsBytes(&target_a)[0..size_a],
        mem.sliceAsBytes(&target_b)[0..size_b],
    );
}

test "SuperBlockManifest" {
    const testing = std.testing;
    const expectEqual = testing.expectEqual;
    const BlockReference = Manifest.BlockReference;

    var manifest = try Manifest.init(testing.allocator, 3, 8);
    defer manifest.deinit(testing.allocator);

    for (manifest.trees) |tree| try expectEqual(@as(u128, 0), tree);
    for (manifest.checksums) |checksum| try expectEqual(@as(u128, 0), checksum);
    for (manifest.addresses) |address| try expectEqual(@as(u64, 0), address);

    // The arguments to append()/remove() are: tree, checksum, address
    // These will be named variables and should be clear at call sites where they are used for real.
    manifest.append(1, 2, 3);
    try expectEqual(@as(?u32, 0), manifest.index_for_address(3));

    manifest.append(2, 3, 4);
    try expectEqual(@as(?u32, 1), manifest.index_for_address(4));

    manifest.append(1, 4, 5);
    try expectEqual(@as(?u32, 2), manifest.index_for_address(5));

    try expectEqual(@as(?BlockReference, null), manifest.oldest_block_queued_for_compaction(1));
    try expectEqual(@as(?BlockReference, null), manifest.oldest_block_queued_for_compaction(2));

    manifest.queue_for_compaction(3);
    try expectEqual(true, manifest.queued_for_compaction(3));
    try expectEqual(
        @as(?BlockReference, BlockReference{ .tree = 1, .checksum = 2, .address = 3 }),
        manifest.oldest_block_queued_for_compaction(1),
    );

    manifest.queue_for_compaction(4);
    try expectEqual(true, manifest.queued_for_compaction(4));
    try expectEqual(
        @as(?BlockReference, BlockReference{ .tree = 2, .checksum = 3, .address = 4 }),
        manifest.oldest_block_queued_for_compaction(2),
    );

    manifest.queue_for_compaction(5);
    try expectEqual(true, manifest.queued_for_compaction(5));
    try expectEqual(
        @as(?BlockReference, BlockReference{ .tree = 1, .checksum = 2, .address = 3 }),
        manifest.oldest_block_queued_for_compaction(1),
    );

    try test_iterator_reverse(
        &manifest,
        1,
        &[_]BlockReference{
            .{
                .tree = 1,
                .checksum = 4,
                .address = 5,
            },
            .{
                .tree = 1,
                .checksum = 2,
                .address = 3,
            },
        },
    );

    try test_iterator_reverse(
        &manifest,
        2,
        &[_]BlockReference{
            .{
                .tree = 2,
                .checksum = 3,
                .address = 4,
            },
        },
    );

    try test_codec(&manifest);

    manifest.remove(1, 2, 3);
    try expectEqual(false, manifest.queued_for_compaction(3));
    try expectEqual(@as(?u32, null), manifest.index_for_address(3));
    try expectEqual(@as(?u32, 0), manifest.index_for_address(4));
    try expectEqual(@as(?u32, 1), manifest.index_for_address(5));
    try expectEqual(
        @as(?BlockReference, BlockReference{ .tree = 1, .checksum = 4, .address = 5 }),
        manifest.oldest_block_queued_for_compaction(1),
    );

    try expectEqual(@as(u128, 0), manifest.trees[2]);
    try expectEqual(@as(u128, 0), manifest.checksums[2]);
    try expectEqual(@as(u64, 0), manifest.addresses[2]);

    manifest.append(1, 2, 3);
    try expectEqual(@as(?u32, 2), manifest.index_for_address(3));

    manifest.remove(1, 4, 5);
    try expectEqual(false, manifest.queued_for_compaction(5));
    try expectEqual(@as(?u32, null), manifest.index_for_address(5));
    try expectEqual(@as(?u32, 1), manifest.index_for_address(3));

    manifest.remove(2, 3, 4);
    try expectEqual(false, manifest.queued_for_compaction(4));
    try expectEqual(@as(?u32, null), manifest.index_for_address(4));
    try expectEqual(@as(?u32, 0), manifest.index_for_address(3));

    manifest.remove(1, 2, 3);
    try expectEqual(false, manifest.queued_for_compaction(3));
    try expectEqual(@as(?u32, null), manifest.index_for_address(3));
    try expectEqual(@as(?u32, null), manifest.index_for_address(4));
    try expectEqual(@as(?u32, null), manifest.index_for_address(5));

    for (manifest.trees) |tree| try expectEqual(@as(u128, 0), tree);
    for (manifest.checksums) |checksum| try expectEqual(@as(u128, 0), checksum);
    for (manifest.addresses) |address| try expectEqual(@as(u64, 0), address);

    try expectEqual(@as(usize, 0), manifest.compaction_set.count());

    try expectEqual(@as(u32, 0), manifest.count);
    try expectEqual(@as(u32, 3), manifest.count_max);

    try expectEqual(@as(?BlockReference, null), manifest.oldest_block_queued_for_compaction(1));
    try expectEqual(@as(?BlockReference, null), manifest.oldest_block_queued_for_compaction(2));
}
