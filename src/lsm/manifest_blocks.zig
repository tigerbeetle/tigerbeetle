const std = @import("std");
const assert = std.debug.assert;
const log = std.log;
const mem = std.mem;

const config = @import("../config.zig");

pub const ManifestBlocks = struct {
    checksum: []u128,
    address: []u64,
    tree: []u8,

    count: u32,
    count_max: u32,

    pub fn init(allocator: mem.Allocator, count_max: u32) !ManifestBlocks {
        const checksum = try allocator.alloc(u128, count_max);
        errdefer allocator.free(checksum);

        const address = try allocator.alloc(u64, count_max);
        errdefer allocator.free(address);

        const tree = try allocator.alloc(u8, count_max);
        errdefer allocator.free(tree);

        mem.set(u128, checksum, 0);
        mem.set(u64, address, 0);
        mem.set(u8, tree, 0);

        return ManifestBlocks{
            .checksum = checksum,
            .address = address,
            .tree = tree,
            .count = 0,
            .count_max = count_max,
        };
    }

    pub fn deinit(blocks: *ManifestBlocks, allocator: mem.Allocator) void {
        allocator.free(blocks.checksum);
        allocator.free(blocks.address);
        allocator.free(blocks.tree);
    }

    pub fn encode(blocks: *const ManifestBlocks, target: []align(@alignOf(u128)) u8) u64 {
        assert(target.len > 0);
        assert(target.len % @sizeOf(u128) == 0);

        var size: u64 = 0;

        mem.copy(u128, mem.bytesAsSlice(u128, target[size..]), blocks.checksum[0..blocks.count]);
        size += blocks.count * @sizeOf(u128);

        mem.copy(u64, mem.bytesAsSlice(u64, target[size..]), blocks.address[0..blocks.count]);
        size += blocks.count * @sizeOf(u64);

        mem.copy(u8, mem.bytesAsSlice(u8, target[size..]), blocks.tree[0..blocks.count]);
        size += blocks.count * @sizeOf(u8);

        mem.set(u8, target[size..], 0);

        assert(@divExact(size, @sizeOf(u128) + @sizeOf(u64) + @sizeOf(u8)) == blocks.count);

        return size;
    }

    pub fn decode(blocks: *ManifestBlocks, source: []align(@alignOf(u128)) const u8) void {
        blocks.count = @intCast(
            u32,
            @divExact(source.len, @sizeOf(u128) + @sizeOf(u64) + @sizeOf(u8)),
        );
        assert(blocks.count <= blocks.count_max);

        var size: u64 = 0;

        mem.copy(
            u128,
            blocks.checksum[0..blocks.count],
            mem.bytesAsSlice(u128, source[size..][0 .. blocks.count * @sizeOf(u128)]),
        );
        size += blocks.count * @sizeOf(u128);

        mem.copy(
            u64,
            blocks.address[0..blocks.count],
            mem.bytesAsSlice(u64, source[size..][0 .. blocks.count * @sizeOf(u64)]),
        );
        size += blocks.count * @sizeOf(u64);

        mem.copy(
            u8,
            blocks.tree[0..blocks.count],
            mem.bytesAsSlice(u8, source[size..][0 .. blocks.count * @sizeOf(u8)]),
        );
        size += blocks.count * @sizeOf(u8);

        assert(size == source.len);

        mem.set(u128, blocks.checksum[blocks.count..], 0);
        mem.set(u64, blocks.address[blocks.count..], 0);
        mem.set(u8, blocks.tree[blocks.count..], 0);

        if (config.verify) blocks.verify();
    }

    pub fn append(blocks: *ManifestBlocks, tree: u8, checksum: u128, address: u64) void {
        assert(address > 0);

        if (config.verify) {
            assert(blocks.find(tree, checksum, address) == null);
        }

        if (blocks.count == blocks.count_max) {
            @panic("manifest_blocks: out of space");
        }

        blocks.checksum[blocks.count] = checksum;
        blocks.address[blocks.count] = address;
        blocks.tree[blocks.count] = tree;

        blocks.count += 1;

        log.debug("manifest_blocks: append: tree={} checksum={} address={} count={}/{}", .{
            tree,
            checksum,
            address,
            blocks.count,
            blocks.count_max,
        });

        if (config.verify) {
            if (blocks.find(tree, checksum, address)) |index| {
                assert(index == blocks.count - 1);
                assert(blocks.checksum[index] == checksum);
                assert(blocks.address[index] == address);
                assert(blocks.tree[index] == tree);
            } else {
                unreachable;
            }

            blocks.verify();
        }
    }

    pub fn remove(blocks: *ManifestBlocks, tree: u8, checksum: u128, address: u64) void {
        assert(address > 0);

        if (blocks.find(tree, checksum, address)) |index| {
            assert(index < blocks.count);
            assert(blocks.checksum[index] == checksum);
            assert(blocks.address[index] == address);
            assert(blocks.tree[index] == tree);

            const tail = blocks.count - (index + 1);
            mem.copy(u128, blocks.checksum[index..], blocks.checksum[index + 1 ..][0..tail]);
            mem.copy(u64, blocks.address[index..], blocks.address[index + 1 ..][0..tail]);
            mem.copy(u8, blocks.tree[index..], blocks.tree[index + 1 ..][0..tail]);

            blocks.count -= 1;

            blocks.checksum[blocks.count] = 0;
            blocks.address[blocks.count] = 0;
            blocks.tree[blocks.count] = 0;

            log.debug("manifest_blocks: remove: tree={} checksum={} address={} count={}/{}", .{
                tree,
                checksum,
                address,
                blocks.count,
                blocks.count_max,
            });

            if (config.verify) {
                assert(blocks.find(tree, checksum, address) == null);
                blocks.verify();
            }
        } else {
            unreachable;
        }
    }

    pub fn find(blocks: *const ManifestBlocks, tree: u8, checksum: u128, address: u64) ?u32 {
        assert(address > 0);

        var index: u32 = 0;
        while (index < blocks.count) : (index += 1) {
            if (blocks.checksum[index] != checksum) continue;
            if (blocks.address[index] != address) continue;
            if (blocks.tree[index] != tree) continue;

            return index;
        }

        return null;
    }

    pub const BlockReference = struct {
        checksum: u128,
        address: u64,
        tree: u8,
    };

    pub const IteratorReverse = struct {
        blocks: *const ManifestBlocks,
        tree: u8,
        count: u32,

        pub fn next(it: *IteratorReverse) ?BlockReference {
            assert(it.count <= it.blocks.count);

            while (it.count > 0) {
                it.count -= 1;

                if (it.blocks.tree[it.count] == it.tree) {
                    assert(it.blocks.address[it.count] > 0);

                    return BlockReference{
                        .checksum = it.blocks.checksum[it.count],
                        .address = it.blocks.address[it.count],
                        .tree = it.blocks.tree[it.count],
                    };
                }
            }
            return null;
        }
    };

    /// Return all block references for a given tree in reverse order, i.e. last-appended-first-out.
    /// This reverse order eliminates updates to the manifest for old tables that were superceded,
    /// when we recover the manifest log and iterate the manifest blocks at startup.
    pub fn iterator_reverse(blocks: *const ManifestBlocks, tree: u8) IteratorReverse {
        return IteratorReverse{
            .blocks = blocks,
            .tree = tree,
            .count = blocks.count,
        };
    }

    pub fn verify(blocks: *const ManifestBlocks) void {
        assert(blocks.count <= blocks.count_max);
        assert(blocks.count <= blocks.count_max);
        assert(blocks.count <= blocks.count_max);

        assert(blocks.checksum.len == blocks.count_max);
        assert(blocks.address.len == blocks.count_max);
        assert(blocks.tree.len == blocks.count_max);

        for (blocks.checksum[blocks.count..]) |checksum| assert(checksum == 0);

        for (blocks.address[0..blocks.count]) |address| assert(address > 0);
        for (blocks.address[blocks.count..]) |address| assert(address == 0);

        for (blocks.tree[blocks.count..]) |tree| assert(tree == 0);
    }
};

fn test_iterator_reverse(
    blocks: *ManifestBlocks,
    tree: u8,
    expect: []const ManifestBlocks.BlockReference,
) !void {
    const expectEqualSlices = std.testing.expectEqualSlices;

    var reverse: [3]ManifestBlocks.BlockReference = undefined;
    var reverse_count: usize = 0;

    var it = blocks.iterator_reverse(tree);
    while (it.next()) |block| {
        reverse[reverse_count] = block;
        reverse_count += 1;
    }

    try expectEqualSlices(ManifestBlocks.BlockReference, expect, reverse[0..reverse_count]);
}

fn test_codec(blocks: *ManifestBlocks) !void {
    const testing = std.testing;
    const expectEqual = testing.expectEqual;
    const expectEqualSlices = testing.expectEqualSlices;

    var target_a: [32]u128 = undefined;
    const size_a = blocks.encode(mem.sliceAsBytes(&target_a));
    try expectEqual(@as(u64, blocks.count * (@sizeOf(u128) + @sizeOf(u64) + @sizeOf(u8))), size_a);

    // Test that the decoded instance matches the original instance:
    var decoded = try ManifestBlocks.init(testing.allocator, blocks.count_max);
    defer decoded.deinit(testing.allocator);

    decoded.decode(mem.sliceAsBytes(&target_a)[0..size_a]);

    try expectEqualSlices(u128, blocks.checksum, decoded.checksum);
    try expectEqualSlices(u64, blocks.address, decoded.address);
    try expectEqualSlices(u8, blocks.tree, decoded.tree);
    try expectEqual(blocks.count_max, decoded.count_max);
    try expectEqual(blocks.count, decoded.count);

    // Test that the decoded instance encodes correctly:
    var target_b: [32]u128 = undefined;
    const size_b = decoded.encode(mem.sliceAsBytes(&target_b));
    try expectEqual(size_a, size_b);
    try expectEqualSlices(
        u8,
        mem.sliceAsBytes(&target_a)[0..size_a],
        mem.sliceAsBytes(&target_b)[0..size_b],
    );
}

test {
    const testing = std.testing;
    const expectEqual = testing.expectEqual;

    var blocks = try ManifestBlocks.init(testing.allocator, 3);
    defer blocks.deinit(testing.allocator);

    for (blocks.checksum) |checksum| try expectEqual(@as(u128, 0), checksum);
    for (blocks.address) |address| try expectEqual(@as(u64, 0), address);
    for (blocks.tree) |tree| try expectEqual(@as(u8, 0), tree);

    // The arguments to append()/remove() are: tree, checksum, address
    // These will be named variables and should be clear where we use them for real.
    blocks.append(1, 2, 3);
    try expectEqual(@as(?u32, 0), blocks.find(1, 2, 3));

    blocks.append(2, 3, 4);
    try expectEqual(@as(?u32, 1), blocks.find(2, 3, 4));

    blocks.append(1, 4, 5);
    try expectEqual(@as(?u32, 2), blocks.find(1, 4, 5));

    try test_iterator_reverse(
        &blocks,
        1,
        &[_]ManifestBlocks.BlockReference{
            .{ .checksum = 4, .address = 5, .tree = 1 },
            .{ .checksum = 2, .address = 3, .tree = 1 },
        },
    );

    try test_iterator_reverse(
        &blocks,
        2,
        &[_]ManifestBlocks.BlockReference{
            .{ .checksum = 3, .address = 4, .tree = 2 },
        },
    );

    try test_codec(&blocks);

    blocks.remove(1, 2, 3);
    try expectEqual(@as(?u32, null), blocks.find(1, 2, 3));
    try expectEqual(@as(?u32, 0), blocks.find(2, 3, 4));
    try expectEqual(@as(?u32, 1), blocks.find(1, 4, 5));

    try expectEqual(@as(u128, 0), blocks.checksum[2]);
    try expectEqual(@as(u64, 0), blocks.address[2]);
    try expectEqual(@as(u8, 0), blocks.tree[2]);

    blocks.append(1, 2, 3);
    try expectEqual(@as(?u32, 2), blocks.find(1, 2, 3));

    blocks.remove(1, 4, 5);
    try expectEqual(@as(?u32, null), blocks.find(1, 4, 5));
    try expectEqual(@as(?u32, 1), blocks.find(1, 2, 3));

    blocks.remove(2, 3, 4);
    try expectEqual(@as(?u32, null), blocks.find(2, 3, 4));
    try expectEqual(@as(?u32, 0), blocks.find(1, 2, 3));

    blocks.remove(1, 2, 3);
    try expectEqual(@as(?u32, null), blocks.find(1, 2, 3));
    try expectEqual(@as(?u32, null), blocks.find(2, 3, 4));
    try expectEqual(@as(?u32, null), blocks.find(1, 4, 5));

    for (blocks.checksum) |checksum| try expectEqual(@as(u128, 0), checksum);
    for (blocks.address) |address| try expectEqual(@as(u64, 0), address);
    for (blocks.tree) |tree| try expectEqual(@as(u8, 0), tree);

    try expectEqual(@as(u32, 0), blocks.count);
    try expectEqual(@as(u32, 3), blocks.count_max);
}
