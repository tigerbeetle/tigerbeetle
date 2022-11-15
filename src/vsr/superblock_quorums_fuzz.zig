const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_vsr_superblock_quorums);

const superblock = @import("./superblock.zig");
const SuperBlockSector = superblock.SuperBlockSector;
const SuperBlockVersion = superblock.SuperBlockVersion;

const fuzz = @import("../test/fuzz.zig");
const superblock_quorums = @import("superblock_quorums.zig");
const QuorumsType = superblock_quorums.QuorumsType;

pub fn main() !void {
    const fuzz_args = try fuzz.parse_fuzz_args(std.testing.allocator);
    var prng = std.rand.DefaultPrng.init(fuzz_args.seed);

    // TODO When there is a top-level fuzz.zig main(), split these fuzzers into two different
    // commands.
    try fuzz_quorums_working(prng.random());

    try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 4 });
    // TODO: Enable these once SuperBlockSector is generic over its Constants.
    // try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 6 });
    // try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 8 });
}

pub fn fuzz_quorums_working(random: std.rand.Random) !void {
    const r = random;
    const t = test_quorums_working;
    const o = CopyTemplate.make_valid;
    const x = CopyTemplate.make_invalid_broken;
    const X = {}; // Ignored; just for text alignment + contrast.

    // No faults:
    try t(r, 2, &.{ o(3), o(3), o(3), o(3) }, 3);
    try t(r, 3, &.{ o(3), o(3), o(3), o(3) }, 3);

    // Single fault:
    try t(r, 3, &.{ x(X), o(4), o(4), o(4) }, 4);
    // Double fault, same quorum:
    try t(r, 2, &.{ x(X), x(X), o(4), o(4) }, 4);
    try t(r, 3, &.{ x(X), x(X), o(4), o(4) }, error.QuorumLost);
    // Double fault, different quorums:
    try t(r, 2, &.{ x(X), x(X), o(3), o(4) }, error.QuorumLost);
    // Triple fault.
    try t(r, 2, &.{ x(X), x(X), x(X), o(4) }, error.QuorumLost);

    // Partial format (broken sequence=1):
    try t(r, 2, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(r, 3, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(r, 2, &.{ x(X), x(X), o(1), o(1) }, 1);
    try t(r, 3, &.{ x(X), x(X), o(1), o(1) }, error.QuorumLost);
    try t(r, 2, &.{ x(X), x(X), x(X), o(1) }, error.QuorumLost);
    try t(r, 2, &.{ x(X), x(X), x(X), x(X) }, error.NotFound);

    // Partial checkpoint() to sequence=4 (2 quorums):
    try t(r, 2, &.{ o(3), o(2), o(2), o(2) }, 2); // open after 1/4
    try t(r, 2, &.{ o(3), o(3), o(2), o(2) }, 3); // open after 2/4
    try t(r, 2, &.{ o(3), o(3), o(3), o(2) }, 3); // open after 3/4
    // Partial checkpoint() to sequence=4 (3 quorums):
    try t(r, 2, &.{ o(1), o(2), o(3), o(3) }, 3);
    try t(r, 3, &.{ o(1), o(2), o(3), o(3) }, error.QuorumLost);

    // Skipped sequence.
    try t(r, 2, &.{ o(2), o(2), o(2), o(4) }, error.ParentSkipped); // open after 1/4
    try t(r, 2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 2/4
    try t(r, 2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 3/4

    // Forked sequence: same sequence number, different checksum, both valid.
    const f = CopyTemplate.make_invalid_fork;
    try t(r, 2, &.{ o(3), o(3), o(3), f(3) }, error.Fork);
    try t(r, 2, &.{ o(3), o(3), f(3), f(3) }, error.Fork);

    // Parent has wrong cluster|replica.
    const m = CopyTemplate.make_invalid_misdirect;
    try t(r, 2, &.{ m(2), m(2), m(2), o(3) }, 2);
    try t(r, 2, &.{ m(2), m(2), o(3), o(3) }, 3);
    try t(r, 2, &.{ m(2), o(3), o(3), o(3) }, 3);
    // Grandparent has wrong cluster|replica.
    try t(r, 2, &.{ m(2), m(2), m(2), o(4) }, 2);
    try t(r, 2, &.{ m(2), m(2), o(4), o(4) }, 4);
    try t(r, 2, &.{ m(2), o(4), o(4), o(4) }, 4);

    // Parent/child hash chain is broken.
    const p = CopyTemplate.make_invalid_parent;
    try t(r, 2, &.{ o(2), o(2), o(2), p(3) }, 2);
    try t(r, 2, &.{ o(2), o(2), p(3), p(3) }, error.ParentNotConnected);
    try t(r, 2, &.{ o(2), p(3), p(3), p(3) }, error.ParentNotConnected);
    try t(r, 2, &.{ p(3), p(3), p(3), p(3) }, 3);

    // Parent view is greater than child view.
    const v = CopyTemplate.make_invalid_vsr_state;
    try t(r, 2, &.{ v(2), v(2), o(3), o(3) }, error.VSRStateNotMonotonic);

    // A member of the quorum has an "invalid" copy, but an otherwise valid checksum.
    const h = CopyTemplate.make_valid_high_copy;
    try t(r, 2, &.{ o(2), o(2), o(3), h(3) }, 3);
}

fn test_quorums_working(
    random: std.rand.Random,
    threshold_count: u8,
    copies: *[4]CopyTemplate,
    result: QuorumsType(.{ .superblock_copies = 4 }).Error!u64,
) !void {
    const Quorums = QuorumsType(.{ .superblock_copies = 4 });
    const misdirect = random.boolean(); // true:cluster false:replica
    var quorums: Quorums = undefined;
    var sectors: [4]SuperBlockSector = undefined;
    // TODO(Zig): Ideally this would be a [6]?u128 and the access would be
    // "checksums[i] orelse random.int(u128)", but that currently causes the compiler to segfault
    // during code generation.
    var checksums: [6]u128 = undefined;
    for (checksums) |*c| c.* = random.int(u128);

    // Create sectors in ascending-sequence order to build the checksum/parent hash chain.
    std.sort.sort(CopyTemplate, copies, {}, CopyTemplate.less_than);

    for (sectors) |*sector, i| {
        sector.* = std.mem.zeroInit(SuperBlockSector, .{
            .copy = @intCast(u8, i),
            .version = SuperBlockVersion,
            .replica = 1,
            .size_max = superblock.data_file_size_min,
            .sequence = copies[i].sequence,
            .parent = checksums[copies[i].sequence - 1],
        });

        var checksum: ?u128 = null;
        switch (copies[i].variant) {
            .valid => {},
            .valid_high_copy => sector.copy = 4,
            .invalid_broken => {
                if (random.boolean() and i > 0) {
                    // Error: duplicate sector (if available).
                    sector.* = sectors[random.uintLessThanBiased(usize, i)];
                    checksum = random.int(u128);
                } else {
                    // Error: invalid checksum.
                    checksum = random.int(u128);
                }
            },
            .invalid_fork => sector.size_max += 1, // Ensure we have a different checksum.
            .invalid_parent => sector.parent += 1,
            .invalid_misdirect => {
                if (misdirect) {
                    sector.cluster += 1;
                } else {
                    sector.replica += 1;
                }
            },
            .invalid_vsr_state => sector.vsr_state.view += 1,
        }
        sector.checksum = checksum orelse sector.calculate_checksum();

        if (copies[i].variant == .valid or copies[i].variant == .invalid_vsr_state) {
            checksums[sector.sequence] = sector.checksum;
        }
    }

    for (copies) |template| {
        if (template.variant == .valid_high_copy) break;
    } else {
        // Shuffling copies can only change the working quorum when we have a corrupt copy index,
        // because we guess that the true index is the slot.
        random.shuffle(SuperBlockSector, &sectors);
    }

    const threshold = switch (threshold_count) {
        2 => Quorums.Threshold.open,
        3 => Quorums.Threshold.verify,
        else => unreachable,
    };
    assert(threshold.count() == threshold_count);

    if (quorums.working(&sectors, threshold)) |working| {
        try std.testing.expectEqual(result, working.sector.sequence);
    } else |err| {
        try std.testing.expectEqual(result, err);
    }
}

pub const CopyTemplate = struct {
    sequence: u64,
    variant: Variant,

    const Variant = enum {
        valid,
        valid_high_copy,
        invalid_broken,
        invalid_fork,
        invalid_misdirect,
        invalid_parent,
        invalid_vsr_state,
    };

    pub fn make_valid(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid };
    }

    /// Construct a copy with a corrupt copy index (≥superblock_copies).
    pub fn make_valid_high_copy(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid_high_copy };
    }

    /// Construct a corrupt (invalid checksum) or duplicate copy copy.
    pub fn make_invalid_broken(_: void) CopyTemplate {
        // Use a high sequence so that invalid copies are the last generated by
        // test_quorums_working(), so that they can become duplicates of (earlier) valid copies.
        return .{ .sequence = 6, .variant = .invalid_broken };
    }

    /// Construct a copy with a valid checksum — but which differs from the "canonical" version
    /// of this sequence.
    pub fn make_invalid_fork(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_fork };
    }

    /// Construct a copy with either an incorrect "cluster" or "replica".
    pub fn make_invalid_misdirect(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_misdirect };
    }

    /// Construct a copy with an invalid "parent" checksum.
    pub fn make_invalid_parent(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_parent };
    }

    /// Construct a copy with a newer `VSRState` than its parent.
    pub fn make_invalid_vsr_state(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_vsr_state };
    }

    fn less_than(_: void, a: CopyTemplate, b: CopyTemplate) bool {
        return a.sequence < b.sequence;
    }
};

// Verify that a torn sector write during repair never compromises the existing quorum.
pub fn fuzz_quorum_repairs(
    random: std.rand.Random,
    comptime options: superblock_quorums.Options,
) !void {
    const superblock_copies = options.superblock_copies;
    const Quorums = QuorumsType(options);

    var q1: Quorums = undefined;
    var q2: Quorums = undefined;

    const sectors_valid = blk: {
        var sectors: [superblock_copies]SuperBlockSector = undefined;
        for (&sectors) |*sector, i| {
            sector.* = std.mem.zeroInit(SuperBlockSector, .{
                .copy = @intCast(u8, i),
                .version = SuperBlockVersion,
                .replica = 1,
                .size_max = superblock.data_file_size_min,
                .sequence = 123,
            });
            sector.set_checksum();
        }
        break :blk sectors;
    };

    const sector_invalid = blk: {
        var sector = sectors_valid[0];
        sector.checksum = 456;
        break :blk sector;
    };

    // Generate a random valid 2/4 quorum.
    // 1 bits indicate valid sectors.
    // 0 bits indicate invalid sectors.
    var valid = std.bit_set.IntegerBitSet(superblock_copies).initEmpty();
    while (valid.count() < Quorums.Threshold.open.count() or random.boolean()) {
        valid.set(random.uintLessThan(usize, superblock_copies));
    }

    var working_sectors: [superblock_copies]SuperBlockSector = undefined;
    for (&working_sectors) |*sector, i| {
        sector.* = if (valid.isSet(i)) sectors_valid[i] else sector_invalid;
    }
    random.shuffle(SuperBlockSector, &working_sectors);
    var repair_sectors = working_sectors;

    const working_quorum = q1.working(&working_sectors, .open) catch unreachable;
    var quorum_repairs = working_quorum.repairs();
    while (quorum_repairs.next()) |repair_copy| {
        {
            // Simulate a torn sector write, crash, recover sequence.
            var damaged_sectors = repair_sectors;
            damaged_sectors[repair_copy] = sector_invalid;
            const damaged_quorum = q2.working(&damaged_sectors, .open) catch unreachable;
            assert(damaged_quorum.sector.checksum == working_quorum.sector.checksum);
        }

        // "Finish" the write so that we can test the next repair.
        repair_sectors[repair_copy] = sectors_valid[repair_copy];

        const quorum_repaired = q2.working(&repair_sectors, .open) catch unreachable;
        assert(quorum_repaired.sector.checksum == working_quorum.sector.checksum);
    }

    // At the end of all repairs, we expect to have every copy of the superblock.
    // They do not need to be in their home slot.
    var copies = Quorums.QuorumCount.initEmpty();
    for (repair_sectors) |repair_sector| {
        assert(repair_sector.checksum == working_quorum.sector.checksum);
        copies.set(repair_sector.copy);
    }
    assert(repair_sectors.len == copies.count());
}
