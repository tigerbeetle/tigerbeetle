const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.superblock_quorums);

const superblock = @import("./superblock.zig");
const SuperBlockSector = superblock.SuperBlockSector;
const SuperBlockVersion = superblock.SuperBlockVersion;

const Options = struct {
    superblock_copies: u8,
};

pub fn QuorumsType(comptime options: Options) type {
    return struct {
        const Quorums = @This();

        const Quorum = struct {
            sector: *const SuperBlockSector,
            valid: bool = false,
            /// Track which copies are a member of the quorum.
            /// Used to ignore duplicate copies of a sector when determining a quorum.
            copies: QuorumCount = QuorumCount.initEmpty(),
            /// An integer value indicates the copy index found in the corresponding slot.
            /// A `null` value indicates that the copy is invalid or not a member of the working
            /// quorum. All copies belong to the same (valid, working) quorum.
            slots: [options.superblock_copies]?u8 = [_]?u8{null} ** options.superblock_copies,

            pub fn repairs(quorum: Quorum) RepairIterator {
                assert(quorum.valid);
                return .{ .slots = quorum.slots };
            }
        };

        const QuorumCount = std.StaticBitSet(options.superblock_copies);

        pub const Error = error{
            NotFound,
            QuorumLost,
            ParentNotConnected,
            VSRStateNotMonotonic,
        };

        /// We use flexible quorums for even quorums with write quorum > read quorum, for example:
        /// * When writing, we must verify that at least 3/4 copies were written.
        /// * At startup, we must verify that at least 2/4 copies were read.
        ///
        /// This ensures that our read and write quorums will intersect.
        /// Using flexible quorums in this way increases resiliency of the superblock.
        pub const Threshold = enum {
            verify,
            open,
            // Working these threshold out by formula is easy to get wrong, so enumerate them:
            // The rule is that the write quorum plus the read quorum must be exactly copies + 1.

            pub fn count(threshold: Threshold) u8 {
                return switch (threshold) {
                    .verify => switch (options.superblock_copies) {
                        4 => 3,
                        6 => 4,
                        8 => 5,
                        else => unreachable,
                    },
                    // The open quorum must allow for at least two copy faults, because our view
                    // change updates an existing set of copies in place, temporarily impairing one
                    // copy.
                    .open => switch (options.superblock_copies) {
                        4 => 2,
                        6 => 3,
                        8 => 4,
                        else => unreachable,
                    },
                };
            }
        };

        array: [options.superblock_copies]Quorum = undefined,
        count: u8 = 0,

        /// Returns the working superblock according to the quorum with the highest sequence number.
        /// When a member of the parent quorum is still present, verify that the highest quorum is
        /// connected.
        pub fn working(
            quorums: *Quorums,
            copies: []SuperBlockSector,
            threshold: Threshold,
        ) Error!Quorum {
            assert(copies.len == options.superblock_copies);
            assert(threshold.count() >= 2 and threshold.count() <= 5);

            quorums.array = undefined;
            quorums.count = 0;

            for (copies) |*copy, index| quorums.count_copy(copy, index, threshold);

            std.sort.sort(Quorum, quorums.slice(), {}, sort_priority_descending);

            for (quorums.slice()) |quorum| {
                if (quorum.copies.count() == options.superblock_copies) {
                    log.debug("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                        quorum.sector.checksum,
                        quorum.sector.parent,
                        quorum.sector.sequence,
                        quorum.copies.count(),
                        quorum.valid,
                    });
                } else {
                    log.warn("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                        quorum.sector.checksum,
                        quorum.sector.parent,
                        quorum.sector.sequence,
                        quorum.copies.count(),
                        quorum.valid,
                    });
                }
            }

            // No working copies of any sequence number exist in the superblock storage zone at all.
            if (quorums.slice().len == 0) return error.NotFound;

            // At least one copy or quorum exists.
            const b = quorums.slice()[0];

            // Verify that the remaining quorums are correctly sorted:
            for (quorums.slice()[1..]) |a| {
                assert(sort_priority_descending({}, b, a));
                assert(a.sector.valid_checksum());
            }

            // Even the best copy with the most quorum still has inadequate quorum.
            if (!b.valid) return error.QuorumLost;

            // If a parent quorum is present (either complete or incomplete) it must be connected to the
            // new working quorum. The parent quorum can exist due to:
            // - a crash during checkpoint()/view_change() before writing all copies
            // - a lost or misdirected write
            // - a latent sector error that prevented a write
            for (quorums.slice()[1..]) |a| {
                if (a.sector.cluster != b.sector.cluster) {
                    log.warn("superblock copy={} has cluster={} instead of {}", .{
                        a.sector.copy,
                        a.sector.cluster,
                        b.sector.cluster,
                    });
                } else if (a.sector.replica != b.sector.replica) {
                    log.warn("superblock copy={} has replica={} instead of {}", .{
                        a.sector.copy,
                        a.sector.replica,
                        b.sector.replica,
                    });
                } else if (a.sector.sequence + 1 == b.sector.sequence) {
                    assert(a.sector.checksum != b.sector.checksum);
                    assert(a.sector.cluster == b.sector.cluster);
                    assert(a.sector.replica == b.sector.replica);

                    if (a.sector.checksum != b.sector.parent) {
                        return error.ParentNotConnected;
                    } else if (!a.sector.vsr_state.monotonic(b.sector.vsr_state)) {
                        return error.VSRStateNotMonotonic;
                    } else {
                        assert(b.sector.valid_checksum());

                        return b;
                    }
                }
            }

            assert(b.sector.valid_checksum());
            return b;
        }

        fn count_copy(
            quorums: *Quorums,
            copy: *const SuperBlockSector,
            slot: usize,
            threshold: Threshold,
        ) void {
            assert(slot < options.superblock_copies);
            assert(threshold.count() >= 2 and threshold.count() <= 5);

            if (!copy.valid_checksum()) {
                log.debug("copy: {}/{}: invalid checksum", .{ slot, options.superblock_copies });
                return;
            }

            if (copy.copy == slot) {
                log.debug("copy: {}/{}: checksum={x} parent={x} sequence={}", .{
                    slot,
                    options.superblock_copies,
                    copy.checksum,
                    copy.parent,
                    copy.sequence,
                });
            } else if (copy.copy >= options.superblock_copies) {
                log.warn("copy: {}/{}: checksum={x} parent={x} sequence={} corrupt copy={}", .{
                    slot,
                    options.superblock_copies,
                    copy.checksum,
                    copy.parent,
                    copy.sequence,
                    copy.copy,
                });
            } else {
                // If our read was misdirected, we definitely still want to count the copy.
                // We must just be careful to count it idempotently.
                log.warn(
                    "copy: {}/{}: checksum={x} parent={x} sequence={} misdirected from copy={}",
                    .{
                        slot,
                        options.superblock_copies,
                        copy.checksum,
                        copy.parent,
                        copy.sequence,
                        copy.copy,
                    },
                );
            }

            var quorum = quorums.find_or_insert_quorum_for_copy(copy);
            assert(quorum.sector.checksum == copy.checksum);
            assert(quorum.sector.equal(copy));

            if (copy.copy >= options.superblock_copies) {
                // This sector is a valid member of the quorum, but with an unexpected copy number.
                // The "SuperBlockSector.copy" field is not protected by the checksum, so if that byte
                // (and only that byte) is corrupted, the superblock is still valid — but we don't know
                // for certain which copy this was supposed to be.
                // We make the assumption that this was not a double-fault (corrupt + misdirect) —
                // that is, the copy is in the correct slot, and its copy index is simply corrupt.
                quorum.slots[slot] = @intCast(u8, slot);
                quorum.copies.set(slot);
            } else if (quorum.copies.isSet(copy.copy)) {
                // Ignore the duplicate copy.
            } else {
                quorum.slots[slot] = copy.copy;
                quorum.copies.set(copy.copy);
            }

            quorum.valid = quorum.copies.count() >= threshold.count();
        }

        fn find_or_insert_quorum_for_copy(quorums: *Quorums, copy: *const SuperBlockSector) *Quorum {
            assert(copy.valid_checksum());

            for (quorums.array[0..quorums.count]) |*quorum| {
                if (copy.checksum == quorum.sector.checksum) return quorum;
            } else {
                quorums.array[quorums.count] = Quorum{ .sector = copy };
                quorums.count += 1;

                return &quorums.array[quorums.count - 1];
            }
        }

        fn slice(quorums: *Quorums) []Quorum {
            return quorums.array[0..quorums.count];
        }

        fn sort_priority_descending(_: void, a: Quorum, b: Quorum) bool {
            assert(a.sector.checksum != b.sector.checksum);

            if (a.valid and !b.valid) return true;
            if (b.valid and !a.valid) return false;

            if (a.sector.sequence > b.sector.sequence) return true;
            if (b.sector.sequence > a.sector.sequence) return false;

            if (a.copies.count() > b.copies.count()) return true;
            if (b.copies.count() > a.copies.count()) return false;

            // The sort order must be stable and deterministic:
            return a.sector.checksum > b.sector.checksum;
        }

        /// Repair a quorum's copies in the safest known order.
        /// Repair is complete when every copy is on-disk (not necessarily in its home slot).
        ///
        /// We must be careful when repairing superblock sectors to avoid endangering our quorum if
        /// an additional fault occurs. We primarily guard against torn sector writes — preventing a
        /// misdirected write from derailing repair is far more expensive and complex — but they are
        /// likewise far less likely to occur.
        ///
        /// For example, consider this case:
        ///   0. Sequence is initially A.
        ///   1. Checkpoint sequence B.
        ///   2.   Write B₀ — ok.
        ///   3.   Write B₁ — misdirected to B₂'s slot.
        ///   4. Crash.
        ///   5. Recover with quorum B[B₀,A₁,B₁,A₃].
        /// If we repair the superblock quorum while only considering the valid copies (and not slots)
        /// the following scenario could occur:
        ///   6. We already have a valid B₀ and B₁, so begin writing B₂.
        ///   7. Crash, tearing the B₂ write.
        ///   8. Recover with quorum A[B₀,A₁,_,A₂].
        /// The working quorum backtracked from B to A!
        pub const RepairIterator = struct {
            /// An integer value indicates the copy index found in the corresponding slot.
            /// A `null` value indicates that the copy is invalid or not a member of the working quorum.
            /// All copies belong to the same (valid, working) quorum.
            slots: [options.superblock_copies]?u8,

            /// Returns the slot/copy to repair next.
            /// We never (deliberately) write a copy to a slot other than its own. This is simpler to
            /// implement, and also reduces risk when one of open()'s reads was misdirected.
            pub fn next(iterator: *RepairIterator) ?u8 {
                // Corrupt copy indices have already been normalized.
                for (iterator.slots) |slot| assert(slot == null or slot.? < options.superblock_copies);

                // Set bits indicate that the corresponding copy was found at least once.
                var copies_any = QuorumCount.initEmpty();
                // Set bits indicate that the corresponding copy was found more than once.
                var copies_duplicate = QuorumCount.initEmpty();

                for (iterator.slots) |slot| {
                    if (slot) |copy| {
                        if (copies_any.isSet(copy)) copies_duplicate.set(copy);
                        copies_any.set(copy);
                    }
                }

                // In descending order, our priorities for repair are:
                // 1. The slot holds no sector, and the copy was not found anywhere.
                // 2. The slot holds no sector, but its copy was found elsewhere.
                // 3. The slot holds a misdirected sector, but that copy is in another slot as well.
                var a: ?u8 = null;
                var b: ?u8 = null;
                var c: ?u8 = null;
                for (iterator.slots) |slot, i| {
                    if (slot == null and !copies_any.isSet(i)) a = @intCast(u8, i);
                    if (slot == null and copies_any.isSet(i)) b = @intCast(u8, i);
                    if (slot) |slot_copy| {
                        if (slot_copy != i and copies_duplicate.isSet(slot_copy)) c = @intCast(u8, i);
                    }
                }

                const repair = a orelse b orelse c orelse {
                    for (iterator.slots) |slot| assert(slot != null);
                    return null;
                };

                iterator.slots[repair] = repair;
                return repair;
            }
        };
    };
}

test "Quorums.working" {
    // Don't print warnings from the Quorums.
    var level = std.log.Level.err;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    const t = test_quorums_working;
    const o = CopyTemplate.make_valid;
    const x = CopyTemplate.make_invalid_broken;
    const X = {}; // Ignored; just for text alignment + contrast.

    // No faults:
    try t(2, &.{ o(3), o(3), o(3), o(3) }, 3);
    try t(3, &.{ o(3), o(3), o(3), o(3) }, 3);

    // Single fault:
    try t(3, &.{ x(X), o(3), o(3), o(3) }, 3);
    // Double fault, same quorum:
    try t(2, &.{ x(X), x(X), o(4), o(4) }, 4);
    try t(3, &.{ x(X), x(X), o(4), o(4) }, error.QuorumLost);
    // Double fault, different quorums:
    try t(2, &.{ x(X), x(X), o(3), o(4) }, error.QuorumLost);
    // Triple fault.
    try t(2, &.{ x(X), x(X), x(X), o(4) }, error.QuorumLost);

    // Partial format (broken sequence=1):
    try t(2, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(3, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(2, &.{ x(X), x(X), o(1), o(1) }, 1);
    try t(3, &.{ x(X), x(X), o(1), o(1) }, error.QuorumLost);
    try t(2, &.{ x(X), x(X), x(X), o(1) }, error.QuorumLost);
    try t(2, &.{ x(X), x(X), x(X), x(X) }, error.NotFound);

    // Partial checkpoint() to sequence=4 (2 quorums):
    try t(2, &.{ o(3), o(2), o(2), o(2) }, 2); // open after 1/4
    try t(2, &.{ o(3), o(3), o(2), o(2) }, 3); // open after 2/4
    try t(2, &.{ o(3), o(3), o(3), o(2) }, 3); // open after 3/4
    // Partial checkpoint() to sequence=4 (3 quorums):
    try t(2, &.{ o(1), o(2), o(3), o(3) }, 3);
    try t(3, &.{ o(1), o(2), o(3), o(3) }, error.QuorumLost);

    // Skipped sequence.
    try t(2, &.{ o(2), o(2), o(2), o(4) }, 2); // open after 1/4
    try t(2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 2/4
    try t(2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 3/4

    // Parent has wrong cluster|replica.
    const m = CopyTemplate.make_invalid_misdirect;
    try t(2, &.{ m(2), m(2), m(2), o(3) }, 2);
    try t(2, &.{ m(2), m(2), o(3), o(3) }, 3);
    try t(2, &.{ m(2), o(3), o(3), o(3) }, 3);
    // Grandparent has wrong cluster|replica.
    try t(2, &.{ m(2), m(2), m(2), o(4) }, 2);
    try t(2, &.{ m(2), m(2), o(4), o(4) }, 4);
    try t(2, &.{ m(2), o(4), o(4), o(4) }, 4);

    // Parent/child hash chain is broken.
    const p = CopyTemplate.make_invalid_parent;
    try t(2, &.{ o(2), o(2), o(2), p(3) }, 2);
    try t(2, &.{ o(2), o(2), p(3), p(3) }, error.ParentNotConnected);
    try t(2, &.{ o(2), p(3), p(3), p(3) }, error.ParentNotConnected);
    try t(2, &.{ p(3), p(3), p(3), p(3) }, 3);

    // Parent view is greater than child view.
    const v = CopyTemplate.make_invalid_vsr_state;
    try t(2, &.{ v(2), v(2), o(3), o(3) }, error.VSRStateNotMonotonic);

    // A member of the quorum has an "invalid" copy, but an otherwise valid checksum.
    const h = CopyTemplate.make_valid_high_copy;
    try t(2, &.{ o(2), o(2), o(3), h(3) }, 3);
}

const CopyTemplate = struct {
    sequence: u64,
    variant: enum {
        valid,
        valid_high_copy,
        invalid_broken,
        invalid_misdirect,
        invalid_parent,
        invalid_vsr_state,
    },

    fn make_valid(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid };
    }

    /// Construct a copy with a corrupt copy index (≥superblock_copies).
    fn make_valid_high_copy(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid_high_copy };
    }

    /// Construct a corrupt (invalid checksum) or duplicate copy copy.
    fn make_invalid_broken(_: void) CopyTemplate {
        // Use a high sequence so that invalid copies are the last generated by
        // test_quorums_working(), so that they can become duplicates of (earlier) valid copies.
        return .{ .sequence = 6, .variant = .invalid_broken };
    }

    /// Construct a copy with either an incorrect "cluster" or "replica".
    fn make_invalid_misdirect(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_misdirect };
    }

    /// Construct a copy with an invalid "parent" checksum.
    fn make_invalid_parent(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_parent };
    }

    /// Construct a copy with a newer `VSRState` than its parent.
    fn make_invalid_vsr_state(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_vsr_state };
    }

    fn less_than(_: void, a: CopyTemplate, b: CopyTemplate) bool {
        return a.sequence < b.sequence;
    }
};

fn test_quorums_working(
    threshold_count: u8,
    copies: *[4]CopyTemplate,
    result: QuorumsType(.{ .superblock_copies = 4 }).Error!u64,
) !void {
    const Quorums = QuorumsType(.{ .superblock_copies = 4 });
    var prng = std.rand.DefaultPrng.init(@intCast(u64, std.time.milliTimestamp()));
    const random = prng.random();
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

    try std.testing.expectEqual(
        result,
        if (quorums.working(&sectors, threshold)) |working| working.sector.sequence else |err| err,
    );
}

// Verify that a torn sector write during repair never compromises the existing quorum.
test "Quorum.repairs" {
    const Quorums = QuorumsType(.{ .superblock_copies = 4 });
    // Don't print warnings from the Quorums.
    var level = std.log.Level.err;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    var prng = std.rand.DefaultPrng.init(@intCast(u64, std.time.milliTimestamp()));
    const random = prng.random();

    var q1: Quorums = undefined;
    var q2: Quorums = undefined;

    const sectors_valid = blk: {
        var sectors: [4]SuperBlockSector = undefined;
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

    var repetitions: usize = 0;
    while (repetitions < 100) : (repetitions += 1) {
        // Generate a random valid 2/4 quorum.
        // 1 bits indicate valid sectors.
        // 0 bits indicate invalid sectors.
        var valid = std.bit_set.IntegerBitSet(4).initEmpty();
        while (valid.count() < 2 or random.boolean()) valid.set(random.uintLessThan(usize, 4));

        var working_sectors: [4]SuperBlockSector = undefined;
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
}
