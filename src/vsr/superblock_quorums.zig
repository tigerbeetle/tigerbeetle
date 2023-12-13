const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.superblock_quorums);

const superblock = @import("./superblock.zig");
const SuperBlockHeader = superblock.SuperBlockHeader;
const SuperBlockVersion = superblock.SuperBlockVersion;
const fuzz = @import("./superblock_quorums_fuzz.zig");

pub const Options = struct {
    superblock_copies: u8,
};

pub fn QuorumsType(comptime options: Options) type {
    return struct {
        const Quorums = @This();

        const Quorum = struct {
            header: *const SuperBlockHeader,
            valid: bool = false,
            /// Track which copies are a member of the quorum.
            /// Used to ignore duplicate copies of a header when determining a quorum.
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

        pub const QuorumCount = std.StaticBitSet(options.superblock_copies);

        pub const Error = error{
            Fork,
            NotFound,
            QuorumLost,
            ParentNotConnected,
            ParentSkipped,
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
                    // The open quorum must allow for at least two copy faults, because we update
                    // copies in place, temporarily impairing one copy.
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
        ///
        /// * When a member of the parent quorum is still present, verify that the highest quorum is
        ///   connected.
        /// * When there are 2 quorums: 1/4 new and 3/4 old, favor the 3/4 old since it is safer to
        ///   repair.
        ///   TODO Re-examine this now that there are no superblock trailers to worry about.
        pub fn working(
            quorums: *Quorums,
            copies: []SuperBlockHeader,
            threshold: Threshold,
        ) Error!Quorum {
            assert(copies.len == options.superblock_copies);
            assert(threshold.count() >= 2 and threshold.count() <= 5);

            quorums.array = undefined;
            quorums.count = 0;

            for (copies, 0..) |*copy, index| quorums.count_copy(copy, index, threshold);

            std.mem.sort(Quorum, quorums.slice(), {}, sort_priority_descending);

            for (quorums.slice()) |quorum| {
                if (quorum.copies.count() == options.superblock_copies) {
                    log.debug("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                        quorum.header.checksum,
                        quorum.header.parent,
                        quorum.header.sequence,
                        quorum.copies.count(),
                        quorum.valid,
                    });
                } else {
                    log.warn("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                        quorum.header.checksum,
                        quorum.header.parent,
                        quorum.header.sequence,
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
                assert(a.header.valid_checksum());
            }

            // Even the best copy with the most quorum still has inadequate quorum.
            if (!b.valid) return error.QuorumLost;

            // If a parent quorum is present (either complete or incomplete) it must be connected to the
            // new working quorum. The parent quorum can exist due to:
            // - a crash during checkpoint()/view_change() before writing all copies
            // - a lost or misdirected write
            // - a latent sector error that prevented a write
            for (quorums.slice()[1..]) |a| {
                if (a.header.cluster != b.header.cluster) {
                    log.warn("superblock copy={} has cluster={} instead of {}", .{
                        a.header.copy,
                        a.header.cluster,
                        b.header.cluster,
                    });
                    continue;
                }

                if (a.header.vsr_state.replica_id != b.header.vsr_state.replica_id) {
                    log.warn("superblock copy={} has replica_id={} instead of {}", .{
                        a.header.copy,
                        a.header.vsr_state.replica_id,
                        b.header.vsr_state.replica_id,
                    });
                    continue;
                }

                if (a.header.sequence == b.header.sequence) {
                    // Two quorums, same cluster+replica+sequence, but different checksums.
                    // This shouldn't ever happen — but if it does, we can't safely repair.
                    assert(a.header.checksum != b.header.checksum);
                    return error.Fork;
                }

                if (a.header.sequence > b.header.sequence + 1) {
                    // We read sequences such as (2,2,2,4) — 2 isn't safe to use, but there isn't a
                    // valid quorum for 4 either.
                    return error.ParentSkipped;
                }

                if (a.header.sequence + 1 == b.header.sequence) {
                    assert(a.header.checksum != b.header.checksum);
                    assert(a.header.cluster == b.header.cluster);
                    assert(a.header.vsr_state.replica_id == b.header.vsr_state.replica_id);

                    if (a.header.checksum != b.header.parent) {
                        return error.ParentNotConnected;
                    } else if (!a.header.vsr_state.monotonic(b.header.vsr_state)) {
                        return error.VSRStateNotMonotonic;
                    } else {
                        assert(b.header.valid_checksum());

                        return b;
                    }
                }
            }

            assert(b.header.valid_checksum());
            return b;
        }

        fn count_copy(
            quorums: *Quorums,
            copy: *const SuperBlockHeader,
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
            assert(quorum.header.checksum == copy.checksum);
            assert(quorum.header.equal(copy));

            if (copy.copy >= options.superblock_copies) {
                // This header is a valid member of the quorum, but with an unexpected copy number.
                // The "SuperBlockHeader.copy" field is not protected by the checksum, so if that byte
                // (and only that byte) is corrupted, the superblock is still valid — but we don't know
                // for certain which copy this was supposed to be.
                // We make the assumption that this was not a double-fault (corrupt + misdirect) —
                // that is, the copy is in the correct slot, and its copy index is simply corrupt.
                quorum.slots[slot] = @as(u8, @intCast(slot));
                quorum.copies.set(slot);
            } else if (quorum.copies.isSet(copy.copy)) {
                // Ignore the duplicate copy.
            } else {
                quorum.slots[slot] = @as(u8, @intCast(copy.copy));
                quorum.copies.set(copy.copy);
            }

            quorum.valid = quorum.copies.count() >= threshold.count();
        }

        fn find_or_insert_quorum_for_copy(quorums: *Quorums, copy: *const SuperBlockHeader) *Quorum {
            assert(copy.valid_checksum());

            for (quorums.array[0..quorums.count]) |*quorum| {
                if (copy.checksum == quorum.header.checksum) return quorum;
            } else {
                quorums.array[quorums.count] = Quorum{ .header = copy };
                quorums.count += 1;

                return &quorums.array[quorums.count - 1];
            }
        }

        fn slice(quorums: *Quorums) []Quorum {
            return quorums.array[0..quorums.count];
        }

        fn sort_priority_descending(_: void, a: Quorum, b: Quorum) bool {
            assert(a.header.checksum != b.header.checksum);

            if (a.valid and !b.valid) return true;
            if (b.valid and !a.valid) return false;

            if (a.header.sequence > b.header.sequence) return true;
            if (b.header.sequence > a.header.sequence) return false;

            if (a.copies.count() > b.copies.count()) return true;
            if (b.copies.count() > a.copies.count()) return false;

            // The sort order must be stable and deterministic:
            return a.header.checksum > b.header.checksum;
        }

        /// Repair a quorum's copies in the safest known order.
        /// Repair is complete when every copy is on-disk (not necessarily in its home slot).
        ///
        /// We must be careful when repairing superblock headers to avoid endangering our quorum if
        /// an additional fault occurs. We primarily guard against torn header writes — preventing a
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
                // 1. The slot holds no header, and the copy was not found anywhere.
                // 2. The slot holds no header, but its copy was found elsewhere.
                // 3. The slot holds a misdirected header, but that copy is in another slot as well.
                var a: ?u8 = null;
                var b: ?u8 = null;
                var c: ?u8 = null;
                for (iterator.slots, 0..) |slot, i| {
                    if (slot == null and !copies_any.isSet(i)) a = @as(u8, @intCast(i));
                    if (slot == null and copies_any.isSet(i)) b = @as(u8, @intCast(i));
                    if (slot) |slot_copy| {
                        if (slot_copy != i and copies_duplicate.isSet(slot_copy)) c = @as(u8, @intCast(i));
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
    var prng = std.rand.DefaultPrng.init(123);

    // Don't print warnings from the Quorums.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    try fuzz.fuzz_quorums_working(prng.random());
}

test "Quorum.repairs" {
    var prng = std.rand.DefaultPrng.init(123);

    // Don't print warnings from the Quorums.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    try fuzz.fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 4 });
    // TODO: Enable these once SuperBlockHeader is generic over its Constants.
    // try fuzz.fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 6 });
    // try fuzz.fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 8 });
}
