const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const stdx = @import("stdx");
const constants = @import("../constants.zig");

const ScanState = @import("scan_state.zig").ScanState;
const Direction = @import("../direction.zig").Direction;
const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;
const ZigZagMergeIteratorType = @import("zig_zag_merge.zig").ZigZagMergeIteratorType;
const ScanType = @import("scan_builder.zig").ScanType;
const Pending = error{Pending};

/// Union ∪ operation over an array of non-specialized `Scan` instances.
/// At a high level, this is an ordered iterator over the set-union of the timestamps of
/// each of the component Scans.
pub fn ScanMergeUnionType(comptime Groove: type, comptime Storage: type) type {
    return ScanMergeType(Groove, Storage, .merge_union);
}

/// Intersection ∩ operation over an array of non-specialized `Scan` instances.
pub fn ScanMergeIntersectionType(comptime Groove: type, comptime Storage: type) type {
    return ScanMergeType(Groove, Storage, .merge_intersection);
}

/// Difference (minus) operation over two non-specialized `Scan` instances.
pub fn ScanMergeDifferenceType(comptime Groove: type, comptime Storage: type) type {
    return ScanMergeType(Groove, Storage, .merge_intersection);
}

fn ScanMergeType(
    comptime Groove: type,
    comptime Storage: type,
    comptime merge: enum {
        merge_union,
        merge_intersection,
        merge_difference,
    },
) type {
    return struct {
        const ScanMerge = @This();
        const Scan = ScanType(Groove, Storage);

        pub const Callback = *const fn (context: *Scan.Context, self: *ScanMerge) void;

        /// Adapts the `Scan` interface into a peek/pop stream required by the merge iterator.
        const MergeScanStream = struct {
            scan: *Scan,
            current: ?u64 = null,

            fn peek(self: *MergeScanStream) Pending!?u64 {
                if (self.current == null) {
                    self.current = try self.scan.next();
                }
                maybe(self.current == null);
                return self.current;
            }

            fn pop(self: *MergeScanStream) u64 {
                assert(self.current != null);
                defer self.current = null;

                return self.current.?;
            }

            fn probe(self: *MergeScanStream, timestamp: u64) void {
                if (self.current != null and
                    switch (self.scan.direction()) {
                        .ascending => self.current.? >= timestamp,
                        .descending => self.current.? <= timestamp,
                    })
                {
                    // The scan may be in a key ahead of the probe key.
                    // E.g. `WHERE P AND (A OR B) ORDER BY ASC`:
                    //  - `P` yields key 2, which is the probe key;
                    //  - `A` yields key 1;
                    //  - `B` yields key 10
                    //  - `KWayMerge(A,B)` yields key 1 and it is probed with key 2 from `P`;
                    //  - `A` needs to move to a key >= 2;
                    //  - `B` is already positioned at key >= 2, no probing is required;
                    assert(self.scan.state() == .seeking);
                    return;
                }

                self.current = null;
                self.scan.probe(timestamp);
            }
        };

        const KWayMergeIterator = KWayMergeIteratorType(
            ScanMerge,
            u64,
            u64,
            .{
                .streams_max = constants.lsm_scans_max,
                .deduplicate = true,
            },
            key_from_value,
            merge_stream_peek,
            merge_stream_pop,
        );

        const ZigZagMergeIterator = ZigZagMergeIteratorType(
            ScanMerge,
            u64,
            u64,
            key_from_value,
            constants.lsm_scans_max,
            merge_stream_peek,
            merge_stream_pop,
            merge_stream_probe,
        );

        direction: Direction,
        snapshot: u64,
        scan_context: Scan.Context = .{ .callback = &scan_read_callback },

        state: union(ScanState) {
            /// The scan has not been executed yet.
            /// The underlying scans are still uninitialized or in the state `.idle`.
            idle,

            /// The scan is at a valid position and ready to yield values, e.g. calling `next()`.
            /// All underlying scans are in the state `.seeking`.
            seeking,

            /// The scan needs to load data from the underlying scans, e.g. calling `read()`.
            /// At least one underlying scan is in the state `.needs_data`, while other ones may
            /// be in the state `.seeking`.
            needs_data,

            /// The scan is attempting to load data from the underlying scans,
            /// e.g. in between calling `read()` and receiving the callback.
            /// The underlying scans are either in the state `.buffering` or `.seeking`.
            buffering: struct {
                context: *Scan.Context,
                callback: Callback,
                pending_count: u32,
            },

            /// The scan was aborted and will not yield any more values.
            aborted,
        },
        streams: stdx.BoundedArrayType(MergeScanStream, constants.lsm_scans_max),

        merge_iterator: ?switch (merge) {
            .merge_union => KWayMergeIterator,
            .merge_intersection => ZigZagMergeIterator,
            .merge_difference => stdx.unimplemented("merge_difference"),
        },

        pub fn init(scans: []const *Scan) ScanMerge {
            assert(scans.len > 0);
            assert(scans.len <= constants.lsm_scans_max);

            const direction_first = scans[0].direction();
            const snapshot_first = scans[0].snapshot();

            if (scans.len > 1) for (scans[1..]) |scan| {
                // Merge can be applied only in scans that yield timestamps sorted in the
                // same direction.
                assert(scan.direction() == direction_first);

                // All scans must have the same snapshot.
                assert(scan.snapshot() == snapshot_first);
            };

            var self = ScanMerge{
                .direction = direction_first,
                .snapshot = snapshot_first,
                .state = .idle,
                .streams = .{},
                .merge_iterator = null,
            };

            for (scans) |scan| {
                assert(scan.assigned == false);
                assert(scan.state() == .idle);

                // Mark this scan as `assigned`, so it can't be used to compose other merges.
                scan.assigned = true;
                self.streams.push(.{ .scan = scan });
            }

            return self;
        }

        pub fn read(self: *ScanMerge, context: *Scan.Context, callback: Callback) void {
            assert(self.state == .idle or self.state == .needs_data);
            assert(self.streams.count() > 0);

            const state_before = self.state;
            self.state = .{
                .buffering = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            for (self.streams.slice()) |*stream| {
                switch (stream.scan.state()) {
                    .idle => assert(state_before == .idle),
                    .seeking => continue,
                    .needs_data => assert(state_before == .needs_data),
                    .buffering, .aborted => unreachable,
                }

                self.state.buffering.pending_count += 1;
                stream.scan.read(&self.scan_context);
            }
            assert(self.state.buffering.pending_count > 0);
        }

        /// Moves the iterator to the next position and returns its `Value` or `null` if the
        /// iterator has no more values to iterate.
        /// May return `error.Pending` if the scan needs to be loaded, in this case
        /// call `read()` and resume the iteration after the read callback.
        pub fn next(self: *ScanMerge) Pending!?u64 {
            switch (self.state) {
                .idle => {
                    assert(self.merge_iterator == null);
                    return error.Pending;
                },
                .seeking => return self.merge_iterator.?.pop() catch |err| switch (err) {
                    error.Pending => {
                        self.state = .needs_data;
                        return error.Pending;
                    },
                },
                .needs_data => return error.Pending,
                .buffering, .aborted => unreachable,
            }
        }

        pub fn probe(self: *ScanMerge, timestamp: u64) void {
            switch (self.state) {
                .idle, .seeking, .needs_data => {
                    // Forwards the `probe` call to the underlying streams,
                    // leaving the merge state unchanged.
                    // That is, `probe` changes the range key_min/key_max of the scan, but the key
                    // may have already been buffered, so the state can be preserved since fetching
                    // data from storage is not always required after a `probe`.
                    for (self.streams.slice()) |*stream| {
                        stream.probe(timestamp);
                    }

                    if (self.merge_iterator) |*merge_iterator| {
                        if (merge_iterator.key_popped) |key_popped| {
                            // The new timestamp may lag behind the merge_iterator's latest key.
                            //
                            // Suppose there is a query:
                            //   (index_0 AND (index_1 OR (index_2 AND index_3)))
                            // with the listed timestamps in each index:
                            //
                            //   zig_zag_merge₁:     [              13, 13     ]
                            //     tree₀:            [          12, 12, 13     ]
                            //     k_way_merge₁:     [ 2, 2, 3,     12, 13, 14 ]
                            //       tree₁:          [ 2, 2, 3,                ]
                            //       zig_zag_merge₂: [              13, 13, 14 ]
                            //         tree₂:        [ 2, 2, 3,     12, 13, 14 ]
                            //         tree₃:        [              13, 13, 14 ]
                            //
                            // 1. While peeking the first key from zig_zag_merge₁, we peek 12 from
                            //    tree₀ and 1 from k_way_merge₁. So we probe k_way_merge₁ with 12.
                            // 2. k_way_merge₁ relays 11 to its streams (tree₁ + zig_zag_merge₂).
                            // 3. Probing zig_zag_merge₂ with 12 trips the assert, because tree₃ has
                            //    already produced a higher key (11 < 12).
                            switch (self.direction) {
                                .ascending => maybe(key_popped < timestamp),
                                .descending => maybe(key_popped > timestamp),
                            }
                        }

                        // Once the underlying streams have been changed, the merge iterator needs
                        // to reset its state, otherwise it may have dirty keys buffered.
                        merge_iterator.reset();
                    } else {
                        assert(self.state == .idle);
                    }
                },
                .buffering => unreachable,
                .aborted => return,
            }
        }

        fn scan_read_callback(context: *Scan.Context, scan: *Scan) void {
            const self: *ScanMerge = @fieldParentPtr("scan_context", context);
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count > 0);
            assert(self.state.buffering.pending_count <= self.streams.count());

            if (constants.verify) {
                assert(found: {
                    for (self.streams.const_slice()) |*stream| {
                        if (stream.scan == scan) break :found true;
                    } else break :found false;
                });
            }

            self.state.buffering.pending_count -= 1;
            if (self.state.buffering.pending_count == 0) {
                const context_outer = self.state.buffering.context;
                const callback = self.state.buffering.callback;
                self.state = .seeking;

                if (self.merge_iterator == null) {
                    self.merge_iterator = switch (merge) {
                        .merge_union => KWayMergeIterator.init(
                            self,
                            @intCast(self.streams.count()),
                            self.direction,
                        ),
                        .merge_intersection => ZigZagMergeIterator.init(
                            self,
                            @intCast(self.streams.count()),
                            self.direction,
                        ),
                        .merge_difference => unreachable,
                    };
                }
                callback(context_outer, self);
            }
        }

        inline fn key_from_value(value: *const u64) u64 {
            return value.*;
        }

        fn merge_stream_peek(
            self: *ScanMerge,
            stream_index: u32,
        ) Pending!?u64 {
            assert(stream_index < self.streams.count());

            var stream = &self.streams.slice()[stream_index];
            return stream.peek();
        }

        fn merge_stream_pop(
            self: *ScanMerge,
            stream_index: u32,
        ) u64 {
            assert(stream_index < self.streams.count());

            var stream = &self.streams.slice()[stream_index];
            return stream.pop();
        }

        fn merge_stream_probe(
            self: *ScanMerge,
            stream_index: u32,
            timestamp: u64,
        ) void {
            assert(stream_index < self.streams.count());

            var stream = &self.streams.slice()[stream_index];
            stream.probe(timestamp);
        }
    };
}
