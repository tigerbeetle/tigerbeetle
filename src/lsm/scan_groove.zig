const std = @import("std");
const assert = std.debug.assert;

const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const ScanBufferType = @import("scan_buffer.zig").ScanBufferType;
const GridType = @import("grid.zig").GridType;

const Direction = @import("direction.zig").Direction;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;

pub fn ScanGrooveType(
    comptime Groove: type,
    comptime Storage: type,
    comptime scans_max: comptime_int,
) type {
    comptime assert(std.meta.fields(Groove.IndexTrees).len > 0);

    return struct {
        const Self = @This();
        const ScanBuffer = ScanBufferType(Storage);

        pub const Scan = ScanType(Groove, Storage, scans_max);
        pub const Fetcher = FetcherType(Scan, Groove, Storage);

        slots: std.BoundedArray(Scan, scans_max) = .{},
        fetcher: ?Fetcher = null,

        pub fn Tree(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            return std.meta.fieldInfo(Groove.IndexTrees, field).field_type;
        }

        pub fn CompositeKey(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            return Tree(field).Table.Key;
        }

        pub fn CompositeKeyPrefix(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            const Key = CompositeKey(field);
            return std.meta.fieldInfo(Key, .field).field_type;
        }

        fn ScanImplType(comptime field: std.meta.FieldEnum(Scan.Dispatcher)) type {
            return std.meta.fieldInfo(Scan.Dispatcher, field).field_type;
        }

        /// Initializes a Scan over the index specified by `index`, searching for an exact match.
        /// Produces the criteria equivalent to
        /// `WHERE field = $value`.
        ///
        /// Results are sorted by `direction`.
        pub fn equal(
            self: *Self,
            comptime index: std.meta.FieldEnum(Groove.IndexTrees),
            buffer: *const ScanBuffer,
            snapshot: u64,
            value: CompositeKeyPrefix(index),
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const field = comptime index_to_dispatcher(index);
            const ScanImpl = ScanImplType(field);
            return scan_add(
                self,
                field,
                ScanImpl.init(
                    self.tree(index),
                    buffer,
                    snapshot,
                    .{ .field = value, .timestamp = timestamp_range.min },
                    .{ .field = value, .timestamp = timestamp_range.max },
                    direction,
                ),
            );
        }

        /// Initializes a Scan over the index specified by `field`, searching for a range.
        /// Produces the criteria equivalent to
        /// `WHERE field BETWEEN $min AND $max` (inclusive range).
        ///
        /// Results are not sorted.
        pub fn between(
            self: *Self,
            comptime index: std.meta.FieldEnum(Groove.IndexTrees),
            buffer: *const ScanBuffer,
            snapshot: u64,
            min: CompositeKeyPrefix(index),
            max: CompositeKeyPrefix(index),
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const field = comptime index_to_dispatcher(index);
            const ScanImpl = ScanImplType(field);
            return scan_add(
                self,
                field,
                ScanImpl.init(
                    self.tree(index),
                    buffer,
                    snapshot,
                    .{ .field = min, .timestamp = timestamp_range.min },
                    .{ .field = max, .timestamp = timestamp_range.max },
                    direction,
                ),
            );
        }

        /// Initializes a Scan performing the union operation over multiple scans.
        /// E.g. S₁ ∪ S₂ ∪ Sₙ.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> OR <condition_2> OR <condition_N>`.
        ///
        /// All scans must yield results sorted in the same direction.
        pub fn set_union(
            self: *Self,
            streams: []*Scan,
            direction: Direction,
        ) *Scan {
            const ScanImpl = ScanImplType(.set_union);
            return scan_add(
                self,
                .set_union,
                ScanImpl.init(
                    streams,
                    direction,
                ),
            );
        }

        /// Initializes a Scan performing the intersection operation over multiple scans.
        /// E.g. S₁ ∩ S₂ ∩ Sₙ.
        /// Produces the criteria equivalent to
        /// WHERE <condition_1> AND <condition_2> AND <condition_N>`.
        ///
        /// All scans must yield results sorted in the same direction.
        pub fn set_intersection(
            self: *Self,
            streams: []*Scan,
            direction: Direction,
        ) *Scan {
            const ScanImpl = ScanImplType(.set_intersection);
            return scan_add(
                self,
                .set_intersection,
                ScanImpl.init(
                    streams,
                    direction,
                ),
            );
        }

        /// Initializes a Scan performing the difference (minus) of two scans.
        /// E.g. S₁ - S₂.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> AND NOT <condition_2>`.
        ///
        /// Both scans must yield results sorted in the same direction.
        pub fn set_difference(
            self: *Self,
            scan_a: *Scan,
            scan_b: *Scan,
            direction: Direction,
        ) *Scan {
            const ScanImpl = ScanImplType(.set_difference);
            return scan_add(
                self,
                .set_difference,
                ScanImpl.init(
                    &.{ scan_a, scan_b },
                    direction,
                ),
            );
        }

        pub inline fn reset(self: *Self) void {
            self.* = .{};
        }

        /// Initializes a `Fetcher` for loading objects from a scan.
        pub inline fn fetch(self: *Self, scan: *Scan, snapshot: u64) *Fetcher {
            assert(self.fetcher == null);
            self.fetcher = Fetcher.init(self.groove(), scan, snapshot);
            return &self.fetcher.?;
        }

        inline fn scan_add(
            self: *Self,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
            init_expression: ScanImplType(field),
        ) *Scan {
            // TODO: instead of panic, we need to expose `error{Overflow}` when
            // exceeding `scans_max`
            var scan = self.slots.addOneAssumeCapacity();
            scan.dispatcher = @unionInit(
                Scan.Dispatcher,
                @tagName(field),
                init_expression,
            );

            return scan;
        }

        inline fn groove(self: *Self) *Groove {
            return @fieldParentPtr(Groove, "scan", self);
        }

        inline fn tree(
            self: *Self,
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
        ) *Tree(field) {
            return &@field(self.groove().indexes, @tagName(field));
        }

        fn index_to_dispatcher(
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
        ) std.meta.FieldEnum(Scan.Dispatcher) {
            return std.meta.stringToEnum(
                std.meta.FieldEnum(Scan.Dispatcher),
                @tagName(field),
            ).?;
        }
    };
}

/// Common `Scan` interface.
///
/// Allows combining different underlying scans into a sigle output,
/// for example `(A₁ ∪ A₂) ∩ B` produces the criteria equivalent to
/// `WHERE (<condition_a1> OR <condition_a2>) AND <condition_b>`.
fn ScanType(
    comptime Groove: type,
    comptime Storage: type,
    comptime scans_max: comptime_int,
) type {
    comptime assert(std.meta.fields(Groove.IndexTrees).len > 0);

    return struct {
        const Scan = @This();

        /// This pattern of callback `fn(*Context, *Scan)` with the `Context` holding the function
        /// pointer is well suitable for this use case as it allows composing multiple scans from
        /// a pool without requiring the caller to keep track of the reference of the topmost scan.
        /// Example:
        /// ```
        /// var scan1: *Scan = scan_groove.equal(...); // Add some condition
        /// var scan2: *Scan = scan_groove.equal(...); // Add another condition
        /// var scan3: *Scan = scan_groove.set_union(scan1, scan2); // Merge both scans.
        /// scan3.read(&context); // This scan will be returned during the callback.
        /// ```
        pub const Callback = *const fn (*Context, *Scan) void;
        pub const Context = struct {
            callback: Callback,
        };

        pub const Dispatcher = DispatcherType();

        dispatcher: Dispatcher,

        pub inline fn read(scan: *Scan, context: *Context) void {
            switch (scan.dispatcher) {
                inline else => |*scan_tree, tag| read_dispatch(
                    tag,
                    scan_tree,
                    context,
                ),
            }
        }

        // Comptime generates an specialized callback function for each type.
        // TODO(Zig): it'd be nice to remove this function and mode this logic to `read`,
        // but for some reason, the Zig compiler can't resolve the correct type.
        inline fn read_dispatch(
            comptime tag: std.meta.Tag(Dispatcher),
            scan_tree: *std.meta.fieldInfo(Dispatcher, tag).field_type,
            context: *Context,
        ) void {
            const Impl = *std.meta.fieldInfo(Dispatcher, tag).field_type;

            scan_tree.read(context, struct {
                fn on_read_callback(ctx: *Context, ptr: Impl) void {
                    ctx.callback(ctx, parent(tag, ptr));
                }
            }.on_read_callback);
        }

        pub inline fn next(scan: *Scan) error{ReadAgain}!?u64 {
            return switch (scan.dispatcher) {
                // Comptime generates an specialized callback function for each type.
                inline else => |*scan_tree| if (try scan_tree.next()) |value|
                    timestamp(value)
                else
                    null,
            };
        }

        /// Returns the direction of the output timestamp values,
        /// or null if the scan can't yield sorted timestamps.
        inline fn timestamp_direction(scan: *Scan) ?Direction {
            switch (scan.dispatcher) {
                inline else => |*scan_tree| {
                    const ScanTree = @TypeOf(scan_tree.*);
                    if (@hasField(ScanTree, "key_min") and
                        @hasField(ScanTree, "key_max"))
                    {
                        const exact_match = scan_tree.key_min.field == scan_tree.key_max.field;
                        return if (exact_match) scan_tree.direction else null;
                    } else {
                        return scan_tree.direction;
                    }
                },
            }
        }

        // TODO(batiati): Move this function to stdx, so we can share the same
        // logic with PrefetchContext unions.
        //
        // TODO(Zig): No need for this cast once Zig is upgraded
        // and @fieldParentPtr() can be used for unions.
        // See: https://github.com/ziglang/zig/issues/6611.
        inline fn parent(
            comptime field: std.meta.Tag(Dispatcher),
            impl: anytype,
        ) *Scan {
            var stub = @unionInit(Dispatcher, @tagName(field), undefined);
            const stub_field_ptr = &@field(stub, @tagName(field));
            comptime assert(@TypeOf(stub_field_ptr) == @TypeOf(impl));

            const offset = @ptrToInt(stub_field_ptr) - @ptrToInt(&stub);
            const dispatcher_ptr = @intToPtr(*Dispatcher, @ptrToInt(impl) - offset);

            return @fieldParentPtr(Scan, "dispatcher", dispatcher_ptr);
        }

        inline fn timestamp(value: anytype) u64 {
            return if (@TypeOf(value) == u64)
                value
            else
                value.timestamp;
        }

        /// Generates a tagged union with an specialized ScanTree field for each
        /// index on the groove, plus one field for each `set` operation that shares
        /// the same interface.
        ///
        /// Example:
        /// ```
        /// const Dispatcher = union(enum) {
        ///   .code: ScanTree(...),
        ///   .ledger: ScanTree(...),
        ///   // ...
        ///   .set_union: ...
        ///   .set_intersection: ...
        ///   .set_difference: ...
        /// };
        /// ```
        fn DispatcherType() type {
            var type_info = std.builtin.Type{
                .Union = .{
                    .layout = .Auto,
                    .tag_type = null,
                    .fields = &.{},
                    .decls = &.{},
                },
            };

            // Union fields for each index tree:
            for (std.meta.fields(Groove.IndexTrees)) |field| {
                const IndexTree = field.field_type;
                const ScanTree = ScanTreeType(*Context, IndexTree, Storage);
                type_info.Union.fields = type_info.Union.fields ++
                    [_]std.builtin.Type.UnionField{.{
                    .name = field.name,
                    .field_type = ScanTree,
                    .alignment = @alignOf(ScanTree),
                }};
            }

            // Add fields for set operations that share the same interface:
            const ScanMergeUnion = ScanMergeUnionType(Scan, scans_max);
            type_info.Union.fields = type_info.Union.fields ++
                [_]std.builtin.Type.UnionField{.{
                .name = "set_union",
                .field_type = ScanMergeUnion,
                .alignment = @alignOf(ScanMergeUnion),
            }};
            const ScanMergeIntersection = ScanMergeIntersectionType(Scan, scans_max);
            type_info.Union.fields = type_info.Union.fields ++
                [_]std.builtin.Type.UnionField{.{
                .name = "set_intersection",
                .field_type = ScanMergeIntersection,
                .alignment = @alignOf(ScanMergeIntersection),
            }};
            const ScanMergeDifference = ScanMergeDifferenceType(Scan);
            type_info.Union.fields = type_info.Union.fields ++
                [_]std.builtin.Type.UnionField{.{
                .name = "set_difference",
                .field_type = ScanMergeDifference,
                .alignment = @alignOf(ScanMergeDifference),
            }};

            // We need a tagged union for dynamic dispatching.
            type_info.Union.tag_type = blk: {
                const union_fields = type_info.Union.fields;
                var tag_fields: [union_fields.len]std.builtin.Type.EnumField =
                    undefined;
                for (union_fields) |union_field, i| {
                    tag_fields[i] = .{
                        .name = union_field.name,
                        .value = i,
                    };
                }

                break :blk @Type(.{ .Enum = .{
                    .layout = .Auto,
                    .tag_type = std.math.IntFittingRange(0, tag_fields.len - 1),
                    .fields = &tag_fields,
                    .decls = &.{},
                    .is_exhaustive = true,
                } });
            };

            return @Type(type_info);
        }
    };
}

/// Union ∪ operation over an array of non-specialized `Scan` instances.
fn ScanMergeUnionType(
    comptime Scan: type,
    comptime scans_max: comptime_int,
) type {
    return struct {
        const Self = @This();
        pub const Callback = *const fn (context: *Scan.Context, self: *Self) void;

        const BoundedArray = std.BoundedArray(struct {
            scan: *Scan,
            popped: ?u64,
        }, scans_max);

        const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIteratorType(
            Self,
            u64,
            u64,
            merge_key_from_value,
            merge_compare_keys,
            scans_max,
            merge_stream_peek,
            merge_stream_pop,
            merge_stream_precedence,
        );

        direction: Direction,
        scan_context: Scan.Context = .{ .callback = on_scan_read },

        state: union(enum) {
            idle,
            seeking,
            fetching: struct {
                context: *Scan.Context,
                callback: Callback,
                pending_count: u32,
            },
        },
        scans: BoundedArray,

        merge_iterator: ?KWayMergeIterator,

        pub fn init(
            scans: []const *Scan,
            direction: Direction,
        ) Self {
            assert(scans.len > 0);
            assert(scans.len <= scans_max);

            for (scans) |scan| {
                // Union merge can be applied only in scans that yield sorted timestamps.
                const scan_direction = scan.timestamp_direction() orelse unreachable;

                // Assert that all scans have the same direction.
                assert(scan_direction == direction);
            }

            var self = Self{
                .direction = direction,
                .state = .idle,
                .scans = BoundedArray.init(0) catch unreachable,
                .merge_iterator = null,
            };

            for (scans) |scan| {
                self.scans.append(.{ .scan = scan, .popped = null }) catch unreachable;
            }

            return self;
        }

        pub fn read(self: *Self, context: *Scan.Context, callback: Callback) void {
            assert(self.state != .fetching);
            self.state = .{
                .fetching = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            for (self.scans.slice()) |*item| {
                self.state.fetching.pending_count += 1;
                item.scan.read(&self.scan_context);
            }
        }

        pub fn next(self: *Self) error{ReadAgain}!?u64 {
            return if (self.merge_iterator) |*it|
                it.pop() catch |err| switch (err) {
                    error.Drained => error.ReadAgain,
                }
            else
                error.ReadAgain;
        }

        fn on_scan_read(context: *Scan.Context, _: *Scan) void {
            const self = @fieldParentPtr(Self, "scan_context", context);
            assert(self.state == .fetching);

            self.state.fetching.pending_count -= 1;
            if (self.state.fetching.pending_count == 0) {
                const context_outer = self.state.fetching.context;
                const callback = self.state.fetching.callback;
                self.state = .seeking;

                if (self.merge_iterator == null) {
                    self.merge_iterator = KWayMergeIterator.init(
                        self,
                        @intCast(u32, self.scans.len),
                        self.direction,
                    );
                }
                callback(context_outer, self);
            }
        }

        inline fn merge_key_from_value(value: *const u64) u64 {
            return value.*;
        }

        inline fn merge_compare_keys(a: u64, b: u64) std.math.Order {
            return std.math.order(a, b);
        }

        fn merge_stream_peek(
            self: *Self,
            stream_index: u32,
        ) error{ Empty, Drained }!u64 {
            assert(stream_index < self.scans.len);

            var item = &self.scans.slice()[stream_index];
            if (item.popped == null) {
                item.popped = item.scan.next() catch |err| switch (err) {
                    error.ReadAgain => return error.Drained,
                };
            }
            return item.popped orelse error.Empty;
        }

        fn merge_stream_pop(
            self: *Self,
            stream_index: u32,
        ) u64 {
            assert(stream_index < self.scans.len);

            var item = &self.scans.slice()[stream_index];
            defer item.popped = null;

            return item.popped.?;
        }

        fn merge_stream_precedence(self: *const Self, a: u32, b: u32) bool {
            _ = self;
            return a < b;
        }
    };
}

/// Intersection ∩ operation over an array of non-specialized `Scan` instances.
fn ScanMergeIntersectionType(
    comptime Scan: type,
    comptime scans_max: comptime_int,
) type {
    // TODO: Implement intersection logic.
    return ScanMergeUnionType(Scan, scans_max);
}

/// Difference (minus) operation over two non-specialized `Scan` instances.
fn ScanMergeDifferenceType(comptime Scan: type) type {
    // TODO: Implement difference logic.
    return ScanMergeUnionType(Scan, 2);
}

/// EXPERIMENTAL:
/// Implements the lookup logic for loading objects directly from scans.
fn FetcherType(comptime Scan: type, comptime Groove: type, comptime Storage: type) type {
    return struct {
        const Self = @This();
        const Grid = GridType(Storage);

        pub const Object = Groove.ObjectTree.Table.Value;

        pub const Callback = *const fn (*Context, *Self) void;
        pub const Context = struct {
            callback: Callback,
        };

        const Lookup = struct {
            parent: *Self,
            lookup_context: Groove.ObjectTree.LookupContext = undefined,
            index_produced: ?usize = null,

            fn next(lookup: *Lookup) void {
                while (true) {
                    // We need to keep the order of the results, so each `Fetcher` acquires
                    // the index which will place the result into the buffer.
                    if (lookup.index_produced) |index| {
                        lookup.parent.buffer_produced_len = std.math.max(
                            lookup.parent.buffer_produced_len,
                            index + 1,
                        );
                        assert(lookup.parent.buffer_produced_len <= lookup.parent.buffer.len);
                    }

                    switch (lookup.parent.state) {
                        .idle => unreachable,
                        .scan, .finished => {
                            lookup.parent.lookup_finished();
                            return;
                        },
                        .lookup => {
                            lookup.index_produced = blk: {
                                const index_next = lookup.parent.buffer_producing_index + 1;
                                if (index_next > lookup.parent.buffer.len) {
                                    // The provided buffer was exhausted.
                                    lookup.parent.lookup_finished();
                                    return;
                                }
                                defer lookup.parent.buffer_producing_index = index_next;
                                break :blk lookup.parent.buffer_producing_index;
                            };

                            const timestamp = lookup.parent.scan.next() catch |err| switch (err) {
                                error.ReadAgain => {
                                    // The scan needs to be buffered again.
                                    lookup.parent.state = .scan;
                                    lookup.parent.lookup_finished();
                                    return;
                                },
                            } orelse {
                                // Reached the end of the scan.
                                lookup.parent.state = .finished;
                                lookup.parent.lookup_finished();
                                return;
                            };

                            switch (lookup.parent.groove.objects.lookup_from_memory(
                                lookup.lookup_snapshot(),
                                timestamp,
                            )) {
                                // Since the scan already found the key,
                                // we don't expected `negative` here.
                                .negative => unreachable,

                                // Object cached in memory,
                                // just continue the loop to fetch the next one.
                                .positive => |object| {
                                    lookup.parent.buffer[lookup.index_produced.?] = object.*;
                                    continue;
                                },

                                // Object need to be loaded from storage.
                                .possible => |level_min| {
                                    lookup.parent.groove.objects.lookup_from_levels_storage(.{
                                        .callback = lookup_callback,
                                        .context = &lookup.lookup_context,
                                        .snapshot = lookup.lookup_snapshot(),
                                        .key = timestamp,
                                        .level_min = level_min,
                                    });
                                    return;
                                },
                            }
                        },
                    }

                    // We can't run infinite loop.
                    unreachable;
                }
            }

            fn lookup_callback(
                completion: *Groove.ObjectTree.LookupContext,
                result: ?*const Object,
            ) void {
                const lookup = @fieldParentPtr(Lookup, "lookup_context", completion);
                lookup.lookup_context = undefined;

                assert(result != null);
                assert(lookup.index_produced != null);

                lookup.parent.buffer[lookup.index_produced.?] = result.?.*;
                lookup.next();
            }

            inline fn lookup_snapshot(lookup: *Lookup) u64 {
                return std.math.min(
                    lookup.parent.snapshot,
                    lookup.parent.groove.prefetch_snapshot.?,
                );
            }
        };

        groove: *Groove,
        context: *Context,
        scan_context: Scan.Context = .{ .callback = on_scan_read },

        scan: *Scan,
        snapshot: u64,
        buffer: []Object,
        buffer_produced_len: usize,
        buffer_producing_index: usize,
        state: enum { idle, scan, lookup, finished },

        lookups: [Grid.read_iops_max]Lookup = undefined,
        lookups_pending: u32 = 0,

        pub fn init(
            groove: *Groove,
            scan: *Scan,
            snapshot: u64,
        ) Self {
            return .{
                .groove = groove,
                .context = undefined,
                .scan = scan,
                .snapshot = snapshot,
                .buffer = undefined,
                .buffer_produced_len = 0,
                .buffer_producing_index = 0,
                .state = .idle,
            };
        }

        pub fn read(
            self: *Self,
            context: *Context,
            buffer: []Object,
        ) void {
            assert(self.state == .idle);

            self.context = context;
            self.buffer = buffer;
            self.buffer_produced_len = 0;

            self.state = .scan;
            self.scan.read(&self.scan_context);
        }

        pub inline fn slice(self: *const Self) []const Object {
            assert(self.state == .idle or self.state == .finished);
            return self.buffer[0..self.buffer_produced_len];
        }

        pub inline fn has_more(self: *const Self) bool {
            return switch (self.state) {
                .idle => true,
                .scan, .lookup => unreachable,
                .finished => false,
            };
        }

        fn on_scan_read(context: *Scan.Context, scan: *Scan) void {
            var self = @fieldParentPtr(Self, "scan_context", context);
            assert(self.state == .scan);
            assert(self.scan == scan);

            self.state = .lookup;
            self.start_lookup();
        }

        fn start_lookup(self: *Self) void {
            assert(self.state == .lookup);
            assert(self.lookups_pending == 0);

            self.buffer_producing_index = self.buffer_produced_len;
            self.lookups_pending += 1;

            for (self.lookups) |*lookup| {
                lookup.* = .{ .parent = self };
                self.lookups_pending += 1;
                lookup.next();
            }

            assert(self.lookups_pending >= 1);
            self.lookup_finished();
        }

        fn lookup_finished(self: *Self) void {
            assert(self.lookups_pending > 0);

            self.lookups_pending -= 1;
            if (self.lookups_pending == 0) {
                switch (self.state) {
                    .idle => unreachable,
                    .scan => {
                        self.scan.read(&self.scan_context);
                        return;
                    },
                    .lookup => self.state = .idle,
                    .finished => {},
                }

                self.context.callback(self.context, self);
            }
        }
    };
}
