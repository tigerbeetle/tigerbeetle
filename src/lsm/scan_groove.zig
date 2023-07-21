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
    comptime scan_max: comptime_int,
) type {
    comptime assert(std.meta.fields(Groove.IndexTrees).len > 0);

    return struct {
        const Self = @This();
        const ScanBuffer = ScanBufferType(Storage);

        pub const Scan = ScanType(Groove, Storage, scan_max);
        pub const Fetcher = FetcherType(Scan, Groove, Storage);

        slots: [scan_max]Scan = undefined,
        slots_used: u32 = 0,

        fetcher: Fetcher = undefined,

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
            self.fetcher = Fetcher.init(self.groove(), scan, snapshot);
            return &self.fetcher;
        }

        inline fn scan_add(
            self: *Self,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
            init_expression: ScanImplType(field),
        ) *Scan {
            assert(self.slots_used < scan_max);
            self.slots[self.slots_used].dispatcher = @unionInit(
                Scan.Dispatcher,
                @tagName(field),
                init_expression,
            );
            defer self.slots_used += 1;

            return &self.slots[self.slots_used];
        }

        inline fn groove(self: *Self) *Groove {
            return @fieldParentPtr(Groove, "scan", self);
        }

        inline fn tree(self: *Self, comptime field: std.meta.FieldEnum(Groove.IndexTrees)) *Tree(field) {
            return &@field(self.groove().indexes, @tagName(field));
        }

        fn index_to_dispatcher(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) std.meta.FieldEnum(Scan.Dispatcher) {
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
        pub const Callback = fn (*Context, *Scan) void;
        pub const Context = struct {
            callback: Callback,
        };

        pub const Dispatcher = DispatcherType();

        dispatcher: Dispatcher,

        pub inline fn read(scan: *Scan, context: *Context) void {
            // TODO(zig): Replace with inline switch:
            // https://github.com/ziglang/zig/issues/7224.
            const Tag = std.meta.Tag(Dispatcher);
            const active_tag = std.meta.activeTag(scan.dispatcher);
            inline for (std.meta.fields(Tag)) |field| {
                if (active_tag == @intToEnum(Tag, field.value)) {
                    var scan_tree = &@field(scan.dispatcher, field.name);
                    read_dispatch(scan_tree, context);
                }
            }
        }

        pub inline fn next(scan: *Scan) error{ReadAgain}!?u64 {
            // TODO(zig): Replace with inline switch:
            // https://github.com/ziglang/zig/issues/7224.
            const Tag = std.meta.Tag(Dispatcher);
            const active_tag = std.meta.activeTag(scan.dispatcher);
            inline for (std.meta.fields(Tag)) |field| {
                if (active_tag == @intToEnum(Tag, field.value)) {
                    var scan_tree = &@field(scan.dispatcher, field.name);
                    return if (try scan_tree.next()) |value| timestamp(value) else null;
                }
            } else unreachable;
        }

        /// Returns the direction of the output timestamp values,
        /// or null if the scan can't yield sorted timestamps.
        inline fn timestamp_direction(scan: *Scan) ?Direction {
            // TODO(zig): Replace with inline switch:
            // https://github.com/ziglang/zig/issues/7224.
            const Tag = std.meta.Tag(Dispatcher);
            const active_tag = std.meta.activeTag(scan.dispatcher);
            inline for (std.meta.fields(Tag)) |field| {
                if (active_tag == @intToEnum(Tag, field.value)) {
                    const ScanTree = @TypeOf(@field(scan.dispatcher, field.name));
                    var scan_tree = &@field(scan.dispatcher, field.name);
                    if (@hasField(ScanTree, "key_min") and @hasField(ScanTree, "key_max")) {
                        const exact_match = scan_tree.key_min.field == scan_tree.key_max.field;
                        return if (exact_match) scan_tree.direction else null;
                    } else {
                        return scan_tree.direction;
                    }
                }
            } else unreachable;
        }

        /// Comptime generates an specialized callback function for each type.
        inline fn read_dispatch(impl: anytype, context: *Context) void {
            const Impl = @TypeOf(impl);
            comptime assert(std.meta.trait.isSingleItemPtr(Impl));

            impl.read(context, struct {
                fn on_read_callback(ctx: *Context, ptr: Impl) void {
                    // TODO(Zig): No need for this cast once Zig is upgraded
                    // and @fieldParentPtr() can be used for unions.
                    // See: https://github.com/ziglang/zig/issues/6611.
                    const dispatcher =
                        @ptrCast(*Dispatcher, @alignCast(@alignOf(Dispatcher), ptr));
                    const scan = @fieldParentPtr(Scan, "dispatcher", dispatcher);
                    ctx.callback(ctx, scan);
                }
            }.on_read_callback);
        }

        inline fn timestamp(value: anytype) u64 {
            return if (@TypeOf(value) == u64)
                value
            else
                value.timestamp;
        }

        /// Generates a tagged union with an specialized ScanTree field for each index on the groove,
        /// plus one field for each `set` operation that shares the same interface.
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
            comptime var type_info = std.builtin.TypeInfo{
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
                type_info.Union.fields = type_info.Union.fields ++ [_]std.builtin.TypeInfo.UnionField{.{
                    .name = field.name,
                    .field_type = ScanTree,
                    .alignment = @alignOf(ScanTree),
                }};
            }

            // Add fields for set operations that share the same interface:
            const ScanMergeUnion = ScanMergeUnionType(Scan, scans_max);
            type_info.Union.fields = type_info.Union.fields ++ [_]std.builtin.TypeInfo.UnionField{.{
                .name = "set_union",
                .field_type = ScanMergeUnion,
                .alignment = @alignOf(ScanMergeUnion),
            }};
            const ScanMergeIntersection = ScanMergeIntersectionType(Scan, scans_max);
            type_info.Union.fields = type_info.Union.fields ++ [_]std.builtin.TypeInfo.UnionField{.{
                .name = "set_intersection",
                .field_type = ScanMergeIntersection,
                .alignment = @alignOf(ScanMergeIntersection),
            }};
            const ScanMergeDifference = ScanMergeDifferenceType(Scan);
            type_info.Union.fields = type_info.Union.fields ++ [_]std.builtin.TypeInfo.UnionField{.{
                .name = "set_difference",
                .field_type = ScanMergeDifference,
                .alignment = @alignOf(ScanMergeDifference),
            }};

            // We need a tagged union for dynamic dispatching.
            type_info.Union.tag_type = blk: {
                comptime var tag_fields: [type_info.Union.fields.len]std.builtin.TypeInfo.EnumField = undefined;
                for (type_info.Union.fields) |union_field, i| {
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
        pub const Callback = fn (context: *Scan.Context, self: *Self) void;

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

        pub const Callback = fn (*Context, *Self) void;
        pub const Context = struct {
            callback: Callback,
        };

        const Lookup = struct {
            parent: *Self,
            lookup_context: Groove.ObjectTree.LookupContext = undefined,
            index_produced: ?usize = null,

            fn next(lookup: *Lookup) void {
                // We need to keep the order of the results, so each `Fetcher` acquires the index
                // which will place the result into the buffer.
                if (lookup.index_produced) |index| {
                    lookup.parent.buffer_produced_len = std.math.max(
                        lookup.parent.buffer_produced_len,
                        index + 1,
                    );
                    assert(lookup.parent.buffer_produced_len <= lookup.parent.buffer.len);
                }

                switch (lookup.parent.state) {
                    .idle => unreachable,
                    .scan, .finished => return lookup.parent.lookup_finished(),
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
                                return lookup.parent.lookup_finished();
                            },
                        } orelse {
                            // Reached the end of the scan.
                            lookup.parent.state = .finished;
                            return lookup.parent.lookup_finished();
                        };

                        lookup.lookup_with_timestamp(timestamp);
                    },
                }
            }

            fn lookup_with_timestamp(lookup: *Lookup, timestamp: u64) void {
                assert(lookup.index_produced != null);

                if (lookup.parent.groove.objects.lookup_from_memory(
                    lookup.lookup_snapshot(),
                    timestamp,
                )) |object| {
                    lookup.parent.buffer[lookup.index_produced.?] = object.*;
                    lookup.next();
                    return;
                }

                lookup.parent.groove.objects.lookup_from_levels(
                    lookup_callback,
                    &lookup.lookup_context,
                    lookup.lookup_snapshot(),
                    timestamp,
                );
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
