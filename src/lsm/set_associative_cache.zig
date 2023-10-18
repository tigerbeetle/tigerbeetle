const std = @import("std");
const builtin = @import("builtin");

const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
const verify = constants.verify;

const tracer = @import("../tracer.zig");

pub const Layout = struct {
    ways: u64 = 16,
    tag_bits: u64 = 8,
    clock_bits: u64 = 2,
    cache_line_size: u64 = 64,
    /// Set this to a non-null value to override the alignment of the stored values.
    value_alignment: ?u29 = null,
};

const TracerStats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
};

/// Each Key is associated with a set of n consecutive ways (or slots) that may contain the Value.
pub fn SetAssociativeCacheType(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime hash: fn (Key) callconv(.Inline) u64,
    comptime layout: Layout,
) type {
    assert(math.isPowerOfTwo(@sizeOf(Key)));
    assert(math.isPowerOfTwo(@sizeOf(Value)));

    switch (layout.ways) {
        // An 8-way set-associative cache has the clock hand as a u3, which would introduce padding.
        2, 4, 16 => {},
        else => @compileError("ways must be 2, 4 or 16 for optimal CLOCK hand size."),
    }
    switch (layout.tag_bits) {
        8, 16 => {},
        else => @compileError("tag_bits must be 8 or 16."),
    }
    switch (layout.clock_bits) {
        1, 2, 4 => {},
        else => @compileError("clock_bits must be 1, 2 or 4."),
    }

    if (layout.value_alignment) |alignment| {
        assert(alignment >= @alignOf(Value));
        assert(@sizeOf(Value) % alignment == 0);
    }
    const value_alignment = layout.value_alignment orelse @alignOf(Value);

    assert(math.isPowerOfTwo(layout.ways));
    assert(math.isPowerOfTwo(layout.tag_bits));
    assert(math.isPowerOfTwo(layout.clock_bits));
    assert(math.isPowerOfTwo(layout.cache_line_size));

    assert(@sizeOf(Key) <= @sizeOf(Value));
    assert(@sizeOf(Key) < layout.cache_line_size);
    assert(layout.cache_line_size % @sizeOf(Key) == 0);

    if (layout.cache_line_size > @sizeOf(Value)) {
        assert(layout.cache_line_size % @sizeOf(Value) == 0);
    } else {
        assert(@sizeOf(Value) % layout.cache_line_size == 0);
    }

    const clock_hand_bits = math.log2_int(u64, layout.ways);
    assert(math.isPowerOfTwo(clock_hand_bits));
    assert((1 << clock_hand_bits) == layout.ways);

    const tags_per_line = @divExact(layout.cache_line_size * 8, layout.ways * layout.tag_bits);
    assert(tags_per_line > 0);

    const clocks_per_line = @divExact(layout.cache_line_size * 8, layout.ways * layout.clock_bits);
    assert(clocks_per_line > 0);

    const clock_hands_per_line = @divExact(layout.cache_line_size * 8, clock_hand_bits);
    assert(clock_hands_per_line > 0);

    return struct {
        const Self = @This();

        const Tag = meta.Int(.unsigned, layout.tag_bits);
        const Count = meta.Int(.unsigned, layout.clock_bits);
        const Clock = meta.Int(.unsigned, clock_hand_bits);

        /// We don't require `value_count_max` in `init` to be a power of 2, but we do require
        /// it to be a multiple of `value_count_max_multiple`. The calculation below
        /// follows from a multiple which will satisfy all asserts.
        pub const value_count_max_multiple = layout.cache_line_size * layout.ways * layout.clock_bits;

        name: []const u8,
        sets: u64,

        tracer_stats: *TracerStats,

        /// A short, partial hash of a Key, corresponding to a Value.
        /// Because the tag is small, collisions are possible:
        /// `tag(v₁) = tag(v₂)` does not imply `v₁ = v₂`.
        /// However, most of the time, where the tag differs, a full key comparison can be avoided.
        /// Since tags are 16-32x smaller than keys, they can also be kept hot in cache.
        tags: []Tag,

        /// When the corresponding Count is zero, the Value is absent.
        values: []align(value_alignment) Value,

        /// Each value has a Count, which tracks the number of recent reads.
        ///
        /// * A Count is incremented when the value is accessed by `get`.
        /// * A Count is decremented when a cache write to the value's Set misses.
        /// * The value is evicted when its Count reaches zero.
        ///
        counts: PackedUnsignedIntegerArray(Count),

        /// Each set has a Clock: a counter that cycles between each of the set's ways (i.e. slots).
        ///
        /// On cache write, entries are checked for occupancy (or eviction) beginning from the
        /// clock's position, wrapping around.
        ///
        /// The algorithm implemented is "CLOCK Nth-Chance" — each way has more than one bit,
        /// to give ways more than one chance before eviction.
        ///
        /// * A similar algorithm called "RRIParoo" is described in
        ///   "Kangaroo: Caching Billions of Tiny Objects on Flash".
        /// * For more general information on CLOCK algorithms, see:
        ///   https://en.wikipedia.org/wiki/Page_replacement_algorithm.
        clocks: PackedUnsignedIntegerArray(Clock),

        pub const Options = struct { name: []const u8 };

        pub fn init(allocator: mem.Allocator, value_count_max: u64, options: Options) !Self {
            const sets = @divExact(value_count_max, layout.ways);

            assert(value_count_max > 0);
            assert(value_count_max >= layout.ways);
            assert(value_count_max % layout.ways == 0);

            const values_size_max = value_count_max * @sizeOf(Value);
            assert(values_size_max >= layout.cache_line_size);
            assert(values_size_max % layout.cache_line_size == 0);

            const counts_size = @divExact(value_count_max * layout.clock_bits, 8);
            assert(counts_size >= layout.cache_line_size);
            assert(counts_size % layout.cache_line_size == 0);

            const clocks_size = @divExact(sets * clock_hand_bits, 8);
            assert(clocks_size >= layout.cache_line_size);
            assert(clocks_size % layout.cache_line_size == 0);

            assert(value_count_max % value_count_max_multiple == 0);

            const tags = try allocator.alloc(Tag, value_count_max);
            errdefer allocator.free(tags);

            const values = try allocator.alignedAlloc(
                Value,
                value_alignment,
                value_count_max,
            );
            errdefer allocator.free(values);

            const counts = try allocator.alloc(u64, @divExact(counts_size, @sizeOf(u64)));
            errdefer allocator.free(counts);

            const clocks = try allocator.alloc(u64, @divExact(clocks_size, @sizeOf(u64)));
            errdefer allocator.free(clocks);

            // Explicitly allocated so that get / get_index can be `*const Self`.
            const tracer_stats = try allocator.create(TracerStats);
            errdefer allocator.destroy(tracer_stats);

            var self = Self{
                .name = options.name,
                .sets = sets,
                .tags = tags,
                .values = values,
                .counts = .{ .words = counts },
                .clocks = .{ .words = clocks },
                .tracer_stats = tracer_stats,
            };

            self.reset();

            return self;
        }

        pub fn deinit(self: *Self, allocator: mem.Allocator) void {
            assert(self.sets > 0);
            self.sets = 0;

            allocator.free(self.tags);
            allocator.free(self.values);
            allocator.free(self.counts.words);
            allocator.free(self.clocks.words);
            allocator.destroy(self.tracer_stats);
        }

        pub fn reset(self: *Self) void {
            @memset(self.tags, 0);
            @memset(self.counts.words, 0);
            @memset(self.clocks.words, 0);
            self.tracer_stats.* = .{};
        }

        pub fn get_index(self: *const Self, key: Key) ?usize {
            const set = self.associate(key);
            if (self.search(set, key)) |way| {
                self.tracer_stats.hits += 1;
                tracer.plot(
                    .{ .cache_hits = .{ .cache_name = self.name } },
                    @as(f64, @floatFromInt(self.tracer_stats.hits)),
                );
                const count = self.counts.get(set.offset + way);
                self.counts.set(set.offset + way, count +| 1);
                return set.offset + way;
            } else {
                self.tracer_stats.misses += 1;
                tracer.plot(
                    .{ .cache_misses = .{ .cache_name = self.name } },
                    @as(f64, @floatFromInt(self.tracer_stats.misses)),
                );
                return null;
            }
        }

        pub fn get(self: *const Self, key: Key) ?*align(value_alignment) Value {
            const index = self.get_index(key) orelse return null;
            return @alignCast(&self.values[index]);
        }

        /// Remove a key from the set associative cache if present.
        /// Returns the removed value, if any.
        pub fn remove(self: *Self, key: Key) ?Value {
            const set = self.associate(key);
            const way = self.search(set, key) orelse return null;

            const removed: Value = set.values[way];
            self.counts.set(set.offset + way, 0);
            set.values[way] = undefined;

            return removed;
        }

        /// Hint that the key is less likely to be accessed in the future, without actually removing
        /// it from the cache.
        pub fn demote(self: *Self, key: Key) void {
            const set = self.associate(key);
            const way = self.search(set, key) orelse return;

            self.counts.set(set.offset + way, 1);
        }

        /// If the key is present in the set, returns the way. Otherwise returns null.
        inline fn search(self: *const Self, set: Set, key: Key) ?usize {
            const ways = search_tags(set.tags, set.tag);

            var it = BitIterator(Ways){ .bits = ways };
            while (it.next()) |way| {
                const count = self.counts.get(set.offset + way);
                if (count > 0 and key_from_value(&set.values[way]) == key) {
                    return way;
                }
            }

            return null;
        }

        /// Where each set bit represents the index of a way that has the same tag.
        const Ways = meta.Int(.unsigned, layout.ways);

        inline fn search_tags(tags: *const [layout.ways]Tag, tag: Tag) Ways {
            const x: @Vector(layout.ways, Tag) = tags.*;
            const y: @Vector(layout.ways, Tag) = @splat(tag);

            const result: @Vector(layout.ways, bool) = x == y;
            return @as(*const Ways, @ptrCast(&result)).*;
        }

        /// Upsert a value, evicting an older entry if needed. The evicted value, if an update or
        /// insert was performed and the index at which the value was inserted is returned.
        pub fn upsert(self: *Self, value: *const Value) struct {
            index: usize,
            updated: UpdateOrInsert,
            evicted: ?Value,
        } {
            const key = key_from_value(value);
            const set = self.associate(key);
            if (self.search(set, key)) |way| {
                // Overwrite the old entry for this key.
                self.counts.set(set.offset + way, 1);
                const evicted = set.values[way];
                set.values[way] = value.*;
                return .{
                    .index = set.offset + way,
                    .updated = .update,
                    .evicted = evicted,
                };
            }

            const clock_index = @divExact(set.offset, layout.ways);

            var way = self.clocks.get(clock_index);
            comptime assert(math.maxInt(@TypeOf(way)) == layout.ways - 1);
            comptime assert(@as(@TypeOf(way), math.maxInt(@TypeOf(way))) +% 1 == 0);

            // The maximum number of iterations happens when every slot in the set has the maximum
            // count. In this case, the loop will iterate until all counts have been decremented
            // to 1. Then in the next iteration it will decrement a count to 0 and break.
            const clock_iterations_max = layout.ways * (math.maxInt(Count) - 1);

            var evicted: ?Value = null;
            var safety_count: usize = 0;
            while (safety_count <= clock_iterations_max) : ({
                safety_count += 1;
                way +%= 1;
            }) {
                var count = self.counts.get(set.offset + way);
                if (count == 0) break; // Way is already free.

                count -= 1;
                self.counts.set(set.offset + way, count);
                if (count == 0) {
                    // Way has become free.
                    evicted = set.values[way];
                    break;
                }
            } else {
                unreachable;
            }
            assert(self.counts.get(set.offset + way) == 0);

            set.tags[way] = set.tag;
            set.values[way] = value.*;
            self.counts.set(set.offset + way, 1);
            self.clocks.set(clock_index, way +% 1);

            return .{
                .index = set.offset + way,
                .updated = .insert,
                .evicted = evicted,
            };
        }

        const Set = struct {
            tag: Tag,
            offset: u64,
            tags: *[layout.ways]Tag,
            values: *[layout.ways]Value,

            fn inspect(set: Set, sac: Self) void {
                const clock_index = @divExact(set.offset, layout.ways);
                std.debug.print(
                    \\{{
                    \\  tag={}
                    \\  offset={}
                    \\  clock_hand={}
                , .{
                    set.tag,
                    set.offset,
                    sac.clocks.get(clock_index),
                });

                std.debug.print("\n  tags={}", .{set.tags[0]});
                for (set.tags[1..]) |tag| std.debug.print(", {}", .{tag});

                std.debug.print("\n  values={}", .{set.values[0]});
                for (set.values[1..]) |value| std.debug.print(", {}", .{value});

                std.debug.print("\n  counts={}", .{sac.counts.get(set.offset)});
                var i: usize = 1;
                while (i < layout.ways) : (i += 1) {
                    std.debug.print(", {}", .{sac.counts.get(set.offset + i)});
                }

                std.debug.print("\n}}\n", .{});
            }
        };

        inline fn associate(self: *const Self, key: Key) Set {
            const entropy = hash(key);

            const tag = @as(Tag, @truncate(entropy >> math.log2_int(u64, self.sets)));
            const index = entropy % self.sets;
            const offset = index * layout.ways;

            return .{
                .tag = tag,
                .offset = offset,
                .tags = self.tags[offset..][0..layout.ways],
                .values = self.values[offset..][0..layout.ways],
            };
        }

        pub fn inspect() void {
            std.debug.print("\nKey={} Value={} ways={} tag_bits={} clock_bits={} " ++
                "clock_hand_bits={} tags_per_line={} clocks_per_line={} " ++
                "clock_hands_per_line={}\n", .{
                @bitSizeOf(Key),
                @sizeOf(Value),
                layout.ways,
                layout.tag_bits,
                layout.clock_bits,
                clock_hand_bits,
                tags_per_line,
                clocks_per_line,
                clock_hands_per_line,
            });
        }
    };
}

pub const UpdateOrInsert = enum { update, insert };

fn set_associative_cache_test(
    comptime Key: type,
    comptime Value: type,
    comptime context: type,
    comptime layout: Layout,
) type {
    const testing = std.testing;
    const expect = testing.expect;
    const expectEqual = testing.expectEqual;

    const log = false;

    const SAC = SetAssociativeCacheType(
        Key,
        Value,
        context.key_from_value,
        context.hash,
        layout,
    );

    return struct {
        fn run() !void {
            if (log) SAC.inspect();

            // TODO Add a nice calculator method to help solve the minimum value_count_max required:
            var sac = try SAC.init(testing.allocator, 16 * 16 * 8, .{ .name = "test" });
            defer sac.deinit(testing.allocator);

            for (sac.tags) |tag| try testing.expectEqual(@as(SAC.Tag, 0), tag);
            for (sac.counts.words) |word| try testing.expectEqual(@as(u64, 0), word);
            for (sac.clocks.words) |word| try testing.expectEqual(@as(u64, 0), word);

            // Fill up the first set entirely.
            {
                var i: usize = 0;
                while (i < layout.ways) : (i += 1) {
                    try expectEqual(i, sac.clocks.get(0));

                    const key = i * sac.sets;
                    _ = sac.upsert(&key);
                    try expect(sac.counts.get(i) == 1);
                    try expectEqual(key, sac.get(key).?.*);
                    try expect(sac.counts.get(i) == 2);
                }
                try expect(sac.clocks.get(0) == 0);
            }

            if (log) sac.associate(0).inspect(sac);

            // Insert another element into the first set, causing key 0 to be evicted.
            {
                const key = layout.ways * sac.sets;
                _ = sac.upsert(&key);
                try expect(sac.counts.get(0) == 1);
                try expectEqual(key, sac.get(key).?.*);
                try expect(sac.counts.get(0) == 2);

                try expectEqual(@as(?*Value, null), sac.get(0));

                {
                    var i: usize = 1;
                    while (i < layout.ways) : (i += 1) {
                        try expect(sac.counts.get(i) == 1);
                    }
                }
            }

            if (log) sac.associate(0).inspect(sac);

            // Ensure removal works.
            {
                const key = 5 * sac.sets;
                assert(sac.get(key).?.* == key);
                try expect(sac.counts.get(5) == 2);

                _ = sac.remove(key);
                try expectEqual(@as(?*Value, null), sac.get(key));
                try expect(sac.counts.get(5) == 0);
            }

            sac.reset();

            for (sac.tags) |tag| try testing.expectEqual(@as(SAC.Tag, 0), tag);
            for (sac.counts.words) |word| try testing.expectEqual(@as(u64, 0), word);
            for (sac.clocks.words) |word| try testing.expectEqual(@as(u64, 0), word);

            // Fill up the first set entirely, maxing out the count for each slot.
            {
                var i: usize = 0;
                while (i < layout.ways) : (i += 1) {
                    try expectEqual(i, sac.clocks.get(0));

                    const key = i * sac.sets;
                    _ = sac.upsert(&key);
                    try expect(sac.counts.get(i) == 1);
                    var j: usize = 2;
                    while (j <= math.maxInt(SAC.Count)) : (j += 1) {
                        try expectEqual(key, sac.get(key).?.*);
                        try expect(sac.counts.get(i) == j);
                    }
                    try expectEqual(key, sac.get(key).?.*);
                    try expect(sac.counts.get(i) == math.maxInt(SAC.Count));
                }
                try expect(sac.clocks.get(0) == 0);
            }

            if (log) sac.associate(0).inspect(sac);

            // Insert another element into the first set, causing key 0 to be evicted.
            {
                const key = layout.ways * sac.sets;
                _ = sac.upsert(&key);
                try expect(sac.counts.get(0) == 1);
                try expectEqual(key, sac.get(key).?.*);
                try expect(sac.counts.get(0) == 2);

                try expectEqual(@as(?*Value, null), sac.get(0));

                {
                    var i: usize = 1;
                    while (i < layout.ways) : (i += 1) {
                        try expect(sac.counts.get(i) == 1);
                    }
                }
            }

            if (log) sac.associate(0).inspect(sac);
        }
    };
}

test "SetAssociativeCache: eviction" {
    const Key = u64;
    const Value = u64;

    const context = struct {
        inline fn key_from_value(value: *const Value) Key {
            return value.*;
        }
        inline fn hash(key: Key) u64 {
            return key;
        }
    };

    try set_associative_cache_test(Key, Value, context, .{}).run();
}

test "SetAssociativeCache: hash collision" {
    const Key = u64;
    const Value = u64;

    const context = struct {
        inline fn key_from_value(value: *const Value) Key {
            return value.*;
        }
        /// This hash function is intentionally broken to simulate hash collision.
        inline fn hash(key: Key) u64 {
            _ = key;
            return 0;
        }
        inline fn equal(a: Key, b: Key) bool {
            return a == b;
        }
    };

    try set_associative_cache_test(Key, Value, context, .{}).run();
}

/// A little simpler than PackedIntArray in the std lib, restricted to little endian 64-bit words,
/// and using words exactly without padding.
fn PackedUnsignedIntegerArray(comptime UInt: type) type {
    const Word = u64;

    assert(builtin.target.cpu.arch.endian() == .Little);
    assert(@typeInfo(UInt).Int.signedness == .unsigned);
    assert(@typeInfo(UInt).Int.bits < @bitSizeOf(u8));
    assert(math.isPowerOfTwo(@typeInfo(UInt).Int.bits));

    const word_bits = @bitSizeOf(Word);
    const uint_bits = @bitSizeOf(UInt);
    const uints_per_word = @divExact(word_bits, uint_bits);

    // An index bounded by the number of unsigned integers that fit exactly into a word.
    const WordIndex = meta.Int(.unsigned, math.log2_int(u64, uints_per_word));
    assert(math.maxInt(WordIndex) == uints_per_word - 1);

    // An index bounded by the number of bits (not unsigned integers) that fit exactly into a word.
    const BitsIndex = math.Log2Int(Word);
    assert(math.maxInt(BitsIndex) == @bitSizeOf(Word) - 1);
    assert(math.maxInt(BitsIndex) == word_bits - 1);
    assert(math.maxInt(BitsIndex) == uint_bits * (math.maxInt(WordIndex) + 1) - 1);

    return struct {
        const Self = @This();

        words: []Word,

        /// Returns the unsigned integer at `index`.
        pub inline fn get(self: Self, index: u64) UInt {
            // This truncate is safe since we want to mask the right-shifted word by exactly a UInt:
            return @as(UInt, @truncate(self.word(index).* >> bits_index(index)));
        }

        /// Sets the unsigned integer at `index` to `value`.
        pub inline fn set(self: Self, index: u64, value: UInt) void {
            const w = self.word(index);
            w.* &= ~mask(index);
            w.* |= @as(Word, value) << bits_index(index);
        }

        inline fn mask(index: u64) Word {
            return @as(Word, math.maxInt(UInt)) << bits_index(index);
        }

        inline fn word(self: Self, index: u64) *Word {
            return &self.words[@divFloor(index, uints_per_word)];
        }

        inline fn bits_index(index: u64) BitsIndex {
            // If uint_bits=2, then it's normal for the maximum return value value to be 62, even
            // where BitsIndex allows up to 63 (inclusive) for a 64-bit word. This is because 62 is
            // the bit index of the highest 2-bit UInt (e.g. bit index + bit length == 64).
            comptime assert(uint_bits * (math.maxInt(WordIndex) + 1) == math.maxInt(BitsIndex) + 1);

            return @as(BitsIndex, uint_bits) * @as(WordIndex, @truncate(index));
        }
    };
}

test "PackedUnsignedIntegerArray: unit" {
    const expectEqual = std.testing.expectEqual;

    var words = [8]u64{ 0, 0b10110010, 0, 0, 0, 0, 0, 0 };

    var p: PackedUnsignedIntegerArray(u2) = .{
        .words = &words,
    };

    try expectEqual(@as(u2, 0b10), p.get(32 + 0));
    try expectEqual(@as(u2, 0b00), p.get(32 + 1));
    try expectEqual(@as(u2, 0b11), p.get(32 + 2));
    try expectEqual(@as(u2, 0b10), p.get(32 + 3));

    p.set(0, 0b01);
    try expectEqual(@as(u64, 0b00000001), words[0]);
    try expectEqual(@as(u2, 0b01), p.get(0));
    p.set(1, 0b10);
    try expectEqual(@as(u64, 0b00001001), words[0]);
    try expectEqual(@as(u2, 0b10), p.get(1));
    p.set(2, 0b11);
    try expectEqual(@as(u64, 0b00111001), words[0]);
    try expectEqual(@as(u2, 0b11), p.get(2));
    p.set(3, 0b11);
    try expectEqual(@as(u64, 0b11111001), words[0]);
    try expectEqual(@as(u2, 0b11), p.get(3));
    p.set(3, 0b01);
    try expectEqual(@as(u64, 0b01111001), words[0]);
    try expectEqual(@as(u2, 0b01), p.get(3));
    p.set(3, 0b00);
    try expectEqual(@as(u64, 0b00111001), words[0]);
    try expectEqual(@as(u2, 0b00), p.get(3));

    p.set(4, 0b11);
    try expectEqual(
        @as(u64, 0b0000000000000000000000000000000000000000000000000000001100111001),
        words[0],
    );
    p.set(31, 0b11);
    try expectEqual(
        @as(u64, 0b1100000000000000000000000000000000000000000000000000001100111001),
        words[0],
    );
}

fn PackedUnsignedIntegerArrayFuzzTest(comptime UInt: type) type {
    const testing = std.testing;

    return struct {
        const Self = @This();

        const Array = PackedUnsignedIntegerArray(UInt);
        random: std.rand.Random,

        array: Array,
        reference: []UInt,

        fn init(random: std.rand.Random, len: usize) !Self {
            const words = try testing.allocator.alloc(u64, @divExact(len * @bitSizeOf(UInt), 64));
            errdefer testing.allocator.free(words);

            const reference = try testing.allocator.alloc(UInt, len);
            errdefer testing.allocator.free(reference);

            @memset(words, 0);
            @memset(reference, 0);

            return Self{
                .random = random,
                .array = Array{ .words = words },
                .reference = reference,
            };
        }

        fn deinit(context: *Self) void {
            testing.allocator.free(context.array.words);
            testing.allocator.free(context.reference);
        }

        fn run(context: *Self) !void {
            var iterations: usize = 0;
            while (iterations < 10_000) : (iterations += 1) {
                const index = context.random.uintLessThanBiased(usize, context.reference.len);
                const value = context.random.int(UInt);

                context.array.set(index, value);
                context.reference[index] = value;

                try context.verify();
            }
        }

        fn verify(context: *Self) !void {
            for (context.reference, 0..) |value, index| {
                try testing.expectEqual(value, context.array.get(index));
            }
        }
    };
}

test "PackedUnsignedIntegerArray: fuzz" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    inline for (.{ u1, u2, u4 }) |UInt| {
        const Context = PackedUnsignedIntegerArrayFuzzTest(UInt);

        var context = try Context.init(random, 1024);
        defer context.deinit();

        try context.run();
    }
}

fn BitIterator(comptime Bits: type) type {
    return struct {
        const Self = @This();
        const BitIndex = math.Log2Int(Bits);

        bits: Bits,

        /// Iterates over the bits, consuming them.
        /// Returns the bit index of each set bit until there are no more set bits, then null.
        inline fn next(it: *Self) ?BitIndex {
            if (it.bits == 0) return null;
            // This @intCast() is safe since we never pass 0 to @ctz().
            const index = @as(BitIndex, @intCast(@ctz(it.bits)));
            // Zero the lowest set bit.
            it.bits &= it.bits - 1;
            return index;
        }
    };
}

test "BitIterator" {
    const expectEqual = @import("std").testing.expectEqual;

    var it = BitIterator(u16){ .bits = 0b1000_0000_0100_0101 };

    for ([_]u4{ 0, 2, 6, 15 }) |e| {
        try expectEqual(@as(?u4, e), it.next());
    }
    try expectEqual(it.next(), null);
}

fn search_tags_test(comptime Key: type, comptime Value: type, comptime layout: Layout) type {
    const testing = std.testing;

    const log = false;

    const context = struct {
        inline fn key_from_value(value: *const Value) Key {
            return value.*;
        }
        inline fn hash(key: Key) u64 {
            return key;
        }
        inline fn equal(a: Key, b: Key) bool {
            return a == b;
        }
    };

    const SAC = SetAssociativeCacheType(
        Key,
        Value,
        context.key_from_value,
        context.hash,
        layout,
    );

    const reference = struct {
        inline fn search_tags(tags: *[layout.ways]SAC.Tag, tag: SAC.Tag) SAC.Ways {
            var bits: SAC.Ways = 0;
            var count: usize = 0;
            for (tags, 0..) |t, i| {
                if (t == tag) {
                    const bit = @as(math.Log2Int(SAC.Ways), @intCast(i));
                    bits |= (@as(SAC.Ways, 1) << bit);
                    count += 1;
                }
            }
            assert(@popCount(bits) == count);
            return bits;
        }
    };

    return struct {
        fn run(random: std.rand.Random) !void {
            if (log) SAC.inspect();

            var iterations: usize = 0;
            while (iterations < 10_000) : (iterations += 1) {
                var tags: [layout.ways]SAC.Tag = undefined;
                random.bytes(mem.asBytes(&tags));

                const tag = random.int(SAC.Tag);

                var indexes: [layout.ways]usize = undefined;
                for (&indexes, 0..) |*x, i| x.* = i;
                random.shuffle(usize, &indexes);

                const matches_count_min = random.uintAtMostBiased(u32, layout.ways);
                for (indexes[0..matches_count_min]) |index| {
                    tags[index] = tag;
                }

                const expected = reference.search_tags(&tags, tag);
                const actual = SAC.search_tags(&tags, tag);
                if (log) std.debug.print("expected: {b:0>16}, actual: {b:0>16}\n", .{
                    expected,
                    actual,
                });
                try testing.expectEqual(expected, actual);
            }
        }
    };
}

test "SetAssociativeCache: search_tags()" {
    const seed = 42;

    const Key = u64;
    const Value = u64;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    inline for ([_]u64{ 2, 4, 16 }) |ways| {
        inline for ([_]u64{ 8, 16 }) |tag_bits| {
            const case = search_tags_test(Key, Value, .{
                .ways = ways,
                .tag_bits = tag_bits,
            });

            try case.run(random);
        }
    }
}
