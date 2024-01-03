//! Fuzz EWAH encode/decode cycle.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_ewah);

const ewah = @import("./ewah.zig");
const fuzz = @import("./testing/fuzz.zig");

pub fn main(args: fuzz.FuzzArgs) !void {
    const allocator = fuzz.allocator;

    inline for (.{ u8, u16, u32, u64, usize }) |Word| {
        var prng = std.rand.DefaultPrng.init(args.seed);
        const random = prng.random();

        const decoded_size_max = @divExact(1024 * 1024, @sizeOf(Word));
        const decoded_size = random.uintLessThan(usize, 1 + decoded_size_max);
        const decoded = try allocator.alloc(Word, decoded_size);
        defer allocator.free(decoded);

        const decoded_bits_total = decoded_size * @bitSizeOf(Word);
        const decoded_bits = random.uintLessThan(usize, decoded_bits_total);
        generate_bits(random, std.mem.sliceAsBytes(decoded[0..decoded_size]), decoded_bits);

        var context = try ContextType(Word).init(allocator, decoded.len);
        defer context.deinit(allocator);

        const encoded_size = try context.test_encode_decode(decoded);

        log.info("word={} decoded={} encoded={} compression_ratio={d:.2} set={d:.2}", .{
            Word,
            decoded_size,
            encoded_size,
            @as(f64, @floatFromInt(decoded_size)) / @as(f64, @floatFromInt(encoded_size)),
            @as(f64, @floatFromInt(decoded_bits)) / @as(f64, @floatFromInt(decoded_bits_total)),
        });
    }
}

pub fn fuzz_encode_decode(
    comptime Word: type,
    allocator: std.mem.Allocator,
    decoded: []const Word,
) !void {
    var context = try ContextType(Word).init(allocator, decoded.len);
    defer context.deinit(allocator);

    _ = try context.test_encode_decode(decoded);
}

/// Modify `data` such that it has exactly `bits_set_total` randomly-chosen bits set,
/// with the remaining bits unset.
fn generate_bits(random: std.rand.Random, data: []u8, bits_set_total: usize) void {
    const bits_total = data.len * @bitSizeOf(u8);
    assert(bits_set_total <= bits_total);

    // Start off full or empty to save some work.
    const init_empty = bits_set_total < @divExact(bits_total, 2);
    @memset(data, if (init_empty) @as(u8, 0) else std.math.maxInt(u8));

    var bits_set = if (init_empty) 0 else bits_total;
    while (bits_set != bits_set_total) {
        const bit = random.uintLessThan(usize, bits_total);
        const word = @divFloor(bit, @bitSizeOf(u8));
        const mask = @as(u8, 1) << @as(std.math.Log2Int(u8), @intCast(bit % @bitSizeOf(u8)));

        if (init_empty) {
            if (data[word] & mask != 0) continue;
            data[word] |= mask;
            bits_set += 1;
        } else {
            if (data[word] & mask == 0) continue;
            data[word] &= ~mask;
            bits_set -= 1;
        }
    }
}

fn ContextType(comptime Word: type) type {
    return struct {
        const Self = @This();
        const Codec = ewah.ewah(Word);

        decoded_actual: []Word,
        encoded_actual: []align(@alignOf(Word)) u8,

        fn init(allocator: std.mem.Allocator, size_max: usize) !Self {
            const decoded_actual = try allocator.alloc(Word, size_max);
            errdefer allocator.free(decoded_actual);

            const encoded_actual = try allocator.alignedAlloc(
                u8,
                @alignOf(Word),
                Codec.encode_size_max(size_max),
            );
            errdefer allocator.free(encoded_actual);

            return Self{
                .decoded_actual = decoded_actual,
                .encoded_actual = encoded_actual,
            };
        }

        fn deinit(context: *Self, allocator: std.mem.Allocator) void {
            allocator.free(context.decoded_actual);
            allocator.free(context.encoded_actual);
        }

        fn test_encode_decode(context: Self, decoded_expect: []const Word) !usize {
            assert(decoded_expect.len > 0);

            const encoded_size = Codec.encode(decoded_expect, context.encoded_actual);
            const decoded_actual_size = Codec.decode(
                context.encoded_actual[0..encoded_size],
                context.decoded_actual[0..],
            );

            try std.testing.expectEqual(decoded_expect.len, decoded_actual_size);
            try std.testing.expectEqualSlices(
                Word,
                decoded_expect,
                context.decoded_actual[0..decoded_actual_size],
            );
            return encoded_size;
        }
    };
}
