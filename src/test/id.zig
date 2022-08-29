const std = @import("std");

/// Permute indices (or other encoded data) into ids to:
///
/// * test different patterns of ids (e.g. random, ascending, descending), and
/// * allow the original index to recovered from the id, enabling less stateful testing.
///
pub const IdPermutation = union(enum) {
    /// Ascending indices become ascending ids.
    identity: void,
    /// Ascending indices become descending ids.
    reflect: void,
    /// Ascending indices alternate between ascending/descending (e.g. 1,100,3,98,…).
    zigzag: void,
    /// Ascending indices become UUIDs.
    random: [32]u8,

    pub fn encode(self: *const IdPermutation, data: u128) u128 {
        return switch (self.*) {
            .identity => data,
            .reflect => std.math.maxInt(u128) - data,
            .zigzag => {
                if (data % 2 == 0) {
                    return data;
                } else {
                    // -1 to stay odd.
                    return std.math.maxInt(u128) - data - 1;
                }
            },
            .random => |seed| {
                var id: u128 = undefined;
                std.crypto.stream.chacha.ChaCha8IETF.xor(
                    std.mem.asBytes(&id),
                    std.mem.asBytes(&data),
                    0,
                    seed,
                    [_]u8{0} ** 12,
                );
                return id;
            },
        };
    }

    pub fn decode(self: *const IdPermutation, id: u128) u128 {
        return self.encode(id);
    }
};

test "IdPermutation" {
    var prng = std.rand.DefaultPrng.init(123);
    const random = prng.random();

    for ([_]IdPermutation{
        .{ .identity = {} },
        .{ .reflect = {} },
        .{ .zigzag = {} },
        .{ .random = [_]u8{3} ** 32 },
    }) |permutation| {
        var i: u128 = 0;
        while (i < 20) : (i += 1) {
            const r = random.int(u128);
            try test_id_permutation(permutation, r);
            try test_id_permutation(permutation, i);
            try test_id_permutation(permutation, std.math.maxInt(u128) - i);
        }
    }
}

fn test_id_permutation(permutation: IdPermutation, value: u128) !void {
    try std.testing.expectEqual(value, permutation.decode(permutation.encode(value)));
    try std.testing.expectEqual(value, permutation.encode(permutation.decode(value)));
}
