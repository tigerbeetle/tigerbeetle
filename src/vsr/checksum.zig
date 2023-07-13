const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const testing = std.testing;

// TODO(king): Replace with std.crypto.aead.aegis.Aegis128LMac_128 once Zig is updated.
const Aegis128 = struct {
    const AesBlock = std.crypto.core.aes.Block;
    const State = [8]AesBlock;

    fn init(key: [16]u8, nonce: [16]u8) State {
        const c1 = AesBlock.fromBytes(&.{ 0xdb, 0x3d, 0x18, 0x55, 0x6d, 0xc2, 0x2f, 0xf1, 0x20, 0x11, 0x31, 0x42, 0x73, 0xb5, 0x28, 0xdd });
        const c2 = AesBlock.fromBytes(&.{ 0x0, 0x1, 0x01, 0x02, 0x03, 0x05, 0x08, 0x0d, 0x15, 0x22, 0x37, 0x59, 0x90, 0xe9, 0x79, 0x62 });
        const key_block = AesBlock.fromBytes(&key);
        const nonce_block = AesBlock.fromBytes(&nonce);
        var blocks = [8]AesBlock{
            key_block.xorBlocks(nonce_block),
            c1,
            c2,
            c1,
            key_block.xorBlocks(nonce_block),
            key_block.xorBlocks(c2),
            key_block.xorBlocks(c1),
            key_block.xorBlocks(c2),
        };
        var i: usize = 0;
        while (i < 10) : (i += 1) {
            update(&blocks, nonce_block, key_block);
        }
        return blocks;
    }

    inline fn update(blocks: *State, d1: AesBlock, d2: AesBlock) void {
        const tmp = blocks[7];
        comptime var i: usize = 7;
        inline while (i > 0) : (i -= 1) {
            blocks[i] = blocks[i - 1].encrypt(blocks[i]);
        }
        blocks[0] = tmp.encrypt(blocks[0]);
        blocks[0] = blocks[0].xorBlocks(d1);
        blocks[4] = blocks[4].xorBlocks(d2);
    }

    fn absorb(blocks: *State, src: *const [32]u8) void {
        const msg0 = AesBlock.fromBytes(src[0..16]);
        const msg1 = AesBlock.fromBytes(src[16..32]);
        update(blocks, msg0, msg1);
    }

    fn mac(blocks: *State, adlen: usize, mlen: usize) [16]u8 {
        var sizes: [16]u8 = undefined;
        mem.writeIntLittle(u64, sizes[0..8], adlen * 8);
        mem.writeIntLittle(u64, sizes[8..16], mlen * 8);
        const tmp = AesBlock.fromBytes(&sizes).xorBlocks(blocks[2]);
        var i: usize = 0;
        while (i < 7) : (i += 1) {
            update(blocks, tmp, tmp);
        }
        return blocks[0].xorBlocks(blocks[1]).xorBlocks(blocks[2]).xorBlocks(blocks[3]).xorBlocks(blocks[4])
            .xorBlocks(blocks[5]).xorBlocks(blocks[6]).toBytes();
    }

    inline fn hash(blocks: *State, source: []const u8) u128 {
        var i: usize = 0;
        while (i + 32 <= source.len) : (i += 32) {
            absorb(blocks, source[i..][0..32]);
        }
        if (source.len % 32 != 0) {
            var src: [32]u8 align(16) = mem.zeroes([32]u8);
            mem.copy(u8, src[0 .. source.len % 32], source[i .. i + source.len % 32]);
            absorb(blocks, &src);
        }
        return @bitCast(u128, mac(blocks, 0, source.len));
    }
};

var seed_once = std.once(seed_init);
var seed_state: Aegis128.State = undefined;

fn seed_init() void {
    const key = mem.zeroes([16]u8);
    const nonce = mem.zeroes([16]u8);
    seed_state = Aegis128.init(key, nonce);
}

// Lazily initialize the Aegis State instead of recomputing it on each call to checksum().
// Then, make a copy of the state and use that to hash the source input bytes.
pub fn checksum(source: []const u8) u128 {
    seed_once.call();
    var state_copy = seed_state;
    return Aegis128.hash(&state_copy, source);
}

fn std_checksum(cipher: []u8, msg: []const u8) u128 {
    std.debug.assert(cipher.len == msg.len);

    const ad = [_]u8{};
    const key: [16]u8 = [_]u8{0x00} ** 16;
    const nonce: [16]u8 = [_]u8{0x00} ** 16;

    var tag: [16]u8 = undefined;
    std.crypto.aead.aegis.Aegis128L.encrypt(cipher, &tag, msg, &ad, nonce, key);
    return @bitCast(u128, tag);
}

test "Aegis test vectors" {
    const TestVector = struct {
        source: []const u8,
        hash: u128,
    };

    var cipher_buf: [16]u8 = undefined;
    for (&[_]TestVector{
        .{
            .source = &[_]u8{0x00} ** 16,
            .hash = @byteSwap(@as(u128, 0xf4d997cc9b94227ada4fe4165422b1c8)),
        },
        .{
            .source = &[_]u8{},
            .hash = @byteSwap(@as(u128, 0x83cc600dc4e3e7e62d4055826174f149)),
        },
    }) |test_vector| {
        const cipher = cipher_buf[0..test_vector.source.len];
        try testing.expectEqual(test_vector.hash, checksum(test_vector.source));
        try testing.expectEqual(test_vector.hash, std_checksum(cipher, test_vector.source));
    }
}

test "Aegis simple fuzzing" {
    var prng = std.rand.DefaultPrng.init(42);

    const msg_min = 1;
    const msg_max = 1 * 1024 * 1024;

    var msg_buf = try testing.allocator.alloc(u8, msg_max);
    defer testing.allocator.free(msg_buf);

    var cipher_buf = try testing.allocator.alloc(u8, msg_max);
    defer testing.allocator.free(cipher_buf);

    var i: usize = 0;
    while (i < 1_000) : (i += 1) {
        const msg_len = prng.random().intRangeAtMostBiased(usize, msg_min, msg_max);
        const cipher = cipher_buf[0..msg_len];
        const msg = msg_buf[0..msg_len];
        prng.fill(msg);

        // Make sure it matches with stdlib.
        const msg_checksum = checksum(msg);
        try testing.expectEqual(msg_checksum, std_checksum(cipher, msg));

        // Sanity check that it's a pure function.
        const msg_checksum_again = checksum(msg);
        try testing.expectEqual(msg_checksum, msg_checksum_again);

        // Change the message and make sure the checksum changes.
        msg[prng.random().uintLessThan(usize, msg.len)] +%= 1;
        const changed_checksum = checksum(msg);
        try testing.expectEqual(changed_checksum, std_checksum(cipher, msg));
        try testing.expect(changed_checksum != msg_checksum);
    }
}
