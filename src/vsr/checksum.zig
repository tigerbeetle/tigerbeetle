//! This file implements vsr.checksum. TigerBeetle uses this checksum to:
//!
//! - detect bitrot in data on disk,
//! - validate network messages before casting raw bytes to an `extern struct` type,
//! - hash-chain prepares and client requests to have strong consistency and ordering guarantees.
//!
//! As this checksum is stored on disk, it is set in stone and impossible to change.
//!
//! We need this checksum to be fast (it's in all our hotpaths) and strong (it's our ultimate line
//! of defense against storage failures and some classes of software bugs).
//!
//! Our checksum of choice is based on Aegis:
//!
//! <https://datatracker.ietf.org/doc/draft-irtf-cfrg-aegis-aead/>
//!
//! We use the implementation from the Zig standard library, but here's the overall overview of the
//! thing works:
//!
//! - AES-block is a widely supported in hardware symmetric encryption primitive (`vaesenc`,
//!   `vaesdec` instructions). Hardware acceleration is what provides speed.
//! - Aegis is an modern Authenticated Encryption with Associated Data (AEAD) scheme based on
//!   AES-block.
//! - In AEAD, the user provides, a key, a nonce, a secret message, and associated data, and gets
//!   a ciphertext and an authentication tag back. Associated data is expected to be sent as plain
//!   text (eg, it could be routing information). The tag authenticates _both_ the secret message
//!   and associated data.
//! - AEAD can be specialized to be a MAC by using an empty secret message and zero nonce. NB:
//!   in mac mode, message to sign is treated as AD, not as a secret message.
//! - A MAC can further be specialized to be a checksum by setting the secret key to zero.
//!   And that's what we do here!

const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const testing = std.testing;
const assert = std.debug.assert;

const Aegis128LMac_128 = std.crypto.auth.aegis.Aegis128LMac_128;

var seed_once = std.once(seed_init);
var seed_state: Aegis128LMac_128 = undefined;

fn seed_init() void {
    const key = mem.zeroes([16]u8);
    seed_state = Aegis128LMac_128.init(&key);
}

// Lazily initialize the Aegis State instead of recomputing it on each call to checksum().
// Then, make a copy of the state and use that to hash the source input bytes.
pub fn checksum(source: []const u8) u128 {
    if (@inComptime()) {
        // Aegis128 uses hardware accelerated AES via inline asm which isn't available at comptime.
        // Use a hard-coded value instead and verify via a test.
        if (source.len == 0) return 0x49F174618255402DE6E7E3C40D60CC83;
    }
    var stream = ChecksumStream.init();
    stream.add(source);
    return stream.checksum();
}

test "checksum empty" {
    var stream = ChecksumStream.init();
    stream.add(&.{});
    try std.testing.expectEqual(stream.checksum(), comptime checksum(&.{}));
}

pub const ChecksumStream = struct {
    state: Aegis128LMac_128,

    pub fn init() ChecksumStream {
        seed_once.call();
        return ChecksumStream{ .state = seed_state };
    }

    pub fn add(stream: *ChecksumStream, bytes: []const u8) void {
        stream.state.update(bytes);
    }

    pub fn checksum(stream: *ChecksumStream) u128 {
        var result: u128 = undefined;
        stream.state.final(mem.asBytes(&result));
        stream.* = undefined;
        return result;
    }
};

// Note: these test vectors are not independent --- there are test vectors in AEAD papers, but they
// don't zero all of (nonce, key, secret message). However, the as underlying AEAD implementation
// matches those test vectors, the entries here are correct.
//
// They can be used to smoke-test independent implementations of TigerBeetle checksum.
//
// "checksum stability" test further nails down the exact behavior.
test "checksum test vectors" {
    const TestVector = struct {
        source: []const u8,
        hash: u128,
    };

    for (&[_]TestVector{
        .{
            .source = &[_]u8{0x00} ** 16,
            .hash = @byteSwap(@as(u128, 0xf72ad48dd05dd1656133101cd4be3a26)),
        },
        .{
            .source = &[_]u8{},
            .hash = @byteSwap(@as(u128, 0x83cc600dc4e3e7e62d4055826174f149)),
        },
    }) |test_vector| {
        try testing.expectEqual(test_vector.hash, checksum(test_vector.source));
    }
}

test "checksum simple fuzzing" {
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
        const msg = msg_buf[0..msg_len];
        prng.fill(msg);

        const msg_checksum = checksum(msg);

        // Sanity check that it's a pure function.
        const msg_checksum_again = checksum(msg);
        try testing.expectEqual(msg_checksum, msg_checksum_again);

        // Change the message and make sure the checksum changes.
        msg[prng.random().uintLessThan(usize, msg.len)] +%= 1;
        const changed_checksum = checksum(msg);
        try testing.expect(changed_checksum != msg_checksum);
    }
}

// Change detector test to ensure we don't inadvertency modify our checksum function.
test "checksum stability" {
    var buf: [1024]u8 = undefined;
    var cases: [896]u128 = undefined;
    var case_index: usize = 0;

    // Zeros of various lengths.
    var subcase: usize = 0;
    while (subcase < 128) : (subcase += 1) {
        const message = buf[0..subcase];
        @memset(message, 0);

        cases[case_index] = checksum(message);
        case_index += 1;
    }

    // 64 bytes with exactly one bit set.
    subcase = 0;
    while (subcase < 64 * 8) : (subcase += 1) {
        const message = buf[0..64];
        @memset(message, 0);
        message[@divFloor(subcase, 8)] = @shlExact(@as(u8, 1), @as(u3, @intCast(subcase % 8)));

        cases[case_index] = checksum(message);
        case_index += 1;
    }

    // Pseudo-random data from a specific PRNG of various lengths.
    var prng = std.rand.Xoshiro256.init(92);
    subcase = 0;
    while (subcase < 256) : (subcase += 1) {
        const message = buf[0 .. subcase + 13];
        prng.fill(message);

        cases[case_index] = checksum(message);
        case_index += 1;
    }

    // Sanity check that we are not getting trivial answers.
    for (cases, 0..) |case_a, i| {
        assert(case_a != 0);
        assert(case_a != std.math.maxInt(u128));
        for (cases[0..i]) |case_b| assert(case_a != case_b);
    }

    // Hash me, baby, one more time! If this final hash changes, we broke compatibility in a major
    // way.
    comptime assert(builtin.target.cpu.arch.endian() == .Little);
    const hash = checksum(mem.sliceAsBytes(&cases));
    try testing.expectEqual(hash, 0x82dcaacf4875b279446825b6830d1263);
}
