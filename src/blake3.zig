const std = @import("std");
const assert = std.debug.assert;

const c = @cImport({
    @cInclude("blake3.h");
});

pub fn hash(source: []const u8) u128 {
    var hasher: c.blake3_hasher = undefined;
    c.blake3_hasher_init(&hasher);
    c.blake3_hasher_update(&hasher, @ptrCast(?*const anyopaque, source), source.len);
    // TODO We use 32 here like std.crypto.hash because we have to produce the same hash
    //      as vsr.checksum_comptime for all inputs.
    //      But @sizeOf(u128) would be preferable.
    comptime {
        assert(c.BLAKE3_OUT_LEN == 32);
    }
    var target: [c.BLAKE3_OUT_LEN]u8 = undefined;
    c.blake3_hasher_finalize(&hasher, &target, c.BLAKE3_OUT_LEN);
    return @bitCast(u128, target[0..@sizeOf(u128)].*);
}
