const std = @import("std");
const mem = std.mem;

/// Minimal LZ4 block compression/decompression (no framing).
/// The implementation is intentionally simple and optimized for correctness over speed.
pub const Error = error{
    OutputTooSmall,
    CorruptInput,
};

/// Compress `src` into `dst`, returning the compressed size.
/// If the compressed output would not fit in `dst`, returns `Error.OutputTooSmall`.
pub fn compress(src: []const u8, dst: []u8) Error!usize {
    const min_match = 4;
    if (src.len == 0) return 0;
    if (src.len < min_match) {
        if (dst.len < src.len + 1) return error.OutputTooSmall;
        dst[0] = @intCast(src.len << 4);
        @memcpy(dst[1 .. 1 + src.len], src);
        return src.len + 1;
    }

    const hash_size = 1 << 16;
    var hash_table = [_]u32{0} ** hash_size;

    var ip: usize = 0;
    var anchor: usize = 0;
    var op: usize = 0;

    const src_end = src.len;
    const match_limit = if (src_end < min_match) 0 else src_end - min_match;
    const hash_shift: u5 = 16;

    const read_u32 = struct {
        inline fn get(bytes: []const u8, i: usize) u32 {
            return mem.readInt(u32, bytes[i..][0..min_match], .little);
        }
    }.get;

    while (ip <= match_limit) {
        const sequence = read_u32(src, ip);
        const hash = (@as(u32, sequence *% 2654435761)) >> hash_shift;
        const ref_plus = hash_table[hash];
        hash_table[hash] = @intCast(ip + 1);

        if (ref_plus == 0) {
            ip += 1;
            continue;
        }

        const ref_pos = ref_plus - 1;
        if (ip <= ref_pos or ip - ref_pos > std.math.maxInt(u16)) {
            ip += 1;
            continue;
        }
        if (read_u32(src, ref_pos) != sequence) {
            ip += 1;
            continue;
        }

        // Found a match.
        const token_index = op;
        if (token_index >= dst.len) return error.OutputTooSmall;
        op += 1; // Token placeholder.

        // Literals.
        const literal_len: usize = ip - anchor;
        var token: u8 = @as(u8, @intCast(@min(literal_len, 15))) << 4;

        if (literal_len >= 15) {
            var len = literal_len - 15;
            while (len >= 255) : (len -= 255) {
                if (op >= dst.len) return error.OutputTooSmall;
                dst[op] = 255;
                op += 1;
            }
            if (op >= dst.len) return error.OutputTooSmall;
            dst[op] = @intCast(len);
            op += 1;
        }

        if (op + literal_len + 2 > dst.len) return error.OutputTooSmall;
        @memcpy(dst[op .. op + literal_len], src[anchor .. anchor + literal_len]);
        op += literal_len;

        const offset = ip - ref_pos;
        mem.writeInt(u16, dst[op..][0..2], @intCast(offset), .little);
        op += 2;

        // Match length (we already have min_match bytes).
        var match_len: usize = min_match;
        while (ip + match_len < src_end and
            ref_pos + match_len < src_end and
            src[ip + match_len] == src[ref_pos + match_len])
        {
            match_len += 1;
        }

        const match_len_token = match_len - min_match;
        token |= @intCast(@min(match_len_token, 15));
        dst[token_index] = token;

        if (match_len_token >= 15) {
            var len_ext = match_len_token - 15;
            while (len_ext >= 255) : (len_ext -= 255) {
                if (op >= dst.len) return error.OutputTooSmall;
                dst[op] = 255;
                op += 1;
            }
            if (op >= dst.len) return error.OutputTooSmall;
            dst[op] = @intCast(len_ext);
            op += 1;
        }

        ip += match_len;
        anchor = ip;
    }

    // Last literals.
    const remaining = src_end - anchor;
    if (op + 1 + remaining > dst.len) return error.OutputTooSmall;
    const token_index = op;
    op += 1;

    const token: u8 = @as(u8, @intCast(@min(remaining, 15))) << 4;
    if (remaining >= 15) {
        var len_ext = remaining - 15;
        while (len_ext >= 255) : (len_ext -= 255) {
            if (op >= dst.len) return error.OutputTooSmall;
            dst[op] = 255;
            op += 1;
        }
        if (op >= dst.len) return error.OutputTooSmall;
        dst[op] = @intCast(len_ext);
        op += 1;
    }

    if (op + remaining > dst.len) return error.OutputTooSmall;
    @memcpy(dst[op .. op + remaining], src[anchor..]);
    op += remaining;

    dst[token_index] = token;
    return op;
}

/// Decompress `src` into `dst`, returning the decompressed size.
pub fn decompress(src: []const u8, dst: []u8) Error!usize {
    var ip: usize = 0;
    var op: usize = 0;

    while (ip < src.len) {
        const token = src[ip];
        ip += 1;

        var literal_len: usize = token >> 4;
        if (literal_len == 15) {
            while (true) {
                if (ip >= src.len) return error.CorruptInput;
                const len_byte = src[ip];
                ip += 1;
                literal_len += len_byte;
                if (len_byte != 255) break;
            }
        }

        if (ip + literal_len > src.len or op + literal_len > dst.len) {
            return error.CorruptInput;
        }

        @memcpy(dst[op .. op + literal_len], src[ip .. ip + literal_len]);
        ip += literal_len;
        op += literal_len;

        if (ip >= src.len) {
            // Input finished on a literal run.
            break;
        }

        if (ip + 2 > src.len) return error.CorruptInput;
        const offset = mem.readInt(u16, src[ip..][0..2], .little);
        ip += 2;
        if (offset == 0 or offset > op) return error.CorruptInput;

        var match_len: usize = token & 0x0F;
        if (match_len == 15) {
            while (true) {
                if (ip >= src.len) return error.CorruptInput;
                const len_byte = src[ip];
                ip += 1;
                match_len += len_byte;
                if (len_byte != 255) break;
            }
        }
        match_len += 4;

        if (op + match_len > dst.len) return error.CorruptInput;
        const ref_start = op - offset;
        // copyForwards handles overlap.
        mem.copyForwards(u8, dst[op .. op + match_len], dst[ref_start .. ref_start + match_len]);
        op += match_len;
    }

    return op;
}
