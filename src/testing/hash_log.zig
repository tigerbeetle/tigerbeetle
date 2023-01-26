//! A tool for narrowing down the point of divergence between two executions that should be identical.
//! Sprinkle calls to `emit(some_hash)` throughout the code.
//! With `-Dhash-log-mode=create`, all emitted hashes are written to ./hash_log.
//! With `-Dhash-log-mode=check`, all emitted hashes are checked against the hashes in ./hash_log.
//! Otherwise, calls to `emit` are noops.

const std = @import("std");
const assert = std.debug.assert;
const panic = std.debug.panic;

const constants = @import("../constants.zig");

var file: ?std.fs.File = null;
var hash_count: usize = 0;

fn ensure_init() void {
    if (file != null) return;
    switch (constants.hash_log_mode) {
        .none => unreachable,
        .create => {
            file = std.fs.cwd().createFile("./hash_log", .{ .truncate = true }) catch unreachable;
        },
        .check => {
            file = std.fs.cwd().openFile("./hash_log", .{ .read = true }) catch unreachable;
        },
    }
}

pub fn emit(hash: u128) void {
    @call(.{ .modifier = .never_inline }, emit_never_inline, .{hash});
}

// Don't inline because we want to be able to break on this function.
fn emit_never_inline(hash: u128) void {
    switch (constants.hash_log_mode) {
        .none => {},
        .create => {
            ensure_init();
            std.fmt.format(file.?.writer(), "{x:0>32}\n", .{hash}) catch unreachable;
            hash_count += 1;
        },
        .check => {
            ensure_init();
            var buffer: [33]u8 = undefined;
            const bytes_read = file.?.readAll(&buffer) catch unreachable;
            if (bytes_read != 33) {
                panic("Unexpected end of hash_log at hash_count={}. Expected EOF, found {x:0>32}.", .{ hash_count, hash });
            }
            const expected_hash = std.fmt.parseInt(u128, buffer[0..32], 16) catch unreachable;
            if (hash != expected_hash) {
                panic(
                    "Hash mismatch at hash_count={}. Expected {x:0>32}, found {x:0>32}.",
                    .{ hash_count, expected_hash, hash },
                );
            }
            hash_count += 1;
        },
    }
}

pub fn emit_autohash(hashable: anytype, comptime strategy: std.hash.Strategy) void {
    if (constants.hash_log_mode == .none) return;
    var hasher = std.hash.Wyhash.init(0);
    std.hash.autoHashStrat(&hasher, hashable, strategy);
    emit(hasher.final());
}
