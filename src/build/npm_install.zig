//! npm install routinely fails on Windows on CI.
//! This script just re-runs it multiple times, hoping for the best!
const std = @import("std");
const stdb = @import("./stdb.zig");

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main(init: std.process.Init) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();
    const io = init.io;

    // Run `npm install` first, to avoid any disk writes if everything's already installed.
    if (stdb.exec_ok(arena, io, &.{ "npm", "install" })) return;

    // Failing that, switch to `clean-install`, which nukes node_modules first.
    for (0..10) |_| {
        if (stdb.exec_ok(arena, io, &.{ "npm", "clean-install" })) return;
    }

    _ = try stdb.exec(arena, io, &.{ "npm", "clean-install" });
}
