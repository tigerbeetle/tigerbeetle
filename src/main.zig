const std = @import("std");
const FRNG = @import("./FRNG.zig");

const MiB = 1 << 20;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const buffer = try gpa.alloc(u8, 4 * MiB);
    defer gpa.free(buffer);

    var stdin_reader = std.Io.File.stdin().reader(io, &.{});
    const entropy = try stdin_reader.interface.allocRemaining(gpa, .unlimited);
    defer gpa.free(entropy);

    var frng = FRNG.init(entropy);

    var world: World = .{
        .frng = &frng,
    };

    world.run();
}

const World = struct {
    frng: *FRNG,

    pub fn run(world: *World) void {
        _ = world;
    }
};
