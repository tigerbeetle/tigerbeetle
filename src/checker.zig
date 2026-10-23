const std = @import("std");
const assert = std.debug.assert;

const FRNG = @import("./FRNG.zig");

const KiB = 1024;
const MiB = 1024 * KiB;
const log = std.log.scoped(.driver);

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = try init.minimal.args.iterateAllocator(gpa);
    defer args.deinit();

    assert(args.skip());
    const sut = args.next().?;

    try FRNG.Driver.main(gpa, io, sut, .{
        // .search = .{},
        .replay = .{
            .size = 28,
            .seed = 16568845351671572594,
        },
    });
}
