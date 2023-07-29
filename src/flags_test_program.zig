const std = @import("std");
const assert = std.debug.assert;

const flags = @import("./flags.zig");

const CliArgs = union(enum) {
    empty,
    values: struct {
        int: u32 = 0,
        size: flags.ByteSize = .{ .bytes = 0 },
        boolean: bool = false,
        path: []const u8 = "not-set",
    },
    required: struct {
        foo: u8,
        bar: u8,
    },
};

pub fn main() !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = gpa_allocator.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    assert(args.skip());

    const cli_args = flags.parse_commands(&args, CliArgs);

    const stdout = std.io.getStdOut();
    const out_stream = stdout.writer();
    switch (cli_args) {
        .empty => try out_stream.print("empty\n", .{}),
        .values => |values| {
            try out_stream.print("int: {}\n", .{values.int});
            try out_stream.print("size: {}\n", .{values.size.bytes});
            try out_stream.print("boolean: {}\n", .{values.boolean});
            try out_stream.print("path: {s}\n", .{values.path});
        },
        .required => |required| {
            try out_stream.print("foo: {}\n", .{required.foo});
            try out_stream.print("bar: {}\n", .{required.bar});
        },
    }
}
