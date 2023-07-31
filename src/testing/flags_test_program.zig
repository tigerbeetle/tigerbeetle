const std = @import("std");
const assert = std.debug.assert;

const flags = @import("../flags.zig");

const CliArgs = union(enum) {
    empty,
    prefix: struct {
        foo: u8 = 0,
        foo_bar: u8 = 0,
        opt: bool = false,
        option: bool = false,
    },
    pos: struct { flag: bool = false, positional: struct {
        p1: []const u8,
        p2: []const u8,
    } },
    required: struct {
        foo: u8,
        bar: u8,
    },
    values: struct {
        int: u32 = 0,
        size: flags.ByteSize = .{ .bytes = 0 },
        boolean: bool = false,
        path: []const u8 = "not-set",
    },

    pub const help =
        \\ flags-test-program [flags]
        \\
    ;
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
        .prefix => |values| {
            try out_stream.print("foo: {}\n", .{values.foo});
            try out_stream.print("foo-bar: {}\n", .{values.foo_bar});
            try out_stream.print("opt: {}\n", .{values.opt});
            try out_stream.print("option: {}\n", .{values.option});
        },
        .pos => |values| {
            try out_stream.print("p1: {s}\n", .{values.positional.p1});
            try out_stream.print("p2: {s}\n", .{values.positional.p2});
            try out_stream.print("flag: {}\n", .{values.flag});
        },
        .required => |required| {
            try out_stream.print("foo: {}\n", .{required.foo});
            try out_stream.print("bar: {}\n", .{required.bar});
        },
        .values => |values| {
            try out_stream.print("int: {}\n", .{values.int});
            try out_stream.print("size: {}\n", .{values.size.bytes});
            try out_stream.print("boolean: {}\n", .{values.boolean});
            try out_stream.print("path: {s}\n", .{values.path});
        },
    }
}
