const std = @import("std");

pub fn info(comptime format: []const u8, args: anytype) void {
    std.log.default.info(format, args);
}

pub fn debug(comptime format: []const u8, args: anytype) void {
    std.log.default.debug(format, args);
}
