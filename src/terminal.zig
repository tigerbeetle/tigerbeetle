const std = @import("std");

pub const Terminal = struct {
    // These are made optional so that printing on failure can be disabled in tests expecting them.
    stdout: ?std.fs.File.Writer,
    stderr: ?std.fs.File.Writer,

    pub fn init() Terminal {
        return Terminal{
            .stdout = std.io.getStdOut().writer(),
            .stderr = std.io.getStdErr().writer(),
        };
    }

    pub fn print(
        self: *const Terminal,
        comptime format: []const u8,
        arguments: anytype,
    ) !void {
        if (self.stdout) |stdout| {
            try stdout.print(format, arguments);
        }
    }

    pub fn print_error(
        self: *const Terminal,
        comptime format: []const u8,
        arguments: anytype,
    ) !void {
        if (self.stderr) |stderr| {
            try stderr.print(format, arguments);
        }
    }
};
