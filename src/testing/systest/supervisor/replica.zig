const LoggedProcess = @import("./logged_process.zig");

name: []const u8,
port: u16,
process: *LoggedProcess,
