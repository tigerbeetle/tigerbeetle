const LoggedProcess = @import("./process.zig").LoggedProcess;

name: []const u8,
port: u16,
process: *LoggedProcess,
