const std = @import("std");
const stdx = @import("stdx");

const conformance = @import("conformance");

// Dumps a list of suites and tests in Markdown format
pub fn main(init: std.process.Init) !void {
    var memory: [2 * stdx.MiB]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&memory);
    const tests = try conformance.parse(fba.allocator());

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buffer);
    const writer = &stdout.interface;
    try writer.writeAll("# Conformance Test Suite\n");
    for (tests.suites) |suite| {
        try writer.writeAll("\n");
        try writer.print("- {s}\n", .{suite.name});
        for (suite.cases) |case| {
            try writer.print("    - {s}", .{case.description});
            if (case.requirement) |requirement| {
                try writer.print(" ({s})", .{@tagName(requirement)});
            }
            try writer.writeAll("\n");
        }
    }
    try writer.flush();
}
