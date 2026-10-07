const std = @import("std");

const conformance = @import("conformance");
const Arena = conformance.Arena;

// Dumps a list of suites and tests in Markdown format
pub fn main(init: std.process.Init) !void {
    var memory: [Arena.default_memory_size]u8 = undefined;
    var arena = Arena.init(&memory);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buffer);
    const writer = &stdout.interface;
    try writer.writeAll("# Conformance Test Suite\n");
    for (conformance.sources) |source| {
        const suite_memory = arena.mark();
        defer arena.release(suite_memory);

        const suite = try conformance.parse(arena.allocator(), source);
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
