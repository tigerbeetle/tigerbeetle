const std = @import("std");

pub const py = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    @cDefine("Py_LIMITED_API", "0x03070000"); // Require Python 3.7 and up.
    @cInclude("Python.h");
});

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("EXPORTS\n", .{});

    for (std.meta.declarations(py)) |declaration| {
        if (std.mem.startsWith(u8, declaration.name, "Py") or
            std.mem.startsWith(u8, declaration.name, "_Py"))
        {
            try stdout.print("    {s}\n", .{declaration.name});
        }
    }
}
