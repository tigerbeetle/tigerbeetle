const std = @import("std");
const testing = std.testing;
const go_bindings = @import("go_bindings.zig");

test "bindings go" {
    var buffer = std.ArrayList(u8).init(testing.allocator);
    defer buffer.deinit();

    try go_bindings.generate_bindings(&buffer);

    const current = try std.fs.cwd().readFileAlloc(testing.allocator, "src/clients/go/pkg/types/bindings.go", std.math.maxInt(usize));
    defer testing.allocator.free(current);

    try testing.expectEqualStrings(current, buffer.items);
}
