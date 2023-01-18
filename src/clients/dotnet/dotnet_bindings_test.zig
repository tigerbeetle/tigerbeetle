const std = @import("std");
const testing = std.testing;
const dotnet_bindings = @import("dotnet_bindings.zig");

test "bindings dotnet" {
    var buffer = std.ArrayList(u8).init(testing.allocator);
    defer buffer.deinit();

    try dotnet_bindings.generate_bindings(&buffer);

    const current = try std.fs.cwd().readFileAlloc(testing.allocator, "src/clients/dotnet/TigerBeetle/Bindings.cs", std.math.maxInt(usize));
    defer testing.allocator.free(current);

    try testing.expectEqualStrings(current, buffer.items);
}
