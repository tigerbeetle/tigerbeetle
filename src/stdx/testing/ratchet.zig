const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;

fn format_string_from_type(comptime T: type) []const u8 {
    const info = @typeInfo(T);
    return switch (info) {
        .array => |value| {
            assert(value.child == u8);
            return "{s}";
        },
        .pointer => |value| {
            assert(value.child == u8 or (
                @typeInfo(value.child) == .array and
                    @typeInfo(value.child).array.child == u8   
            ));
            return "{s}";
        },
        .bool => "{}",
        .int, .comptime_int => "{d}",
        .float, .comptime_float => "{d:.2}",
        else => {
            @panic("invalid ratchet type");
        }
    };
}

pub fn measurement_write(
    comptime src: std.builtin.SourceLocation,
    writer: anytype,
    params: anytype,
    measurements: anytype,
) !void {
    assert(std.meta.fields(@TypeOf(measurements)).len > 0);
    maybe(std.meta.fields(@TypeOf(params)).len == 0);
    try writer.print("CI_PERF {s} // ", .{src.fn_name});
    inline for (std.meta.fields(@TypeOf(params))) |field| {
        const format_string = comptime format_string_from_type(field.type);
        try writer.print("{s}=" ++ format_string ++ " ", .{
            field.name,
            @field(params, field.name),
        });
    }
    try writer.writeAll("// ");
    inline for (std.meta.fields(@TypeOf(measurements))) |field| {
        const field_type = @typeInfo(field.type);
        comptime assert(field_type == .int or field_type == .float
                            or field_type == .comptime_float or field_type == .comptime_int);
        const format_string = comptime format_string_from_type(field.type);
        try writer.print("{s}=" ++ format_string ++ " ", .{
            field.name,
            @field(measurements, field.name),
        });
    }
    try writer.writeByte('\n');
}

test "measurement_write accepts empty parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    measurement_write(@src(), output.writer(), .{}, .{
        .time_ms = 10,
        .checksum = 1.61
    }) catch unreachable;

    try std.testing.expectEqualStrings(
        "CI_PERF test.measurement_write accepts empty parameters // // time_ms=10 checksum=1.61 \n",
        output.getWritten(),
    );
}

test "measurement_write prints parameters" {
    var output_buffer: [4096]u8 = undefined;
    var output = std.io.fixedBufferStream(&output_buffer);

    try measurement_write(@src(), output.writer(), .{
        .factor = 50.3,
        .strategy = "random"
        }, .{
        .time_s = 3.9
    });

    try std.testing.expectEqualStrings(
        "CI_PERF test.measurement_write prints parameters // factor=50.30 strategy=random // time_s=3.90 \n",
        output.getWritten(),
    );
}


pub fn measurement_read(
    gpa: std.mem.Allocator,
) void {
    _ = gpa; 
}
