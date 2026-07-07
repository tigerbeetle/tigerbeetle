const std = @import("std");
const assert = std.debug.assert;

pub fn TabularOutputType(comptime row_types: []const type) type {
    return struct {
        const TabularOutput = @This();
        pub const Row = ConcatStructsType(row_types);

        writer: std.io.AnyWriter,

        pub fn init(writer: std.io.AnyWriter, options: struct {
            header: bool = true,
        }) !TabularOutput {
            var output: TabularOutput = .{ .writer = writer };
            if (options.header) {
                try output.write_header();
            }
            return output;
        }

        fn write_header(tabular_output: *TabularOutput) !void {
            inline for (comptime std.meta.fields(Row), 0..) |field, index| {
                if (index > 0) _ = try tabular_output.writer.write(", ");
                _ = try tabular_output.writer.print("{s: >4}", .{field.name});
            }
            _ = try tabular_output.writer.write("\n");
        }

        pub inline fn write_row(tabular_output: *TabularOutput, row: *const Row) !void {
            inline for (comptime std.meta.fields(Row), 0..) |field, index| {
                if (index > 0) _ = try tabular_output.writer.write(", ");
                const cell_fmt = switch (@typeInfo(field.type)) {
                    .int => "{[field_value]d: >[field_width]}",
                    .float => "{[field_value]d: >[field_width].2}",
                    .pointer => "{[field_value]s: >[field_width]}",
                    .bool => "{: >[field_width]}",
                    .@"enum" => "{[field_value]any: >[field_width]}",
                    else => @panic("Type not supported for serialization"),
                };
                try tabular_output.writer.print(cell_fmt, .{
                    .field_value = @field(row, field.name),
                    .field_width = @max(field.name.len, 4),
                });
            }
            _ = try tabular_output.writer.write("\n");
        }

        pub inline fn row_from_bag(bag: anytype) ConcatStructsType(field_types(@TypeOf(bag))) {
            var result: ConcatStructsType(field_types(@TypeOf(bag))) = undefined;

            var fields_set: u64 = 0;
            inline for (comptime std.meta.fields(@TypeOf(bag))) |field_outer| {
                assert(@typeInfo(field_outer.type) == .@"struct");
                const value_outer = @field(bag, field_outer.name);
                inline for (comptime std.meta.fields(@TypeOf(value_outer))) |field_inner| {
                    fields_set += 1;
                    @field(result, field_inner.name) = @field(value_outer, field_inner.name);
                }
            }

            assert(fields_set == std.meta.fields(@TypeOf(result)).len);
            return result;
        }
    };
}

fn field_types(comptime tuple: type) []const type {
    comptime var types: []const type = &.{};
    inline for (comptime std.meta.fields(tuple)) |field_outer| {
        types = types ++ [_]type{field_outer.type};
    }
    return types;
}

fn ConcatStructsType(types: []const type) type {
    comptime var fields: []const std.builtin.Type.StructField = &.{};
    inline for (types) |t| {
        const struct_type = @typeInfo(t).@"struct";
        assert(struct_type.layout == .auto);
        assert(!struct_type.is_tuple);

        fields = fields ++ struct_type.fields;
    }

    return @Type(.{
        .@"struct" = .{ .layout = .auto, .fields = fields, .decls = &.{}, .is_tuple = false },
    });
}
