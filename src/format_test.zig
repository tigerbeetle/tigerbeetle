const std = @import("std");
usingnamespace @import("tigerbeetle.zig");

pub fn main() !void {
    var myValue = MyType{
        .x = 1,
        .y = 2,
    };
    var account = Account{
        .id = 1,
        .custom = 10,
        .flags = .{},
        .unit = 710, // Let's use the ISO-4217 Code Number for ZAR
        .debit_reserved = 0,
        .debit_accepted = 0,
        .credit_reserved = 0,
        .credit_accepted = 0,
        .debit_reserved_limit = 100_000,
        .debit_accepted_limit = 1_000_000,
        .credit_reserved_limit = 0,
        .credit_accepted_limit = 0,
    };

    var transfer = Transfer{
        .id = 1000,
        .debit_account_id = 1,
        .credit_account_id = 2,
        .custom_1 = 0,
        .custom_2 = 0,
        .custom_3 = 0,
        .flags = .{
            .accept = true,
            .auto_commit = true,
        },
        .amount = 1000,
        .timeout = 0,
    };
    std.debug.print("{}\n", .{transfer});
}

pub const MyType = packed struct {
    x: u64 = 0,
    y: u64 = 0,

    pub fn format(value: MyType, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        if (fmt.len > 0) {
            if (fmt.len > 1) {
                return std.fmt.format(writer, "{}", .{fmt.len});
            }
            switch (fmt[0]) {
                // json format
                'j' => return std.fmt.format(writer, "[{},{}]", .{ value.x, value.y }),
                else => unreachable,
            }
        }
        return std.fmt.format(writer, "{},{}", .{ value.x, value.y });
    }
};
