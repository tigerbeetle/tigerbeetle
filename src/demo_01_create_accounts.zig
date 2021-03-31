const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.default_port);
    defer std.os.close(fd);

    var accounts = [_]Account{
        Account{
            .id = 1,
            .custom = 0,
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
        },
        Account{
            .id = 2,
            .custom = 0,
            .flags = .{},
            .unit = 710,
            .debit_reserved = 0,
            .debit_accepted = 0,
            .credit_reserved = 0,
            .credit_accepted = 900_000,
            .debit_reserved_limit = 0,
            .debit_accepted_limit = 0,
            .credit_reserved_limit = 200_000,
            .credit_accepted_limit = 2_000_000_000,
        },
    };

    try send(fd, .create_accounts, accounts, CreateAccountResults);
}
