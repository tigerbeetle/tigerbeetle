//! In the generated tests files, suites appear in the order defined in `suites`.

pub const suites = .{
    @import("suites/ids_generate.zig"),
    @import("suites/accounts_create.zig"),
    @import("suites/accounts_lookup.zig"),
    @import("suites/transfers_create.zig"),
    @import("suites/transfers_lookup.zig"),
    @import("suites/account_transfers_get.zig"),
    @import("suites/account_balances_get.zig"),
    @import("suites/accounts_query.zig"),
    @import("suites/transfers_query.zig"),
    @import("suites/uint128_max.zig"),
    @import("suites/transfers_create_concurrent.zig"),
    @import("suites/client_close.zig"),
};
