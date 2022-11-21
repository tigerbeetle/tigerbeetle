const std = @import("std");
const assert = std.debug.assert;

const Transfer = @import("../src/tigerbeetle.zig").Transfer;
const IndexCompositeKeyType = @import("../src/lsm/groove.zig").IndexCompositeKeyType;
const StateMachine = @import("../src/state_machine.zig").StateMachineType(
    @import("../src/storage.zig").Storage,
    .{
        .message_body_size_max = config.message_body_size_max,
    },
);
const config = @import("../src/config.zig");

pub fn main() !void {
    std.debug.print("Workload: 1_000_000 transfers/s.\n\n", .{});

    const transfers_per_second = 1_000_000;
    const transfers_per_batch = StateMachine.constants.batch_max.create_transfers;

    const cases = .{
        // The current code.
        .current,
        // Change level 0 to only contain 1 level.
        .small_level_0,
        // Change the immutable table to config.lsm_table_size_max.
        // (Complicates compaction pacing.)
        .large_immutable_table,
        // Change the disk tables to match the size of the immutable table.
        .small_disk_tables,
        .small_level_0_and_small_disk_tables,
        // Change the growth factor to 4.
        .slower_growth,
        .slower_growth_and_small_level_0,
    };
    inline for (cases) |case| {
        std.debug.print("{}:\n", .{case});

        const existing_transfer_counts = [_]usize{
            1_000_000,
            10_000_000,
            100_000_000,
            1_000_000_000,
        };
        for (existing_transfer_counts) |existing_transfer_count| {
            var read_bytes_per_second: usize = 0;
            var write_bytes_per_second: usize = 0;

            // Every transfer must be written to the WAL.
            write_bytes_per_second += transfers_per_second * @sizeOf(Transfer);

            // TODO We have to read credit/debit account/balance to validate each transfer.
            //      It's hard to estimate the average case given caching and bloom filters,
            //      and the (very unlikely) worst case is so enormous that it dwarfs all other calculations.

            const field_names = [_][]const u8{
                "id",
                "debit_account_id",
                "credit_account_id",
                "user_data",
                "pending_id",
                "timeout",
                "ledger",
                "code",
                "amount",
                "timestamp",
                "balance",
            };
            inline for (field_names) |field_name| {
                const Value = comptime if (std.meta.eql(field_name, "timestamp"))
                    // Object tree.
                    Transfer
                else if (std.meta.eql(field_name, "balance"))
                    // Proposed balance tree.
                    struct {
                        account_id: u128,
                        transfer_id: u128,
                        debits_pending: u64,
                        debits_posted: u64,
                        credits_pending: u64,
                        credits_posted: u64,
                    }
                else
                    // Secondary index.
                    IndexCompositeKeyType(@TypeOf(@field(@as(Transfer, undefined), field_name)));

                const lsm_growth_factor = switch (case) {
                    .current, .small_level_0, .large_immutable_table, .small_disk_tables, .small_level_0_and_small_disk_tables => config.lsm_growth_factor,
                    .slower_growth, .slower_growth_and_small_level_0 => @divTrunc(config.lsm_growth_factor, 2),
                    else => unreachable,
                };

                const values_per_immutable_table = switch (case) {
                    .current, .small_level_0, .small_disk_tables, .small_level_0_and_small_disk_tables, .slower_growth, .slower_growth_and_small_level_0 => transfers_per_batch * config.lsm_batch_multiple,
                    .large_immutable_table => @divTrunc(config.lsm_table_size_max, @sizeOf(Value)),
                    else => unreachable,
                };
                const values_per_disk_table = switch (case) {
                    .current, .small_level_0, .large_immutable_table, .slower_growth, .slower_growth_and_small_level_0 => @divTrunc(config.lsm_table_size_max, @sizeOf(Value)),
                    .small_disk_tables, .small_level_0_and_small_disk_tables => transfers_per_batch * config.lsm_batch_multiple,
                    else => unreachable,
                };
                const table_count = try std.math.divCeil(usize, existing_transfer_count, values_per_disk_table);

                const tables_on_level_0 = switch (case) {
                    .current, .large_immutable_table, .small_disk_tables, .slower_growth => lsm_growth_factor,
                    .small_level_0, .small_level_0_and_small_disk_tables, .slower_growth_and_small_level_0 => 1,
                    else => unreachable,
                };

                var level_count: usize = 0;
                {
                    var table_count_remaining: usize = table_count;
                    var tables_per_level: usize = tables_on_level_0;
                    while (table_count_remaining > 0) {
                        level_count += 1;
                        table_count_remaining = table_count_remaining -| tables_per_level;
                        tables_per_level *= lsm_growth_factor;
                    }
                }

                if (std.meta.eql(field_name, "timestamp")) {
                    // The object tree does not need to be compacted so just count write bandwidth per value.
                    write_bytes_per_second += transfers_per_second * @sizeOf(Value);
                } else {
                    // We compact the immutable table into level 0 whenever the immutable tables is full;
                    const level_0_compactions_per_second = try std.math.divCeil(usize, transfers_per_second, values_per_immutable_table);
                    // Lower levels are compacted whenever we need room for ingress from above.
                    const level_n_compactions_per_second = try std.math.divCeil(usize, transfers_per_second, values_per_disk_table);

                    read_bytes_per_second +=
                        level_0_compactions_per_second *
                        // Read all tables on level 0
                        tables_on_level_0 *
                        values_per_disk_table * @sizeOf(Value);
                    read_bytes_per_second +=
                        (level_count -| 1) *
                        level_n_compactions_per_second *
                        // Read 1 table from level a and lsm_growth_factor tables from level b.
                        (1 + lsm_growth_factor) *
                        values_per_disk_table * @sizeOf(Value);

                    write_bytes_per_second +=
                        level_0_compactions_per_second * (
                    // Write the immutable table
                        (values_per_immutable_table * @sizeOf(Value)) +
                        // Rewrite all tables on level 0
                        (tables_on_level_0 * values_per_disk_table * @sizeOf(Value)));
                    write_bytes_per_second +=
                        (level_count -| 1) *
                        level_n_compactions_per_second *
                        // Rewrite 1 table from level a and lsm_growth_factor tables from level b.
                        (1 + lsm_growth_factor) * values_per_disk_table *
                        @sizeOf(Value);
                }

                // TODO Calculate bandwidth for checkpoint - smaller tables mean cheaper compactions but a larger manifest.
            }

            std.debug.print("Existing transfers = {d:>10.0}. Read ~= {d:>5.2} gb/s. Write ~= {d:>5.2} gb/s. Throughput at 4gb/s ~= {d:>7.0} transfers/s.\n", .{
                existing_transfer_count,
                @intToFloat(f64, read_bytes_per_second) / 1024 / 1024 / 1024,
                @intToFloat(f64, write_bytes_per_second) / 1024 / 1024 / 1024,
                transfers_per_second * ((4 * 1024 * 1024 * 1024) / @intToFloat(f64, read_bytes_per_second + write_bytes_per_second)),
            });
        }

        std.debug.print("\n", .{});
    }
}

const ReadWrite = struct {
    read: usize,
    write: usize,
};
