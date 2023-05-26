const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const constants = @import("../constants.zig");

const TableType = @import("table.zig").TableType;
const TableMutableType = @import("table_mutable.zig").TableMutableType;

const TransferContext = struct {
    const Transfer = @import("../tigerbeetle.zig").Transfer;

    inline fn compare_keys(timestamp_a: u64, timestamp_b: u64) std.math.Order {
        return std.math.order(timestamp_a, timestamp_b);
    }

    inline fn key_from_value(value: *const Transfer) u64 {
        return value.timestamp & ~@as(u64, tombstone_bit);
    }

    const sentinel_key = std.math.maxInt(u64);
    const tombstone_bit = 1 << (64 - 1);

    inline fn tombstone(value: *const Transfer) bool {
        return (value.timestamp & tombstone_bit) != 0;
    }

    inline fn tombstone_from_key(timestamp: u64) Transfer {
        var value = std.mem.zeroes(Transfer); // Full zero-initialized Value.
        value.timestamp = timestamp | tombstone_bit;
        return value;
    }

    const config = constants.state_machine_config;
    const Storage = @import("../testing/storage.zig").Storage;
    const StateMachine = @import("../vsr.zig").state_machine.StateMachineType(Storage, config);

    pub const Table = TableType(
        u64, // key = timestamp
        Transfer,
        compare_keys,
        key_from_value,
        sentinel_key,
        tombstone,
        tombstone_from_key,
        config.lsm_batch_multiple * StateMachine.constants.batch_max.create_transfers,
        .general,
    );

    pub const tree_name = @typeName(Transfer);
    pub const TableMutable = TableMutableType(Table, tree_name);

    pub fn derive_value(id: u64) Transfer {
        var value = std.mem.zeroes(Transfer);
        value.timestamp = id;
        return value;
    }
};

const TransferIdContext = struct {
    pub const tree_name = "Transfer.id";

    const config = constants.state_machine_config;
    const Storage = @import("../testing/storage.zig").Storage;
    const StateMachine = @import("../vsr.zig").state_machine.StateMachineType(Storage, config);

    const Key = @import("composite_key.zig").CompositeKey(u128);
    pub const Table = TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
        config.lsm_batch_multiple * StateMachine.constants.batch_max.create_transfers,
        .secondary_index,
    );

    pub const TableMutable = TableMutableType(Table, tree_name);

    pub fn derive_value(id: u64) Key.Value {
        return .{ .field = id, .timestamp = 0 };
    }
};

const TransferDebitsPendingContext = struct {
    pub const tree_name = "Transfer.debits_pending";

    const config = constants.state_machine_config;
    const Storage = @import("../testing/storage.zig").Storage;
    const StateMachine = @import("../vsr.zig").state_machine.StateMachineType(Storage, config);

    const Key = @import("composite_key.zig").CompositeKey(u128);
    pub const Table = TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
        config.lsm_batch_multiple * StateMachine.constants.batch_max.create_transfers,
        .secondary_index,
    );

    pub const TableMutable = TableMutableType(Table, tree_name);

    pub fn derive_value(id: u64) Key.Value {
        return .{ .field = id, .timestamp = 0 };
    }
};

const Order = enum {
    in_order,
    random_order,
};

pub fn main() !void {
    //try bench(TransferDebitsPendingContext);
    try bench(TransferIdContext);
    try bench(TransferContext);
}

fn bench(comptime Context: type) !void {
    const stdout = std.io.getStdOut().writer();
    var timer = try std.time.Timer.start();
    var prng = std.rand.DefaultPrng.init(42);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tombstone = Context.Table.tombstone;
    const compare_keys = Context.Table.compare_keys;
    const key_from_value = Context.Table.key_from_value;

    var table_mutable = try Context.TableMutable.init(allocator, null);
    defer table_mutable.deinit(allocator);

    var values = try allocator.alloc(Context.Table.Value, Context.Table.value_count_max);
    defer allocator.free(values);

    const co_prime = blk: {
        var i = values.len / 2;
        while (i < values.len) : (i += 1) {
            var gcd = [_]usize{ i, values.len };
            while (gcd[0] != gcd[1]) {
                const gt = std.math.order(gcd[0], gcd[1]) == .gt;
                gcd[@boolToInt(!gt)] -= gcd[@boolToInt(gt)];
            }
            if (gcd[0] == 1) break :blk i;
        }
        break :blk 1;
    };

    try stdout.print("TableMutable({s})\n{s}\n", .{
        Context.tree_name,
        "-" ** 32,
    });

    for ([_]Order{ .in_order, .random_order }) |insert_order| {
        var rand_i = prng.random().int(usize) % values.len;
        for (values) |*v, i| {
            const k = switch (insert_order) {
                .in_order => i,
                .random_order => blk: {
                    const new_i = rand_i;
                    rand_i += co_prime;
                    if (rand_i >= values.len) rand_i -= values.len;
                    break :blk new_i;
                },
            };
            v.* = Context.derive_value(k);
            assert(!tombstone(v));
        }

        const insert_start = timer.read();
        for (values) |*v| table_mutable.put(v);
        const insert_elapsed = timer.read() - insert_start;

        try stdout.print("insert({}): {}\n", .{
            insert_order,
            std.fmt.fmtDuration(insert_elapsed),
        });

        var hit_elapsed: u64 = 0;
        var miss_elapsed: u64 = 0;
        for (values) |*v, i| {
            const invalid_value = Context.derive_value(i + values.len);
            const hit_key = key_from_value(v);
            const miss_key = key_from_value(&invalid_value);

            const hit_start = timer.read();
            const hit_result = table_mutable.get(hit_key);
            hit_elapsed += timer.read() - hit_start;

            const miss_start = timer.read();
            const miss_result = table_mutable.get(miss_key);
            miss_elapsed += timer.read() - miss_start;

            assert(hit_result != null);
            assert(miss_result == null);
            assert(std.mem.eql(
                u8,
                std.mem.asBytes(hit_result.?),
                std.mem.asBytes(v),
            ));
        }

        try stdout.print("\tpositive lookup total: {}\n\tnegative lookup total: {}\n", .{
            std.fmt.fmtDuration(hit_elapsed),
            std.fmt.fmtDuration(miss_elapsed),
        });

        const sort_start = timer.read();
        const sorted = table_mutable.sort_into_values_and_clear(values);
        const sort_elapsed = timer.read() - sort_start;

        assert(sorted.ptr == values.ptr);
        assert(sorted.len == values.len);
        for (sorted[1..]) |*v, i| {
            const prev_key = key_from_value(&sorted[i]);
            assert(compare_keys(prev_key, key_from_value(v)) == .lt);
        }

        try stdout.print("\tsort({}): {}\n", .{
            insert_order,
            std.fmt.fmtDuration(sort_elapsed),
        });
    }

    try stdout.print("\n", .{});
}
