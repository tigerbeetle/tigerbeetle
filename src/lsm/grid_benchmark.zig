const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.grid_benchmark);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");
const data_file_size_min = @import("../vsr/superblock.zig").data_file_size_min;
const IO = @import("../io.zig").IO;
const Storage = @import("../storage.zig").Storage;
const MessagePool = @import("../message_pool.zig").MessagePool;
const SuperBlock = @import("../vsr.zig").SuperBlockType(Storage);
const Grid = @import("grid.zig").GridType(Storage);
const alloc_block = @import("grid.zig").alloc_block;
const Header = @import("../vsr.zig").Header;
const TableType = @import("table.zig").TableType;

const Key = packed struct {
    id: u64 align(@alignOf(u64)),

    const Value = packed struct {
        id: u64,
        value: u63,
        tombstone: u1 = 0,
    };

    inline fn compare_keys(a: Key, b: Key) std.math.Order {
        return std.math.order(a.id, b.id);
    }

    inline fn key_from_value(value: *const Key.Value) Key {
        return Key{ .id = value.id };
    }

    const sentinel_key = Key{
        .id = std.math.maxInt(u64),
    };

    inline fn tombstone(value: *const Key.Value) bool {
        return value.tombstone != 0;
    }

    inline fn tombstone_from_key(key: Key) Key.Value {
        return Key.Value{
            .id = key.id,
            .value = 0,
            .tombstone = 1,
        };
    }
};

const Table = TableType(
    Key,
    Key.Value,
    Key.compare_keys,
    Key.key_from_value,
    Key.sentinel_key,
    Key.tombstone,
    Key.tombstone_from_key,
    .general,
);

pub const tigerbeetle_config = @import("../config.zig").configs.default_production;

const Environment = struct {
    const State = enum {
        idle,
        superblock_format,
        superblock_open,
        writing,
        reading,
    };

    state: State = .idle,
    storage: *Storage,

    superblock_context: SuperBlock.Context = undefined,

    write: Grid.Write = undefined,

    block_read: Grid.BlockPtrConst = undefined,
    read: Grid.Read = undefined,

    fn change_state(env: *Environment, current_state: State, next_state: State) void {
        assert(env.state == current_state);
        env.state = next_state;
    }

    fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) void {
        while (env.state == current_state) env.storage.tick();
        assert(env.state == next_state);
    }

    fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_format, .idle);
    }

    fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
        env.change_state(.superblock_open, .idle);
    }

    fn write_callback(write: *Grid.Write) void {
        const env = @fieldParentPtr(Environment, "write", write);
        env.change_state(.writing, .idle);
    }

    fn read_callback(read: *Grid.Read, block: Grid.BlockPtrConst) void {
        const env = @fieldParentPtr(Environment, "read", read);
        env.block_read = block;
        env.change_state(.reading, .idle);
    }
};

const block_count = 100_000;

const cluster = 32;
const replica = 4;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    try tracer.init(allocator);
    defer tracer.deinit(allocator);

    const dir_fd = try IO.open_dir(".");
    const must_create = false;
    const fd = try IO.open_file(dir_fd, "test", data_file_size_min, must_create);

    var io = try IO.init(128, 0);

    var storage = try Storage.init(&io, fd);

    var message_pool = try MessagePool.init(allocator, .replica);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = constants.storage_size_max,
        .message_pool = &message_pool,
    });

    var grid = try Grid.init(allocator, &superblock);

    var env = Environment{
        .storage = &storage,
    };

    env.change_state(.idle, .superblock_format);
    superblock.format(Environment.superblock_format_callback, &env.superblock_context, .{
        .cluster = cluster,
        .replica = replica,
    });
    env.tick_until_state_change(.superblock_format, .idle);

    env.change_state(.idle, .superblock_open);
    superblock.open(Environment.superblock_open_callback, &env.superblock_context);
    env.tick_until_state_change(.superblock_open, .idle);

    const checksums = try allocator.alloc(u128, block_count);

    // Zero out the file.
    {
        var table_builder = try Table.Builder.init(allocator);

        const reservation = grid.reserve(block_count).?;
        defer grid.forfeit(reservation);

        var block_index: usize = 0;
        while (block_index < block_count) : (block_index += 1) {
            const block_address = block_index + 1;
            assert(grid.acquire(reservation) == block_address);

            const value = Key.Value{
                .id = block_index,
                .value = 0,
            };
            table_builder.data_block_append(&value);
            table_builder.data_block_finish(.{
                .cluster = cluster,
                .address = block_address,
            });

            // We have to finish the other blocks even though we don't use them.
            table_builder.filter_block_finish(.{
                .cluster = cluster,
                .address = block_address,
            });
            _ = table_builder.index_block_finish(.{
                .cluster = cluster,
                .address = block_address,
                .snapshot_min = 0,
            });

            const header_bytes = table_builder.data_block[0..@sizeOf(Header)];
            const header = std.mem.bytesAsValue(Header, header_bytes);
            checksums[block_index] = header.checksum;

            env.change_state(.idle, .writing);
            grid.write_block(
                Environment.write_callback,
                &env.write,
                &table_builder.data_block,
                block_address,
            );
            env.tick_until_state_change(.writing, .idle);
        }
    }

    try read_blocks(&grid, &env, checksums, false);

    try read_blocks(&grid, &env, checksums, true);
}

fn read_blocks(
    grid: *Grid,
    env: *Environment,
    checksums: []u128,
    hash_blocks: bool,
) !void {
    // Clear the cache.
    grid.cache.reset();

    var hasher = std.crypto.hash.Blake3.init(.{});

    // Read `block_count` blocks and hash them.
    const timer = try std.time.Timer.start();
    var hash_index: usize = 0;
    var block_index: usize = 0;
    while (hash_index < block_count) : (hash_index += 1) {
        // Take long prime-number strides to confound prefetching and avoid caching.
        block_index = (block_index + 101) % block_count;
        env.change_state(.idle, .reading);
        grid.read_block(
            Environment.read_callback,
            &env.read,
            block_index + 1,
            checksums[block_index],
            .data,
        );
        env.tick_until_state_change(.reading, .idle);
        if (hash_blocks) hasher.update(env.block_read);
    }
    const time_ns = timer.read();

    const time_s = @intToFloat(f64, time_ns) / 1000 / 1000 / 1000;
    const bandwidth_mb_per_s = @intToFloat(f64, block_count) * constants.block_size / 1024 / 1024 / time_s;
    const verb: []const u8 = if (hash_blocks) "Read and hashed" else "Read";
    std.debug.print(
        "{s} {} blocks in {d:2}s => {d:2}mb/s\n",
        .{ verb, block_count, time_s, bandwidth_mb_per_s },
    );
}
