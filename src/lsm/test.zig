const std = @import("std");
const testing = std.testing;
const allocator = testing.allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../storage.zig").Storage;
const IO = @import("../io.zig").IO;

const GridType = @import("grid.zig").GridType;
const GrooveType = @import("groove.zig").GrooveType;
const ForestType = @import("forest.zig").ForestType;

const Grid = GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const Forest = ForestType(Storage, .{
    .accounts = GrooveType(
        Storage,
        Account,
        .{
            .ignored = &[_][]const u8{ "reserved", "flags" },
            .derived = .{},
        },
    ),
    .transfers = GrooveType(
        Storage,
        Transfer,
        .{
            .ignored = &[_][]const u8{ "reserved", "flags" },
            .derived = .{},
        },
    ),
});

pub fn main() !void {
    const dir_path = ".";
    const dir_fd = try IO.open_dir(dir_path);
    defer std.os.close(dir_fd);

    const cluster = 32;
    const replica = 4;
    const size_max = (512 + 64) * 1024 * 1024;

    const must_create = true;
    const fd = try IO.open_file(dir_fd, "test_forest", size_max, must_create);
    defer std.os.close(fd);

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, fd);
    defer storage.deinit();

    var message_pool = try MessagePool.init(allocator, .replica);

    var superblock = try SuperBlock.init(allocator, &storage, &message_pool);
    defer superblock.deinit(allocator);

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    const node_count = 1024;
    const cache_size = 2 * 1024 * 1024;
    const forest_config = .{
        .transfers = .{
            .cache_size = cache_size,
            .commit_count_max = 8191 * 2,
        },
        .accounts = .{
            .cache_size = cache_size,
            .commit_count_max = 8191,
        },
    };

    var forest = try Forest.init(allocator, &grid, node_count, forest_config);
    defer forest.deinit(allocator);

    try superblock_eval(&superblock, &io, "format", .{
        .{
            .cluster = cluster,
            .replica = replica,
            .size_max = size_max,
        },
    });

    // --------------------------------------------------------------------------
}

fn superblock_eval(
    superblock: *SuperBlock, 
    io: *IO, 
    comptime func_name: []const u8,
    args: anytype,
) !void {
    const EvalContext = struct {
        superblock_context: SuperBlock.Context = undefined,
        evaluated: bool = false,

        fn callback(superblock_context: *SuperBlock.Context) void {
            const eval_context = @fieldParentPtr(
                @This(), 
                "superblock_context", 
                superblock_context,
            );
            assert(!eval_context.evaluated);
            eval_context.evaluated = true;
        }
    };

    var eval_context = EvalContext{};
    @call(
        .{}, 
        @field(SuperBlock, func_name), 
        .{ superblock, EvalContext.callback, &eval_context.superblock_context } ++ args,
    );

    while (!eval_context.evaluated) {
        try io.tick();
    }
}