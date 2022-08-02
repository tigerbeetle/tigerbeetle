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

const Environment = struct {
    dir_fd: os.fd_t,
    fd: os.fd_t,
    io: IO,
    storage: Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    grid: Grid,
    forest: Forest,

    fn init(env: *Environment, must_create: bool) !void {
        const dir_path = ".";
        env.dir_fd = try IO.open_dir(dir_path);
        errdefer std.os.close(env.dir_fd);

        const cluster = 32;
        const replica = 4;
        const size_max = (512 + 64) * 1024 * 1024;

        env.fd = try IO.open_file(dir_fd, "test_forest", size_max, must_create);
        errdefer std.os.close(env.fd);

        env.io = try IO.init(128, 0);
        errdefer env.io.deinit();

        env.storage = try Storage.init(&env.io, fd);
        errdefer env.storage.deinit();

        env.message_pool = try MessagePool.init(allocator, .replica);

        env.superblock = try SuperBlock.init(allocator, &env.storage, &env.message_pool);
        errdefer env.superblock.deinit(allocator);

        env.grid = try Grid.init(allocator, &env.superblock);
        errdefer env.grid.deinit(allocator);

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

        env.forest = try Forest.init(allocator, &env.grid, node_count, forest_config);
        errdefer env.forest.deinit(allocator);
    }

    fn deinit(env: *Environment) void {
        env.forest.deinit(allocator);
        env.grid.deinit(allocator);
        env.superblock.deinit(allocator);
        // message_pool doesn't need to be deinit()
        env.storage.deinit(allocator);
        env.io.deinit(allocator);
        std.os.close(env.fd);
        std.os.close(env.dir_fd);

        env.* = undefined;
    }

    fn superblock_eval(env: *Environment, comptime func_name: []const u8, args: anytype) !void {
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
            .{ &env.superblock, EvalContext.callback, &eval_context.superblock_context } ++ args,
        );

        while (!eval_context.evaluated) {
            try env.io.tick();
        }
    }
};

pub fn main() !void {
    var env: Environment = undefined;

    // Setup the storage file:
    {
        const must_create = true;
        try env.init(must_create);
        defer env.deinit();

        try env.superblock_eval("format", .{
            SuperBlock.FormatOptions{
                .cluster = cluster,
                .replica = replica,
                .size_max = size_max,
            },
        });
    }
    
    // Run insert tests
    const must_create = false;
    try env.init(must_create);
    defer env.deinit();

    // TODO
}

