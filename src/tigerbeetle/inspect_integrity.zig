const std = @import("std");
const stdx = vsr.stdx;
const assert = std.debug.assert;
const log = std.log.scoped(.integrity);

const cli = @import("cli.zig");
const vsr = @import("vsr");
const constants = vsr.constants;
const IO = vsr.io.IO;
const StateMachine = @import("main.zig").StateMachine;
const Replica = @import("main.zig").Replica;
const Storage = @import("main.zig").Storage;
const Grid = @import("main.zig").Grid;
const Tracer = vsr.trace.Tracer;

const Forest = StateMachine.Forest;
const CheckpointTrailer = vsr.CheckpointTrailerType(Storage);
const SuperBlock = Replica.SuperBlock;

/// Special offline GridScrubber type - uses constants.grid_iops_read_max rather than
/// constants.grid_scrubber_reads_max.
const GridScrubber = vsr.GridScrubberType(Forest, constants.grid_iops_read_max);

pub fn command_inspect_integrity(
    gpa: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    args: *const cli.Command.Inspect.Integrity,
) !void {
    var integrity: Integrity = undefined;
    var checked_bytes: u64 = 0;

    try Integrity.init(
        &integrity,
        gpa,
        io,
        tracer,
        args.path,
        args.lsm_forest_node_count,
    );
    defer integrity.deinit(gpa);

    // The superblock is checked as part of initializing and opening the checker - it cannot be
    // skipped. A fully corrupt superblock stops any further scrubbing.
    const checked_bytes_superblock = try integrity.open();
    checked_bytes += checked_bytes_superblock;

    if (!args.skip_wal) {
        const checked_bytes_wal = try integrity.check_wal();
        assert(
            checked_bytes_wal == vsr.Zone.wal_headers.size().? + vsr.Zone.wal_prepares.size().?,
        );

        checked_bytes += checked_bytes_wal;
    }

    if (!args.skip_client_replies) {
        const checked_bytes_client_replies = try integrity.check_client_replies();
        assert(checked_bytes_client_replies == vsr.Zone.client_replies.size().?);

        checked_bytes += checked_bytes_client_replies;
    }

    const grid_blocks_expected_count =
        (integrity.grid.free_set.count_acquired() - integrity.grid.free_set.count_released()) +
        integrity.grid.free_set_checkpoint_blocks_acquired.block_count() +
        integrity.grid.free_set_checkpoint_blocks_released.block_count();

    if (!args.skip_grid) {
        // If no seed was given, use a random seed for better coverage.
        const seed: u64 = seed_from_arg: {
            const seed_argument = args.seed orelse
                break :seed_from_arg @truncate(stdx.unique_u128());
            break :seed_from_arg vsr.testing.parse_seed(seed_argument);
        };

        const checked_bytes_grid = try integrity.check_grid(seed);
        assert(checked_bytes_grid == grid_blocks_expected_count * constants.block_size);

        checked_bytes += checked_bytes_grid;
    }

    const checked_bytes_target: u64 = blk: {
        var checked_bytes_target: u64 = 0;

        for (std.enums.values(vsr.Zone)) |zone| {
            if (vsr.Zone.size(zone)) |size| {
                checked_bytes_target += size;
            }
        }
        checked_bytes_target += grid_blocks_expected_count * constants.block_size;
        checked_bytes_target -= vsr.Zone.grid_padding.size().?;

        // At this point, checked_bytes_target is the highest it will ever be if everything were
        // checked. Then, subtract off the skipped items explicitly.

        if (args.skip_wal) {
            checked_bytes_target -= vsr.Zone.wal_headers.size().? + vsr.Zone.wal_prepares.size().?;
        }

        if (args.skip_client_replies) {
            checked_bytes_target -= vsr.Zone.client_replies.size().?;
        }

        if (args.skip_grid) {
            checked_bytes_target -= grid_blocks_expected_count * constants.block_size;
        }

        break :blk checked_bytes_target;
    };

    assert(checked_bytes == checked_bytes_target);
}

const Integrity = @This();

io: *IO,
storage: Storage,

superblock: SuperBlock,
client_sessions_checkpoint: CheckpointTrailer,
grid: Grid,
forest: Forest,
grid_scrubber: GridScrubber,
grid_blocks_scrubbed: std.bit_set.DynamicBitSetUnmanaged,

buffer_headers: []align(constants.sector_size) u8,
buffer_prepare: []align(constants.sector_size) u8,

fn init(
    integrity: *Integrity,
    gpa: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    path: []const u8,
    lsm_forest_node_count: u32,
) !void {
    integrity.io = io;

    integrity.storage = try Storage.init(io, tracer, .{
        .path = path,
        .size_min = vsr.superblock.data_file_size_min,
        .purpose = .inspect,
        .direct_io = .direct_io_optional,
    });
    errdefer integrity.storage.deinit();

    const data_file_stat = try (std.fs.File{ .handle = integrity.storage.fd }).stat();

    integrity.superblock = try SuperBlock.init(
        gpa,
        &integrity.storage,
        .{
            .storage_size_limit = std.mem.alignForward(
                u64,
                data_file_stat.size,
                constants.block_size,
            ),
        },
    );
    errdefer integrity.superblock.deinit(gpa);

    // Opening the forest requires an open superblock, so this is done explicitly here and not in
    // open() like the others.
    var superblock_context: SuperBlock.Context = undefined;
    integrity.superblock.open(struct {
        fn superblock_open_callback(_: *SuperBlock.Context) void {}
    }.superblock_open_callback, &superblock_context);
    while (!integrity.superblock.opened) integrity.superblock.storage.run();

    // Unlike the other zones, the superblock has redundant copies internally. Consider the
    // superblock to be valid if it's valid as a whole, even if an individual copy might be corrupt.
    integrity.superblock.working.vsr_state.assert_internally_consistent();
    log.info("superblock opened and checked", .{});

    integrity.client_sessions_checkpoint = try CheckpointTrailer.init(
        gpa,
        .client_sessions,
        vsr.ClientSessions.encode_size,
    );
    errdefer integrity.client_sessions_checkpoint.deinit(gpa);

    integrity.grid = try Grid.init(gpa, .{
        .superblock = &integrity.superblock,
        .trace = tracer,
        .cache_blocks_count = Grid.Cache.value_count_max_multiple,
        .missing_blocks_max = constants.grid_missing_blocks_max,
        .missing_tables_max = constants.grid_missing_tables_max,
        .blocks_released_prior_checkpoint_durability_max = Forest
            .compaction_blocks_released_per_pipeline_max() +
            vsr.checkpoint_trailer.block_count_for_trailer_size(vsr.ClientSessions.encode_size),
    });
    errdefer integrity.grid.deinit(gpa);

    try integrity.forest.init(
        gpa,
        &integrity.grid,
        .{
            .compaction_block_count = Forest.Options.compaction_block_count_min,
            .node_count = lsm_forest_node_count,
        },
        StateMachine.forest_options(.{
            .batch_size_limit = constants.message_body_size_max,
            .lsm_forest_compaction_block_count = Forest.Options.compaction_block_count_min,
            .lsm_forest_node_count = lsm_forest_node_count,

            .cache_entries_accounts = 0,
            .cache_entries_transfers = 0,
            .cache_entries_transfers_pending = 0,

            .log_trace = false,
        }),
    );
    errdefer integrity.forest.deinit(gpa);

    integrity.grid_scrubber = try GridScrubber.init(
        gpa,
        &integrity.forest,
        &integrity.client_sessions_checkpoint,
    );
    errdefer integrity.grid_scrubber.deinit(gpa);

    integrity.grid_blocks_scrubbed = try .initEmpty(
        gpa,
        // Safe estimation for the maximum number of grid blocks based on file size. Using
        // storage_size_limit_max would increase the memory usage dramatically for small data files.
        @divFloor(data_file_stat.size, constants.block_size),
    );
    errdefer integrity.grid_blocks_scrubbed.deinit(gpa);

    integrity.buffer_headers = try gpa.alignedAlloc(
        u8,
        constants.sector_size,
        constants.journal_size_headers,
    );
    errdefer gpa.free(integrity.buffer_headers);

    integrity.buffer_prepare = try gpa.alignedAlloc(
        u8,
        constants.sector_size,
        constants.message_size_max,
    );
    errdefer gpa.free(integrity.buffer_prepare);
}

fn open(integrity: *Integrity) !u64 {
    var checked_bytes: u64 = 0;

    assert(integrity.superblock.opened);
    integrity.superblock.working.vsr_state.assert_internally_consistent();
    for (integrity.superblock.reading) |_| {
        checked_bytes += vsr.superblock.superblock_copy_size;
    }

    integrity.client_sessions_checkpoint.open(
        &integrity.grid,
        integrity.superblock.working.client_sessions_reference(),
        struct {
            fn client_sessions_checkpoint_callback(_: *CheckpointTrailer) void {}
        }.client_sessions_checkpoint_callback,
    );
    while (integrity.client_sessions_checkpoint.callback != .none) {
        try integrity.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("client sessions checkpoint opened", .{});

    integrity.grid.open(struct {
        fn grid_open_callback(_: *Grid) void {}
    }.grid_open_callback);
    while (integrity.grid.callback != .none) {
        try integrity.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("grid opened", .{});

    integrity.forest.open(struct {
        fn forest_open_callback(_: *Forest) void {}
    }.forest_open_callback);
    while (integrity.forest.progress != null) {
        try integrity.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("forest opened", .{});

    return checked_bytes;
}

fn deinit(integrity: *Integrity, gpa: std.mem.Allocator) void {
    gpa.free(integrity.buffer_headers);
    gpa.free(integrity.buffer_prepare);

    integrity.grid_blocks_scrubbed.deinit(gpa);
    integrity.grid_scrubber.deinit(gpa);
    integrity.forest.deinit(gpa);
    integrity.grid.deinit(gpa);
    integrity.client_sessions_checkpoint.deinit(gpa);
    integrity.superblock.deinit(gpa);
    integrity.storage.deinit();
}

/// Checks the WAL headers and prepares, using sync IO.
fn check_wal(integrity: *Integrity) !u64 {
    var checked_bytes: u64 = 0;

    const headers_bytes_read = try integrity.sync_read_all(
        integrity.buffer_headers,
        vsr.Zone.wal_headers.start(),
    );
    assert(headers_bytes_read == integrity.buffer_headers.len);

    const wal_headers: []vsr.Header.Prepare align(16) =
        std.mem.bytesAsSlice(vsr.Header.Prepare, integrity.buffer_headers);

    for (wal_headers, 0..) |*wal_header, slot| {
        const offset = slot * constants.message_size_max;

        const bytes_read = try integrity.sync_read_all(
            integrity.buffer_prepare,
            vsr.Zone.wal_prepares.start() + offset,
        );
        assert(bytes_read == integrity.buffer_prepare.len);

        const wal_prepare: *align(16) vsr.Header.Prepare = std.mem.bytesAsValue(
            vsr.Header.Prepare,
            integrity.buffer_prepare[0..@sizeOf(vsr.Header)],
        );

        const wal_prepare_body_valid =
            wal_prepare.valid_checksum() and
            wal_prepare.valid_checksum_body(
                integrity.buffer_prepare[@sizeOf(vsr.Header)..wal_prepare.size],
            );

        assert(wal_header.valid_checksum());
        assert(wal_prepare_body_valid);
        assert(wal_header.checksum == wal_prepare.checksum);
        checked_bytes += bytes_read;
    }

    assert(wal_headers.len == constants.journal_slot_count);
    checked_bytes += headers_bytes_read;

    log.info("successfully checked {} wal headers and prepares", .{wal_headers.len});
    return checked_bytes;
}

/// Checks the client replies, using sync IO.
fn check_client_replies(integrity: *Integrity) !u64 {
    var checked_bytes: u64 = 0;

    for (0..constants.clients_max) |slot| {
        const offset = slot * constants.message_size_max;

        const bytes_read = try integrity.sync_read_all(
            integrity.buffer_prepare,
            vsr.Zone.client_replies.start() + offset,
        );
        assert(bytes_read == integrity.buffer_prepare.len);

        const reply: *align(16) vsr.Header.Reply = std.mem.bytesAsValue(
            vsr.Header.Reply,
            integrity.buffer_prepare[0..@sizeOf(vsr.Header)],
        );

        const reply_empty = reply.checksum == 0 and reply.checksum_body == 0;
        const reply_valid = reply.valid_checksum() and
            reply.valid_checksum_body(integrity.buffer_prepare[@sizeOf(vsr.Header)..reply.size]);
        assert(reply_empty or reply_valid);

        checked_bytes += bytes_read;
    }

    log.info("successfully checked {} client replies", .{constants.clients_max});
    return checked_bytes;
}

/// Checks the grid, using the grid scrubber.
fn check_grid(integrity: *Integrity, seed: u64) !u64 {
    var checked_bytes: u64 = 0;
    const grid = &integrity.grid;

    // The free set isn't included in the grid's acquired count, but is scrubbed.
    // Ensure this is accounted for.
    const blocks_expected_count =
        (grid.free_set.count_acquired() - grid.free_set.count_released()) +
        grid.free_set_checkpoint_blocks_acquired.block_count() +
        grid.free_set_checkpoint_blocks_released.block_count();

    log.info("checking {} grid blocks with seed {}...", .{
        blocks_expected_count,
        seed,
    });

    var prng = stdx.PRNG.from_seed(seed);
    integrity.grid_scrubber.open(&prng);

    const parent_progress_node = std.Progress.start(.{
        .root_name = "checking grid blocks",
        .estimated_total_items = blocks_expected_count,
    });
    defer parent_progress_node.end();

    var timer = try std.time.Timer.start();
    timer.reset();

    while (true) {
        for (0..integrity.grid_scrubber.reads.available() + 1) |_| {
            if (integrity.grid_scrubber.tour == .done) break;
            if (!integrity.grid_scrubber.read_next()) break;
        }

        while (integrity.grid_scrubber.read_result_next()) |result| {
            assert(result.status == .ok);

            // `read_result_next` returns addresses, starting from 1, but the free sets and such
            // are zero indexed.
            const block_set = integrity.grid_blocks_scrubbed.isSet(
                result.block.block_address - 1,
            );

            // Only completeOne() if no existing entry was found, as the grid scrubber will
            // run through index blocks multiple times.
            if (!block_set) {
                integrity.grid_blocks_scrubbed.set(result.block.block_address - 1);
                checked_bytes += constants.block_size;
                parent_progress_node.completeOne();
            }
        }

        // When the .tour is .done, there might still be reads executing. Wait for those, too.
        if (integrity.grid_scrubber.tour == .done and
            integrity.grid_scrubber.reads.executing() == 0)
        {
            break;
        }

        try integrity.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    const grid_duration_ms = stdx.div_ceil(timer.read(), std.time.ns_per_ms);

    assert(integrity.grid_scrubber.tour == .done and
        integrity.grid_scrubber.reads.executing() == 0);

    // Verify that the blocks checked from the grid scrubber and the free set itself are
    // identical.
    assert(integrity.grid_blocks_scrubbed.count() == blocks_expected_count);

    var acquired_iterator = grid.free_set.blocks_acquired.iterator(.{});
    while (acquired_iterator.next()) |block| {
        if (grid.free_set.blocks_released.isSet(block)) {
            continue;
        }

        assert(integrity.grid_blocks_scrubbed.isSet(block));
    }

    // Check in reverse, too, that all the blocks we visited are listed in the free set.
    var visisted_iterator = integrity.grid_blocks_scrubbed.iterator(.{});
    while (visisted_iterator.next()) |entry| {
        const in_free_set = grid.free_set.blocks_acquired.isSet(entry) and
            !grid.free_set.blocks_released.isSet(entry);

        assert(in_free_set);
    }

    const throughput = @divFloor(@divFloor(
        blocks_expected_count * constants.block_size,
        stdx.div_ceil(grid_duration_ms, std.time.ms_per_s),
    ), 1024 * 1024);

    log.info("successfully checked {} grid blocks in {}ms. ({}MiB/s)", .{
        blocks_expected_count,
        grid_duration_ms,
        throughput,
    });

    return checked_bytes;
}

/// Windows doesn't support using sync IO functions like preadAll on the handles IO opens.
fn sync_read_all(integrity: *Integrity, buffer: []u8, offset: u64) !u64 {
    var completion: IO.Completion = undefined;

    const Context = struct {
        bytes_read: ?u64 = null,

        fn read_callback(
            context: *@This(),
            _: *IO.Completion,
            result: IO.ReadError!usize,
        ) void {
            context.bytes_read = result catch unreachable;
        }
    };
    var context: Context = .{};
    var bytes_read: u64 = 0;

    while (bytes_read < buffer.len) {
        integrity.io.read(
            *Context,
            &context,
            Context.read_callback,
            &completion,
            integrity.storage.fd,
            buffer[bytes_read..],
            offset,
        );

        while (context.bytes_read == null) {
            try integrity.io.run();
        }

        if (context.bytes_read.? == 0) break;
        bytes_read += context.bytes_read.?;
    }

    assert(bytes_read == buffer.len);
    return bytes_read;
}
