const std = @import("std");
const stdx = vsr.stdx;
const assert = std.debug.assert;
const log = std.log.scoped(.scrub);

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

pub fn command_scrub(
    gpa: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    args: *const cli.Command.Scrub,
) !void {
    var scrub: Scrub = undefined;
    var scrubbed_bytes: u64 = 0;

    // The superblock is checked as part of initalizing the scrubber - it cannot be skipped.
    const scrubbed_bytes_superblock = try Scrub.init(
        &scrub,
        gpa,
        io,
        tracer,
        args.path,
        args.lsm_forest_node_count,
    );
    defer scrub.deinit(gpa);

    scrubbed_bytes += scrubbed_bytes_superblock;

    try scrub.open();

    if (!args.skip_wal) {
        const scrubbed_bytes_wal = try scrub.scrub_wal();
        assert(
            scrubbed_bytes_wal == vsr.Zone.wal_headers.size().? + vsr.Zone.wal_prepares.size().?,
        );

        scrubbed_bytes += scrubbed_bytes_wal;
    }

    if (!args.skip_client_replies) {
        const scrubbed_bytes_client_replies = try scrub.scrub_client_replies();
        assert(scrubbed_bytes_client_replies == vsr.Zone.client_replies.size().?);

        scrubbed_bytes += scrubbed_bytes_client_replies;
    }

    const grid_blocks_expected_count =
        (scrub.grid.free_set.count_acquired() - scrub.grid.free_set.count_released()) +
        scrub.grid.free_set_checkpoint_blocks_acquired.block_count() +
        scrub.grid.free_set_checkpoint_blocks_released.block_count();

    if (!args.skip_grid) {
        // If no seed was given, use a random seed for better coverage.
        const seed: u64 = seed_from_arg: {
            const seed_argument = args.seed orelse
                break :seed_from_arg @truncate(stdx.unique_u128());
            break :seed_from_arg vsr.testing.parse_seed(seed_argument);
        };

        const scrubbed_bytes_grid = try scrub.scrub_grid(seed);
        assert(scrubbed_bytes_grid == grid_blocks_expected_count * constants.block_size);

        scrubbed_bytes += scrubbed_bytes_grid;
    }

    const scrubbed_bytes_target: u64 = blk: {
        var scrubbed_bytes_target: u64 = 0;

        for (std.enums.values(vsr.Zone)) |zone| {
            if (vsr.Zone.size(zone)) |size| {
                scrubbed_bytes_target += size;
            }
        }
        scrubbed_bytes_target += grid_blocks_expected_count * constants.block_size;
        scrubbed_bytes_target -= vsr.Zone.grid_padding.size().?;

        // At this point, scrubbed_bytes_target is the highest it will ever be if everything were
        // scrubbed. Then, subtract off the skipped items explicitly.

        if (args.skip_wal) {
            scrubbed_bytes_target -= vsr.Zone.wal_headers.size().? + vsr.Zone.wal_prepares.size().?;
        }

        if (args.skip_client_replies) {
            scrubbed_bytes_target -= vsr.Zone.client_replies.size().?;
        }

        if (args.skip_grid) {
            scrubbed_bytes_target -= grid_blocks_expected_count * constants.block_size;
        }

        break :blk scrubbed_bytes_target;
    };

    assert(scrubbed_bytes == scrubbed_bytes_target);
}

const Scrub = @This();

io: *IO,
storage: Storage,

superblock: SuperBlock,
client_sessions_checkpoint: CheckpointTrailer,
grid: Grid,
forest: Forest,
grid_scrubber: GridScrubber,
grid_blocks_scrubbed: std.AutoArrayHashMapUnmanaged(u64, void),

buffer_headers: []u8,
buffer_prepare: []u8,

fn init(
    scrub: *Scrub,
    gpa: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    path: []const u8,
    lsm_forest_node_count: u32,
) !u64 {
    var scrubbed_bytes: u64 = 0;

    scrub.io = io;

    scrub.storage = try Storage.init(io, tracer, .{
        .path = path,
        .size_min = vsr.superblock.data_file_size_min,
        .purpose = .inspect,
        .direct_io = .direct_io_optional,
    });
    errdefer scrub.storage.deinit();

    const data_file_stat = try (std.fs.File{ .handle = scrub.storage.fd }).stat();

    scrub.superblock = try SuperBlock.init(
        gpa,
        &scrub.storage,
        .{
            .storage_size_limit = std.mem.alignForward(
                u64,
                data_file_stat.size,
                constants.block_size,
            ),
        },
    );
    errdefer scrub.superblock.deinit(gpa);

    // Opening the forest requires an open superblock, so this is done explicitly here and not in
    // open() like the others.
    var superblock_context: SuperBlock.Context = undefined;
    scrub.superblock.open(struct {
        fn superblock_open_callback(_: *SuperBlock.Context) void {}
    }.superblock_open_callback, &superblock_context);
    while (!scrub.superblock.opened) scrub.superblock.storage.run();

    // Unlike the other zones, the superblock has redundant copies internally. Consider the
    // superblock to be valid if it's valid as a whole, even if an individual copy might be corrupt.
    scrub.superblock.working.vsr_state.assert_internally_consistent();
    for (scrub.superblock.reading) |_| {
        scrubbed_bytes += vsr.superblock.superblock_copy_size;
    }
    log.info("superblock opened and scrubbed", .{});

    scrub.client_sessions_checkpoint = try CheckpointTrailer.init(
        gpa,
        .client_sessions,
        vsr.ClientSessions.encode_size,
    );
    errdefer scrub.client_sessions_checkpoint.deinit(gpa);

    scrub.grid = try Grid.init(gpa, .{
        .superblock = &scrub.superblock,
        .trace = tracer,
        .cache_blocks_count = Grid.Cache.value_count_max_multiple,
        .missing_blocks_max = constants.grid_missing_blocks_max,
        .missing_tables_max = constants.grid_missing_tables_max,
        .blocks_released_prior_checkpoint_durability_max = Forest
            .compaction_blocks_released_per_pipeline_max() +
            vsr.checkpoint_trailer.block_count_for_trailer_size(vsr.ClientSessions.encode_size),
    });
    errdefer scrub.grid.deinit(gpa);

    try scrub.forest.init(
        gpa,
        &scrub.grid,
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
    errdefer scrub.forest.deinit(gpa);

    scrub.grid_scrubber = try GridScrubber.init(
        gpa,
        &scrub.forest,
        &scrub.client_sessions_checkpoint,
    );
    errdefer scrub.grid_scrubber.deinit(gpa);

    scrub.grid_blocks_scrubbed = .{};

    try scrub.grid_blocks_scrubbed.ensureTotalCapacity(
        gpa,
        // Safe estimation for the maximum number of grid blocks based on file size. Using
        // storage_size_limit_max would increase the memory usage dramatically for small data files.
        @divFloor(data_file_stat.size, constants.block_size),
    );
    errdefer scrub.grid_blocks_scrubbed.deinit(gpa);

    scrub.buffer_headers = try gpa.alignedAlloc(
        u8,
        constants.sector_size,
        constants.journal_size_headers,
    );
    errdefer gpa.free(scrub.buffer_headers);

    scrub.buffer_prepare = try gpa.alignedAlloc(
        u8,
        constants.sector_size,
        constants.message_size_max,
    );
    errdefer gpa.free(scrub.buffer_prepare);

    return scrubbed_bytes;
}

fn open(scrub: *Scrub) !void {
    scrub.client_sessions_checkpoint.open(
        &scrub.grid,
        scrub.superblock.working.client_sessions_reference(),
        struct {
            fn client_sessions_checkpoint_callback(_: *CheckpointTrailer) void {}
        }.client_sessions_checkpoint_callback,
    );
    while (scrub.client_sessions_checkpoint.callback != .none) {
        try scrub.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("client sessions checkpoint opened", .{});

    scrub.grid.open(struct {
        fn grid_open_callback(_: *Grid) void {}
    }.grid_open_callback);
    while (scrub.grid.callback != .none) {
        try scrub.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("grid opened", .{});

    scrub.forest.open(struct {
        fn forest_open_callback(_: *Forest) void {}
    }.forest_open_callback);
    while (scrub.forest.progress != null) {
        try scrub.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    log.debug("forest opened", .{});
}

fn deinit(scrub: *Scrub, gpa: std.mem.Allocator) void {
    gpa.free(scrub.buffer_headers);
    gpa.free(scrub.buffer_prepare);

    scrub.grid_blocks_scrubbed.deinit(gpa);
    scrub.grid_scrubber.deinit(gpa);
    scrub.forest.deinit(gpa);
    scrub.grid.deinit(gpa);
    scrub.client_sessions_checkpoint.deinit(gpa);
    scrub.superblock.deinit(gpa);
    scrub.storage.deinit();
}

/// Scrubs the WAL headers and prepares, using sync IO.
fn scrub_wal(scrub: *Scrub) !u64 {
    var scrubbed_bytes: u64 = 0;

    const headers_bytes_read = try scrub.sync_read_all(
        scrub.buffer_headers,
        vsr.Zone.wal_headers.start(),
    );
    assert(headers_bytes_read == scrub.buffer_headers.len);

    const wal_headers: []vsr.Header.Prepare align(16) = @alignCast(
        std.mem.bytesAsSlice(vsr.Header.Prepare, scrub.buffer_headers),
    );

    for (wal_headers, 0..) |*wal_header, slot| {
        const offset = slot * constants.message_size_max;

        const bytes_read = try scrub.sync_read_all(
            scrub.buffer_prepare,
            vsr.Zone.wal_prepares.start() + offset,
        );
        assert(bytes_read == scrub.buffer_prepare.len);

        const wal_prepare: *align(16) vsr.Header.Prepare = @alignCast(std.mem.bytesAsValue(
            vsr.Header.Prepare,
            scrub.buffer_prepare[0..@sizeOf(vsr.Header)],
        ));

        const wal_prepare_body_valid =
            wal_prepare.valid_checksum() and
            wal_prepare.valid_checksum_body(
                scrub.buffer_prepare[@sizeOf(vsr.Header)..wal_prepare.size],
            );

        assert(wal_header.valid_checksum() and wal_prepare_body_valid);
        assert(wal_header.checksum == wal_prepare.checksum);
        scrubbed_bytes += bytes_read;
    }

    assert(wal_headers.len == constants.journal_slot_count);
    scrubbed_bytes += headers_bytes_read;

    log.info("successfully scrubbed {} wal headers and prepares", .{wal_headers.len});
    return scrubbed_bytes;
}

/// Scrubs the client replies, using sync IO.
fn scrub_client_replies(scrub: *Scrub) !u64 {
    var scrubbed_bytes: u64 = 0;

    for (0..constants.clients_max) |slot| {
        const offset = slot * constants.message_size_max;

        const bytes_read = try scrub.sync_read_all(
            scrub.buffer_prepare,
            vsr.Zone.client_replies.start() + offset,
        );
        assert(bytes_read == scrub.buffer_prepare.len);

        const reply: *align(16) vsr.Header.Reply = @alignCast(std.mem.bytesAsValue(
            vsr.Header.Reply,
            scrub.buffer_prepare[0..@sizeOf(vsr.Header)],
        ));

        const reply_empty = reply.checksum == 0 and reply.checksum_body == 0;
        const reply_valid = reply.valid_checksum() and
            reply.valid_checksum_body(scrub.buffer_prepare[@sizeOf(vsr.Header)..reply.size]);
        assert(reply_empty or reply_valid);

        scrubbed_bytes += bytes_read;
    }

    log.info("successfully scrubbed {} client replies", .{constants.clients_max});
    return scrubbed_bytes;
}

/// Scrubs the grid, using the grid scrubber.
fn scrub_grid(scrub: *Scrub, seed: u64) !u64 {
    var scrubbed_bytes: u64 = 0;
    const grid = &scrub.grid;

    // The free set isn't included in the grid's acquired count, but is scrubbed.
    // Ensure this is accounted for.
    const blocks_expected_count =
        (grid.free_set.count_acquired() - grid.free_set.count_released()) +
        grid.free_set_checkpoint_blocks_acquired.block_count() +
        grid.free_set_checkpoint_blocks_released.block_count();

    log.info("grid scrubber starting on {} blocks with seed {}...", .{
        blocks_expected_count,
        seed,
    });

    var prng = stdx.PRNG.from_seed(seed);
    scrub.grid_scrubber.open(&prng);

    const parent_progress_node = std.Progress.start(.{
        .root_name = "scrubbing grid blocks",
        .estimated_total_items = blocks_expected_count,
    });
    defer parent_progress_node.end();

    var timer = try std.time.Timer.start();
    timer.reset();

    while (true) {
        for (0..scrub.grid_scrubber.reads.available() + 1) |_| {
            if (scrub.grid_scrubber.tour == .done) break;
            if (!scrub.grid_scrubber.read_next()) break;
        }

        while (scrub.grid_scrubber.read_result_next()) |read| {
            assert(read == .ok);

            // `read_result_next` returns addresses, starting from 1, but the free sets and such
            // are zero indexed.
            const gop = scrub.grid_blocks_scrubbed.getOrPutAssumeCapacity(read.ok - 1);

            // Only completeOne() if no existing entry was found, as the grid scrubber will
            // run through index blocks multiple times.
            if (!gop.found_existing) {
                scrubbed_bytes += constants.block_size;
                parent_progress_node.completeOne();
            }
        }

        // When the .tour is .done, there might still be reads executing. Wait for those, too.
        if (scrub.grid_scrubber.tour == .done and scrub.grid_scrubber.reads.executing() == 0) {
            break;
        }

        try scrub.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    const scrub_duration_ms = stdx.div_ceil(timer.read(), std.time.ns_per_ms);

    assert(scrub.grid_scrubber.tour == .done and scrub.grid_scrubber.reads.executing() == 0);

    // Verify that the blocks scrubbed from the grid scrubber and the free set itself are
    // identical.
    assert(scrub.grid_blocks_scrubbed.count() == blocks_expected_count);

    var acquired_iterator = grid.free_set.blocks_acquired.iterator(.{});
    while (acquired_iterator.next()) |block| {
        if (grid.free_set.blocks_released.isSet(block)) {
            continue;
        }

        assert(scrub.grid_blocks_scrubbed.get(block) != null);
    }

    // Check in reverse, too, that all the blocks we visited are listed in the free set.
    var visisted_iterator = scrub.grid_blocks_scrubbed.iterator();
    while (visisted_iterator.next()) |entry| {
        const in_free_set = grid.free_set.blocks_acquired.isSet(entry.key_ptr.*) and
            !grid.free_set.blocks_released.isSet(entry.key_ptr.*);

        assert(in_free_set);
    }

    const throughput = @divFloor(@divFloor(
        blocks_expected_count * constants.block_size,
        stdx.div_ceil(scrub_duration_ms, std.time.ms_per_s),
    ), 1024 * 1024);

    log.info("successfully scrubbed {} grid blocks in {}ms. ({}MiB/s)", .{
        blocks_expected_count,
        scrub_duration_ms,
        throughput,
    });

    return scrubbed_bytes;
}

/// Windows doesn't support using sync IO functions like preadAll on the handles IO opens.
fn sync_read_all(scrub: *Scrub, buffer: []u8, offset: u64) !u64 {
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
        scrub.io.read(
            *Context,
            &context,
            Context.read_callback,
            &completion,
            scrub.storage.fd,
            buffer[bytes_read..],
            offset,
        );

        while (context.bytes_read == null) {
            try scrub.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
        }

        if (context.bytes_read.? == 0) break;
        bytes_read += context.bytes_read.?;
    }

    assert(bytes_read == buffer.len);
    return bytes_read;
}
