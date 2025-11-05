//! Decode a TigerBeetle data file without running a replica or modifying the data file.
//!
//! This tool is intended for TigerBeetle developers, for debugging and understanding data files.
//!
//! Principles:
//! - Never modify the data file.
//! - Adhere to the "be liberal in what you accept" side of Postel's Law.
//!   When the data file is corrupt, decode as much as possible. (This is somewhat aspirational).
//! - Outside of the "summary" commands, don't discard potentially useful information.

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.inspect);

const cli = @import("cli.zig");
const vsr = @import("vsr");
const stdx = vsr.stdx;
const schema = vsr.lsm.schema;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Tracer = vsr.trace.Tracer;
const Storage = @import("main.zig").Storage;
const SuperBlockHeader = vsr.superblock.SuperBlockHeader;
const SuperBlockVersion = vsr.superblock.SuperBlockVersion;
const SuperBlockQuorums = vsr.superblock.Quorums;
const StateMachine = @import("main.zig").StateMachine;
const BlockPtr = vsr.grid.BlockPtr;
const BlockPtrConst = vsr.grid.BlockPtrConst;
const allocate_block = vsr.grid.allocate_block;
const is_composite_key = vsr.lsm.composite_key.is_composite_key;

const EventMetric = vsr.trace.EventMetric;
const EventMetricAggregate = vsr.trace.EventMetricAggregate;
const EventTiming = vsr.trace.EventTiming;
const EventTimingAggregate = vsr.trace.EventTimingAggregate;
const command_inspect_integrity = @import("inspect_integrity.zig").command_inspect_integrity;

pub fn command_inspect(
    allocator: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    cli_args: *const cli.Command.Inspect,
) !void {
    var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());
    var stdout_writer = stdout_buffer.writer();

    const inspect_result = run_inspect(allocator, io, tracer, cli_args, stdout_writer.any());
    const flush_result = stdout_buffer.flush();

    inline for (.{ inspect_result, flush_result }) |result| {
        result catch |err| switch (err) {
            // Ignore BrokenPipe so that e.g. "tigerbeetle inspect ... | head -n12" succeeds.
            error.BrokenPipe => {},
            else => return err,
        };
    }
}

fn run_inspect(
    allocator: std.mem.Allocator,
    io: *IO,
    tracer: *Tracer,
    cli_args: *const cli.Command.Inspect,
    stdout: std.io.AnyWriter,
) !void {
    const data_file = switch (cli_args.*) {
        .constants => return try inspect_constants(stdout),
        .metrics => return try inspect_metrics(stdout),
        .op => |op| return try inspect_op(stdout, op),
        .integrity => |*args| return try command_inspect_integrity(allocator, io, tracer, args),
        .data_file => |data_file| data_file,
    };

    const inspector = try Inspector.create(allocator, io, tracer, data_file.path);
    defer inspector.destroy();

    switch (data_file.query) {
        .superblock => try inspector.inspect_superblock(stdout),
        .wal => |args| {
            if (args.slot) |slot| {
                if (slot >= constants.journal_slot_count) {
                    return vsr.fatal(
                        .cli,
                        "--slot: slot exceeds {}",
                        .{constants.journal_slot_count - 1},
                    );
                }
                try inspector.inspect_wal_slot(stdout, slot);
            } else {
                try inspector.inspect_wal(stdout);
            }
        },
        .replies => |args| {
            if (args.slot) |slot| {
                if (slot >= constants.clients_max) {
                    return vsr.fatal(.cli, "--slot: slot exceeds {}", .{constants.clients_max - 1});
                }
                try inspector.inspect_replies_slot(stdout, args.superblock_copy, slot);
            } else {
                try inspector.inspect_replies(stdout, args.superblock_copy);
            }
        },
        .grid => |args| {
            if (args.superblock_copy != null and
                args.superblock_copy.? >= constants.superblock_copies)
            {
                return vsr.fatal(
                    .cli,
                    "--superblock-copy: copy exceeds {}",
                    .{constants.superblock_copies - 1},
                );
            }

            if (args.block) |address| {
                try inspector.inspect_grid_block(stdout, address);
            } else {
                try inspector.inspect_grid(stdout, args.superblock_copy);
            }
        },
        .manifest => |args| {
            if (args.superblock_copy != null and
                args.superblock_copy.? >= constants.superblock_copies)
            {
                return vsr.fatal(
                    .cli,
                    "--superblock-copy: copy exceeds {}",
                    .{constants.superblock_copies - 1},
                );
            }

            try inspector.inspect_manifest(stdout, args.superblock_copy);
        },
        .tables => |args| {
            if (args.superblock_copy != null and
                args.superblock_copy.? >= constants.superblock_copies)
            {
                return vsr.fatal(
                    .cli,
                    "--superblock-copy: copy exceeds {}",
                    .{constants.superblock_copies - 1},
                );
            }

            const tree_id = parse_tree_id(args.tree) orelse {
                return vsr.fatal(.cli, "--tree: invalid tree name/id: {s}", .{args.tree});
            };
            try inspector.inspect_tables(stdout, args.superblock_copy, .{
                .tree_id = tree_id,
                .level = args.level,
            });
        },
    }
}

fn inspect_constants(output: std.io.AnyWriter) !void {
    try output.print("VSR:\n", .{});
    try print_header(output, 0, "prepare_queue");
    try output.print("{}\n", .{constants.pipeline_prepare_queue_max});
    try print_header(output, 0, "request_queue");
    try output.print("{}\n", .{constants.pipeline_request_queue_max});
    try print_header(output, 0, "prepare_cache");
    try output.print("{}\n", .{
        constants.pipeline_prepare_queue_max + constants.pipeline_request_queue_max,
    });
    try output.print("\n", .{});

    try output.print("LSM:\n", .{});
    try print_header(output, 0, "compaction_ops");
    try output.print("{}\n", .{constants.lsm_compaction_ops});
    try print_header(output, 0, "checkpoint_ops");
    try output.print("{}\n", .{constants.vsr_checkpoint_ops});
    try print_header(output, 0, "journal_slot_count");
    try output.print("{}\n", .{constants.journal_slot_count});
    try output.print("\n", .{});

    try output.print("Data File Layout:\n", .{});
    inline for (comptime std.enums.values(vsr.Zone)) |zone| {
        try print_header(output, 0, @tagName(zone));
        switch (zone) {
            inline else => |zone_sized| {
                try print_size_count(
                    output,
                    zone_sized.size().?,
                    1,
                );
            },
            .grid => {
                try output.print("elastic\n", .{});
            },
        }
        switch (zone) {
            .superblock => {
                try print_header(output, 1, "copy");
                try print_size_count(
                    output,
                    vsr.superblock.superblock_copy_size,
                    constants.superblock_copies,
                );
            },
            .wal_headers => {
                try print_header(output, 1, "sector");
                try print_size_count(
                    output,
                    constants.sector_size,
                    @divExact(vsr.Zone.wal_headers.size().?, constants.sector_size),
                );

                try print_header(output, 2, "header");
                try print_size_count(
                    output,
                    @sizeOf(vsr.Header),
                    @divExact(constants.sector_size, @sizeOf(vsr.Header)),
                );
            },
            .wal_prepares => {
                try print_header(output, 1, "prepare");
                try print_size_count(
                    output,
                    constants.message_size_max,
                    constants.journal_slot_count,
                );
            },
            .client_replies => {
                try print_header(output, 1, "reply");
                try print_size_count(
                    output,
                    constants.message_size_max,
                    constants.clients_max,
                );
            },
            .grid => {
                try print_header(output, 1, "block");
                try print_size_count(output, constants.block_size, 1);
            },
            else => {},
        }
        try output.print("\n", .{});
    }

    // Print the size required to store each object + indexes.
    try output.print("StateMachine:\n", .{});
    try print_objects(output);

    // Memory usage is intentionally estimated from constants, rather than measured, to sanity
    // check that our observed memory usage is reasonable.
    try output.print("Memory (approximate):\n", .{});
    const datafile_size = constants.storage_size_limit_max;
    try print_header(output, 0, "datafile (on disk)");
    try output.print("{}\n", .{
        stdx.fmt_int_size_bin_exact(datafile_size),
    });

    {
        const grid_size_limit = datafile_size - vsr.superblock.data_file_size_min;
        const blocks_count = vsr.FreeSet.block_count_max(grid_size_limit);
        const ewah = vsr.ewah(vsr.FreeSet.Word);

        // 2x since both `blocks_acquired` and `blocks_released` are encoded.
        const free_set_encoded_blocks_max = 2 *
            vsr.checkpoint_trailer.block_count_for_trailer_size(ewah.encode_size_max(blocks_count));

        const client_sessions_encoded_blocks_max =
            vsr.checkpoint_trailer.block_count_for_trailer_size(vsr.ClientSessions.encode_size);

        try print_header(output, 0, "free_set");
        const hashmap_entries = stdx.div_ceil(
            100 * (StateMachine.Forest.compaction_blocks_released_per_pipeline_max() +
                free_set_encoded_blocks_max + client_sessions_encoded_blocks_max),
            std.hash_map.default_max_load_percentage,
        );

        try output.print("{:.2}\n", .{std.fmt.fmtIntSizeBin(
            // HashMap of block addresses plus two bitsets with bit per block.
            hashmap_entries * @sizeOf(u64) + 2 * stdx.div_ceil(blocks_count, 8),
        )});
    }
}

fn inspect_metrics(output: std.io.AnyWriter) !void {
    const EventMetricTag = std.meta.Tag(EventMetric);
    const EventTimingTag = std.meta.Tag(EventTiming);

    const stats_per_gauge = std.meta.fields(EventMetricAggregate).len - 1; // -1 to ignore `event`.
    const stats_per_timing = std.meta.fields(std.meta.FieldType(EventTimingAggregate, .values)).len;
    var stats_total: usize = 0;

    log.info("Format: [metric type]: [metric name]([metric tags])=[metric cardinality]", .{});

    inline for (std.meta.fields(EventMetric)) |field| {
        const metric_tag = std.meta.stringToEnum(EventMetricTag, field.name).?;
        try output.print("gauge: {s}(", .{field.name});
        if (field.type != void) {
            inline for (std.meta.fields(field.type), 0..) |data_field, i| {
                if (i != 0) try output.print(", ");
                try output.print("{s}", .{data_field.name});
            }
        }
        const metric_stats = EventMetric.slot_limits.get(metric_tag) * stats_per_gauge;
        try output.print(")={}\n", .{metric_stats});
        stats_total += metric_stats;
    }
    inline for (std.meta.fields(EventTiming)) |field| {
        const timing_tag = std.meta.stringToEnum(EventTimingTag, field.name).?;
        try output.print("timing: {s}(", .{field.name});
        if (field.type != void) {
            inline for (std.meta.fields(field.type), 0..) |data_field, i| {
                if (i != 0) try output.print(", ");
                try output.print("{s}", .{data_field.name});
            }
        }
        const timing_stats = EventTiming.slot_limits.get(timing_tag) * stats_per_timing;
        try output.print(")={}\n", .{timing_stats});
        stats_total += timing_stats;
    }
    log.info("Total stats per replica: {}", .{stats_total});
    log.info(
        "(All stats are tagged with the replica, so the cluster has 6x as many stats.)",
        .{},
    );
}

// Example output:
// checkpoint          op                  trigger             prepare_max         checkpoint_next
// 624894719      +20  624894739      +12  624894751      +16  624894767      +912 624895679
fn inspect_op(output: std.io.AnyWriter, op: u64) !void {
    const checkpoint = if (op < constants.vsr_checkpoint_ops - 1) 0 else checkpoint: {
        // op = q * checkpoints_ops - 1 + r
        const r = (op + 1) % constants.vsr_checkpoint_ops;
        const q = @divExact(op + 1 - r, constants.vsr_checkpoint_ops);
        break :checkpoint q * constants.vsr_checkpoint_ops - 1;
    };
    const checkpoint_next = vsr.Checkpoint.checkpoint_after(checkpoint);

    const points = .{
        .checkpoint = checkpoint,
        .trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint) orelse 0,
        .prepare_max = vsr.Checkpoint.prepare_max_for_checkpoint(checkpoint) orelse 0,
        .checkpoint_next = checkpoint_next,
        .op = op,
    };
    const Points = @TypeOf(points);

    const Entry = struct {
        label: []const u8,
        op: u64,
        fn less_than(_: void, a: @This(), b: @This()) bool {
            return a.op < b.op;
        }
    };

    var entries: [std.meta.fields(Points).len]Entry = undefined;
    inline for (std.meta.fields(Points), 0..) |field, index| {
        entries[index] = .{
            .label = field.name,
            .op = @field(points, field.name),
        };
    }
    std.sort.insertion(Entry, &entries, {}, Entry.less_than);
    for (entries) |entry| {
        try output.print("{s: <20}", .{entry.label});
    }
    try output.print("\n", .{});
    for (entries[0 .. entries.len - 1], entries[1..]) |entry, entry_next| {
        try output.print("{d: <15}", .{entry.op});
        try output.print("+{d: <4}", .{entry_next.op - entry.op});
    }
    try output.print("{d: <20}", .{entries[entries.len - 1].op});
    try output.print("\n", .{});
}

fn print_header(output: std.io.AnyWriter, comptime level: u8, comptime header: []const u8) !void {
    const width_total = 32;
    const pad_left = "  " ** level;
    const pad_right = " " ** (width_total -| level * 2 -| header.len);
    try output.print(pad_left ++ header ++ pad_right, .{});
}

fn print_size_count(output: std.io.AnyWriter, comptime size: u64, comptime count: u64) !void {
    if (count == 1) {
        try output.print("{}\n", .{stdx.fmt_int_size_bin_exact(size)});
    } else {
        const size_formatted = comptime if (size < 1024)
            std.fmt.comptimePrint("{}B", .{size})
        else
            std.fmt.comptimePrint("{}", .{stdx.fmt_int_size_bin_exact(size)});
        try output.print("{s<8} x{}\n", .{ size_formatted, count });
    }
}

fn print_objects(output: std.io.AnyWriter) !void {
    const Grooves = StateMachine.Forest.Grooves;
    inline for (std.meta.fields(Grooves)) |groove_field| {
        const Groove = groove_field.type;
        const ObjectTree = Groove.ObjectTree;

        comptime var size_total: usize = 0;

        const object_size = @sizeOf(ObjectTree.Table.Value);
        size_total += object_size;

        const id_size = if (Groove.IdTree == void) 0 else @sizeOf(Groove.IdTree.Table.Value);
        size_total += id_size;

        comptime {
            for (std.meta.fields(Groove.IndexTrees)) |index_field| {
                const IndexTree = index_field.type;
                const index_size = @sizeOf(IndexTree.Table.Value);
                size_total += index_size;
            }
        }

        try print_header(output, 0, ObjectTree.tree_name());
        try print_size_count(output, size_total, 1);

        try print_header(output, 1, "object");
        try print_size_count(output, object_size, 1);

        try print_header(output, 1, "id");
        try print_size_count(output, id_size, 1);

        inline for (std.meta.fields(Groove.IndexTrees)) |index_field| {
            const IndexTree = index_field.type;
            const index_size = @sizeOf(IndexTree.Table.Value);

            try print_header(output, 1, index_field.name);
            try print_size_count(output, index_size, 1);
        }

        try output.print("\n", .{});
    }
}

const Inspector = struct {
    allocator: std.mem.Allocator,
    io: *IO,
    storage: Storage,

    superblock_buffer: []align(constants.sector_size) u8,
    superblock_headers: [constants.superblock_copies]*const SuperBlockHeader,

    busy: bool = false,
    read: Storage.Read = undefined,

    fn create(
        allocator: std.mem.Allocator,
        io: *IO,
        tracer: *Tracer,
        path: []const u8,
    ) !*Inspector {
        var inspector = try allocator.create(Inspector);
        errdefer allocator.destroy(inspector);

        inspector.* = .{
            .allocator = allocator,
            .io = io,
            .storage = undefined,
            .superblock_buffer = undefined,
            .superblock_headers = undefined,
        };

        inspector.storage = try Storage.init(io, tracer, .{
            .path = path,
            .size_min = vsr.superblock.data_file_size_min,
            .purpose = .inspect,
            .direct_io = .direct_io_optional,
        });
        errdefer inspector.storage.deinit();

        inspector.superblock_buffer = try allocator.alignedAlloc(
            u8,
            constants.sector_size,
            vsr.superblock.superblock_zone_size,
        );
        errdefer allocator.free(inspector.superblock_buffer);

        try inspector.read_buffer(inspector.superblock_buffer, .superblock, 0);

        for (&inspector.superblock_headers, 0..) |*superblock_header, copy| {
            const offset = @as(u64, copy) * vsr.superblock.superblock_copy_size;
            superblock_header.* = @alignCast(std.mem.bytesAsValue(
                SuperBlockHeader,
                inspector.superblock_buffer[offset..][0..@sizeOf(SuperBlockHeader)],
            ));
        }

        const superblock = try inspector.read_superblock(null);
        if (superblock.version != SuperBlockVersion) {
            return vsr.fatal(
                .cli,
                "invalid superblock version; inspector supports version={}, version in {s}={}",
                .{
                    SuperBlockVersion,
                    path,
                    superblock.version,
                },
            );
        }
        return inspector;
    }

    fn destroy(inspector: *Inspector) void {
        inspector.allocator.free(inspector.superblock_buffer);
        inspector.storage.deinit();
        inspector.allocator.destroy(inspector);
    }

    fn work(inspector: *Inspector) !void {
        assert(!inspector.busy);
        inspector.busy = true;

        while (inspector.busy) {
            try inspector.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
        }
    }

    fn inspector_read_callback(read: *Storage.Read) void {
        const inspector: *Inspector = @alignCast(@fieldParentPtr("read", read));
        assert(inspector.busy);

        inspector.busy = false;
    }

    fn inspect_superblock(inspector: *Inspector, output: std.io.AnyWriter) !void {
        log.info("In the left column of the output, \"|\" denotes which copies have a " ++
            "particular value.", .{});
        log.info("\"||||\" means that all four superblock copies are in agreement.", .{});
        log.info("\"|_|_\" means that the value matches in copies 0/2, but differs from copies " ++
            "1/3.", .{});

        var header_valid: [constants.superblock_copies]bool = undefined;
        for (&inspector.superblock_headers, 0..) |header, i| {
            header_valid[i] = header.valid_checksum();
        }

        inline for (std.meta.fields(SuperBlockHeader)) |field| {
            var group_by = GroupByType(constants.superblock_copies){};
            for (inspector.superblock_headers) |header| {
                group_by.compare(std.mem.asBytes(&@field(header, field.name)));
            }

            var label_buffer: [128]u8 = undefined;
            for (group_by.groups()) |group| {
                const header_index = group.first_set().?;
                const header = &inspector.superblock_headers[header_index];
                const header_mark: u8 = if (header_valid[header_index]) '|' else 'X';

                var label_stream = std.io.fixedBufferStream(&label_buffer);
                for (0..constants.superblock_copies) |j| {
                    try label_stream.writer().writeByte(if (group.is_set(j)) header_mark else '_');
                }
                try label_stream.writer().writeByte(' ');
                try label_stream.writer().writeAll(field.name);

                try print_struct(output, label_stream.getWritten(), &@field(header.*, field.name));
            }
        }
    }

    fn inspect_wal(inspector: *Inspector, output: std.io.AnyWriter) !void {
        log.info("In the left column of the output, \"|\" denotes which set of headers has " ++
            "each value.", .{});
        log.info("\"||\" denotes that the prepare and the redundant header match.", .{});
        log.info("\"|_\" is the redundant header.", .{});
        log.info("\"_|\" is the prepare's header.", .{});

        const headers_buffer = try inspector.allocator.alignedAlloc(
            u8,
            constants.sector_size,
            constants.journal_size_headers,
        );
        defer inspector.allocator.free(headers_buffer);

        const prepare_buffer = try inspector.allocator.alignedAlloc(
            u8,
            constants.sector_size,
            constants.message_size_max,
        );
        defer inspector.allocator.free(prepare_buffer);

        try inspector.read_buffer(headers_buffer, .wal_headers, 0);

        for (std.mem.bytesAsSlice(vsr.Header.Prepare, headers_buffer), 0..) |*wal_header, slot| {
            const offset = slot * constants.message_size_max;
            try inspector.read_buffer(prepare_buffer, .wal_prepares, offset);

            const wal_prepare = std.mem.bytesAsValue(
                vsr.Header.Prepare,
                prepare_buffer[0..@sizeOf(vsr.Header)],
            );

            const wal_prepare_body_valid =
                wal_prepare.valid_checksum() and
                wal_prepare.valid_checksum_body(
                    prepare_buffer[@sizeOf(vsr.Header)..wal_prepare.size],
                );

            const header_pair = [_]*const vsr.Header.Prepare{ wal_header, wal_prepare };

            var group_by = GroupByType(2){};
            group_by.compare(std.mem.asBytes(wal_header));
            group_by.compare(std.mem.asBytes(wal_prepare));

            var label_buffer: [64]u8 = undefined;
            for (group_by.groups()) |group| {
                const header = header_pair[group.first_set().?];
                const header_valid = header.valid_checksum() and
                    (!group.is_set(1) or wal_prepare_body_valid);

                const mark: u8 = if (header_valid) '|' else 'X';
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().writeByte(if (group.is_set(0)) mark else '_');
                try label_stream.writer().writeByte(if (group.is_set(1)) mark else '_');
                try label_stream.writer().print("{:_>4}: ", .{slot});

                try print_struct(output, label_stream.getWritten(), &.{
                    "checksum=",  header.checksum,
                    "release=",   header.release,
                    "view=",      header.view,
                    "op=",        header.op,
                    "size=",      header.size,
                    "operation=", header.operation,
                });
            }
        }
    }

    fn inspect_wal_slot(inspector: *Inspector, output: std.io.AnyWriter, slot: usize) !void {
        assert(slot <= constants.journal_slot_count);

        const headers_buffer = try inspector.allocator.alignedAlloc(
            u8,
            constants.sector_size,
            constants.journal_size_headers,
        );
        defer inspector.allocator.free(headers_buffer);

        const prepare_buffer = try inspector.allocator.alignedAlloc(
            u8,
            constants.sector_size,
            constants.message_size_max,
        );
        defer inspector.allocator.free(prepare_buffer);

        try inspector.read_buffer(headers_buffer, .wal_headers, 0);
        try inspector.read_buffer(prepare_buffer, .wal_prepares, slot * constants.message_size_max);

        const headers = std.mem.bytesAsSlice(vsr.Header.Prepare, headers_buffer);
        const prepare_header =
            std.mem.bytesAsValue(vsr.Header.Prepare, prepare_buffer[0..@sizeOf(vsr.Header)]);

        const prepare_body_valid =
            prepare_header.valid_checksum() and
            prepare_header.valid_checksum_body(
                prepare_buffer[@sizeOf(vsr.Header)..prepare_header.size],
            );

        const copies: [2]*const vsr.Header.Prepare = .{ &headers[slot], prepare_header };

        var group_by = GroupByType(2){};
        for (copies) |h| group_by.compare(std.mem.asBytes(h));

        var label_buffer: [2]u8 = undefined;
        for (group_by.groups()) |group| {
            const header = copies[group.first_set().?];
            const header_mark: u8 = if (header.valid_checksum()) '|' else 'X';
            label_buffer[0] = if (group.is_set(0)) header_mark else '_';
            label_buffer[1] = if (group.is_set(1)) header_mark else '_';

            try print_struct(output, &label_buffer, header);
        }
        try print_prepare_body(output, prepare_buffer);

        if (!prepare_body_valid) {
            try output.writeAll("error: invalid prepare body!\n");
        }
    }

    fn inspect_replies(
        inspector: *Inspector,
        output: std.io.AnyWriter,
        superblock_copy: ?u8,
    ) !void {
        const entries_block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(entries_block);

        const reply_sector =
            try inspector.allocator.alignedAlloc(u8, constants.sector_size, constants.sector_size);
        defer inspector.allocator.free(reply_sector);

        const entries =
            try inspector.read_client_sessions(entries_block, superblock_copy) orelse return;

        var label_buffer: [64]u8 = undefined;
        for (&entries.headers, &entries.sessions, 0..) |*session_header, session, slot| {
            try inspector.read_buffer(
                reply_sector,
                .client_replies,
                constants.message_size_max * slot,
            );

            const reply_header =
                std.mem.bytesAsValue(vsr.Header.Reply, reply_sector[0..@sizeOf(vsr.Header)]);
            const copies: [2]*const vsr.Header.Reply = .{ session_header, reply_header };
            var group_by = GroupByType(2){};
            for (copies) |h| group_by.compare(std.mem.asBytes(h));

            // The session doesn't include the group diff labels since it is only stored in the
            // client sessions, not the replies.
            try output.print("{:_>2}     session={}\n", .{ slot, session });

            for (group_by.groups()) |group| {
                const header_index = group.first_set().?;
                const header = copies[header_index];
                const header_mark: u8 = if (header.valid_checksum()) '|' else 'X';

                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().print("{:_>2}: ", .{slot});
                try label_stream.writer().writeByte(if (group.is_set(0)) header_mark else '_');
                try label_stream.writer().writeByte(if (group.is_set(1)) header_mark else '_');
                try label_stream.writer().writeAll(" header");
                try print_struct(output, label_stream.getWritten(), header);
            }
        }
    }

    fn inspect_replies_slot(
        inspector: *Inspector,
        output: std.io.AnyWriter,
        superblock_copy: ?u8,
        slot: usize,
    ) !void {
        assert(slot < constants.clients_max);

        const block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(block);

        const reply = try inspector.allocator.alignedAlloc(
            u8,
            constants.sector_size,
            constants.message_size_max,
        );
        defer inspector.allocator.free(reply);

        log.info("\"||\" denotes that the client session header and reply header match.", .{});
        log.info("\"|_\" is the client session header.", .{});
        log.info("\"_|\" is the client reply's header.", .{});

        const entries = try inspector.read_client_sessions(block, superblock_copy) orelse {
            try output.writeAll("error: no client sessions\n");
            return;
        };

        try inspector.read_buffer(
            reply,
            .client_replies,
            constants.message_size_max * slot,
        );

        const reply_header = std.mem.bytesAsValue(vsr.Header.Reply, reply[0..@sizeOf(vsr.Header)]);
        const copies: [2]*const vsr.Header.Reply = .{ &entries.headers[slot], reply_header };
        var group_by = GroupByType(2){};
        for (copies) |h| group_by.compare(std.mem.asBytes(h));

        var label_buffer: [2]u8 = undefined;
        for (group_by.groups()) |group| {
            const header = copies[group.first_set().?];
            const header_mark: u8 = if (header.valid_checksum()) '|' else 'X';
            label_buffer[0] = if (group.is_set(0)) header_mark else '_';
            label_buffer[1] = if (group.is_set(1)) header_mark else '_';

            try print_struct(output, &label_buffer, header);
        }
        try print_reply_body(output, reply);
    }

    fn inspect_grid(inspector: *Inspector, output: std.io.AnyWriter, superblock_copy: ?u8) !void {
        const superblock = try inspector.read_superblock(superblock_copy);

        const free_set_blocks_acquired_size =
            superblock.vsr_state.checkpoint.free_set_blocks_acquired_size;
        const free_set_blocks_released_size =
            superblock.vsr_state.checkpoint.free_set_blocks_released_size;

        const free_set_blocks_acquired_buffer =
            try inspector.allocator.alignedAlloc(
                u8,
                @alignOf(vsr.FreeSet.Word),
                free_set_blocks_acquired_size,
            );
        defer inspector.allocator.free(free_set_blocks_acquired_buffer);

        var free_set_blocks_acquired_addresses =
            try std.ArrayList(u64).initCapacity(
                inspector.allocator,
                stdx.div_ceil(
                    free_set_blocks_acquired_size,
                    constants.block_size - @sizeOf(vsr.Header),
                ),
            );
        defer free_set_blocks_acquired_addresses.deinit();

        const free_set_blocks_released_buffer =
            try inspector.allocator.alignedAlloc(
                u8,
                @alignOf(vsr.FreeSet.Word),
                free_set_blocks_released_size,
            );
        defer inspector.allocator.free(free_set_blocks_released_buffer);

        var free_set_blocks_released_addresses =
            try std.ArrayList(u64).initCapacity(
                inspector.allocator,
                stdx.div_ceil(
                    free_set_blocks_released_size,
                    constants.block_size - @sizeOf(vsr.Header),
                ),
            );
        defer free_set_blocks_released_addresses.deinit();

        try inspector.read_free_set_bitset(
            output,
            superblock,
            .blocks_acquired,
            free_set_blocks_acquired_buffer,
            &free_set_blocks_acquired_addresses,
        );
        try inspector.read_free_set_bitset(
            output,
            superblock,
            .blocks_released,
            free_set_blocks_released_buffer,
            &free_set_blocks_released_addresses,
        );

        // This is not exact, but is an overestimate:
        const grid_blocks_max =
            @divFloor(constants.storage_size_limit_max, constants.block_size);

        var free_set = try vsr.FreeSet.init(
            inspector.allocator,
            .{
                .grid_size_limit = grid_blocks_max * constants.block_size,
                .blocks_released_prior_checkpoint_durability_max = 0,
            },
        );
        defer free_set.deinit(inspector.allocator);

        const SliceOfAlignedWordSlice = []const []align(@alignOf(vsr.FreeSet.Word)) const u8;
        const encoded_free_set_blocks_acquired: SliceOfAlignedWordSlice =
            if (free_set_blocks_acquired_buffer.len != 0)
                &.{free_set_blocks_acquired_buffer}
            else
                &.{};
        const encoded_free_set_blocks_released: SliceOfAlignedWordSlice =
            if (free_set_blocks_released_buffer.len != 0)
                &.{free_set_blocks_released_buffer}
            else
                &.{};

        free_set.open(.{
            .encoded = .{
                .blocks_acquired = encoded_free_set_blocks_acquired,
                .blocks_released = encoded_free_set_blocks_released,
            },
            .free_set_block_addresses = .{
                .blocks_acquired = free_set_blocks_acquired_addresses.items,
                .blocks_released = free_set_blocks_released_addresses.items,
            },
        });

        const free_set_acquired_address_max = free_set.highest_address_acquired() orelse 0;
        const free_set_blocks_acquired_compression_ratio =
            @as(f64, @floatFromInt(stdx.div_ceil(free_set_acquired_address_max, 8))) /
            @as(f64, @floatFromInt(superblock.vsr_state.checkpoint.free_set_blocks_acquired_size));

        const free_set_released_address_max = free_set.highest_address_released() orelse 0;
        const free_set_blocks_released_compression_ratio =
            @as(f64, @floatFromInt(stdx.div_ceil(free_set_released_address_max, 8))) /
            @as(f64, @floatFromInt(superblock.vsr_state.checkpoint.free_set_blocks_released_size));

        try output.print(
            \\free_set.blocks_free={}
            \\free_set.blocks_acquired={}
            \\free_set.blocks_released={}
            \\free_set.highest_address_acquired={?}
            \\free_set.acquired_size={}
            \\free_set.acquired_compression_ratio={d:0.4}
            \\free_set.highest_address_released={?}
            \\free_set.released_size={}
            \\free_set.released_compression_ratio={d:0.4}
            \\
        ,
            .{
                free_set.count_free(),
                free_set.count_acquired(),
                free_set.count_released(),
                free_set.highest_address_acquired(),
                std.fmt.fmtIntSizeBin(superblock.vsr_state.checkpoint
                    .free_set_blocks_acquired_size),
                free_set_blocks_acquired_compression_ratio,
                free_set.highest_address_released(),
                std.fmt.fmtIntSizeBin(superblock.vsr_state.checkpoint
                    .free_set_blocks_released_size),
                free_set_blocks_released_compression_ratio,
            },
        );
    }

    fn inspect_grid_block(inspector: *Inspector, output: std.io.AnyWriter, address: u64) !void {
        const block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(block);

        try inspector.read_block(block, address, null);

        // If this is an unexpected (but valid) block, log an error but keep going.
        const header = schema.header_from_block(block);
        if (header.address != address) log.err("misdirected block", .{});

        try print_block(output, block);
    }

    fn inspect_manifest(
        inspector: *Inspector,
        output: std.io.AnyWriter,
        superblock_copy: ?u8,
    ) !void {
        const superblock = try inspector.read_superblock(superblock_copy);

        const block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(block);

        var manifest_block_address = superblock.vsr_state.checkpoint.manifest_newest_address;
        var manifest_block_checksum = superblock.vsr_state.checkpoint.manifest_newest_checksum;
        for (0..superblock.vsr_state.checkpoint.manifest_block_count) |i| {
            try output.print(
                "manifest_log.blocks[{}]: address={} checksum={x:0>32} ",
                .{ i, manifest_block_address, manifest_block_checksum },
            );

            inspector.read_block(
                block,
                manifest_block_address,
                manifest_block_checksum,
            ) catch {
                try output.writeAll("error: manifest block not found\n");
                break;
            };

            var entry_counts = std.enums.EnumArray(
                schema.ManifestNode.Event,
                [constants.lsm_levels]usize,
            ).initDefault([_]usize{0} ** constants.lsm_levels, .{});

            const manifest_node = schema.ManifestNode.from(block);
            for (manifest_node.tables_const(block)) |*table_info| {
                entry_counts.getPtr(table_info.label.event)[table_info.label.level] += 1;
            }

            try output.print(
                "entries={}/{}",
                .{ manifest_node.entry_count, schema.ManifestNode.entry_count_max },
            );

            for (std.enums.values(schema.ManifestNode.Event)) |event| {
                if (event == .reserved) continue;
                try output.print(" {s}=", .{@tagName(event)});
                for (0..constants.lsm_levels) |level| {
                    if (level != 0) try output.writeAll(",");
                    try output.print("{}", .{entry_counts.get(event)[level]});
                }
            }
            try output.writeAll("\n");

            const manifest_metadata = schema.ManifestNode.metadata(block);
            manifest_block_address = manifest_metadata.previous_manifest_block_address;
            manifest_block_checksum = manifest_metadata.previous_manifest_block_checksum;
        }
    }

    fn inspect_tables(
        inspector: *Inspector,
        output: std.io.AnyWriter,
        superblock_copy: ?u8,
        filter: struct { tree_id: u16, level: ?u6 },
    ) !void {
        var tables_latest =
            std.AutoHashMap(u128, ?schema.ManifestNode.TableInfo).init(inspector.allocator);
        defer tables_latest.deinit();

        const block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(block);

        // Construct a set of all active tables.
        const superblock = try inspector.read_superblock(superblock_copy);
        var manifest_block_address = superblock.vsr_state.checkpoint.manifest_newest_address;
        var manifest_block_checksum = superblock.vsr_state.checkpoint.manifest_newest_checksum;
        for (0..superblock.vsr_state.checkpoint.manifest_block_count) |_| {
            try inspector.read_block(block, manifest_block_address, manifest_block_checksum);

            const manifest_node = schema.ManifestNode.from(block);
            const tables = manifest_node.tables_const(block);
            for (0..tables.len) |i| {
                const table_info = &tables[tables.len - i - 1];
                const table_latest = try tables_latest.getOrPut(table_info.checksum);
                if (!table_latest.found_existing) {
                    if (table_info.label.event == .remove) {
                        table_latest.value_ptr.* = null;
                    } else {
                        table_latest.value_ptr.* = table_info.*;
                    }
                }
            }

            const manifest_metadata = schema.ManifestNode.metadata(block);
            manifest_block_address = manifest_metadata.previous_manifest_block_address;
            manifest_block_checksum = manifest_metadata.previous_manifest_block_checksum;
        }

        var tables_filtered =
            std.ArrayList(schema.ManifestNode.TableInfo).init(inspector.allocator);
        defer tables_filtered.deinit();

        // Construct a list of only the tables matching the `filter`.
        var tables_latest_iterator = tables_latest.iterator();
        while (tables_latest_iterator.next()) |table_or_null| {
            const table = table_or_null.value_ptr.* orelse continue;
            if (table.tree_id != filter.tree_id) continue;
            if (filter.level) |level| {
                if (table.label.level != level) continue;
            }
            try tables_filtered.append(table);
        }

        // Order the tables in a predictable way, since the manifest log can shuffle them around.
        std.mem.sortUnstable(schema.ManifestNode.TableInfo, tables_filtered.items, {}, struct {
            fn less_than(
                _: void,
                table_a: schema.ManifestNode.TableInfo,
                table_b: schema.ManifestNode.TableInfo,
            ) bool {
                for ([_]std.math.Order{
                    std.math.order(table_a.tree_id, table_b.tree_id),
                    std.math.order(table_a.label.level, table_b.label.level),
                    std.math.order(
                        std.mem.bytesAsValue(u256, &table_a.key_min).*,
                        std.mem.bytesAsValue(u256, &table_b.key_min).*,
                    ),
                    std.math.order(
                        std.mem.bytesAsValue(u256, &table_a.key_max).*,
                        std.mem.bytesAsValue(u256, &table_b.key_max).*,
                    ),
                    std.math.order(table_a.snapshot_min, table_b.snapshot_min),
                    std.math.order(table_a.snapshot_max, table_b.snapshot_max),
                    std.math.order(table_a.checksum, table_b.checksum),
                }) |order| {
                    if (order != .eq) return order == .lt;
                }
                // This *should* be unreachable, especially given the checksum comparison.
                return false;
            }
        }.less_than);

        inline for (StateMachine.Forest.tree_infos) |tree_info| {
            if (tree_info.tree_id == filter.tree_id) {
                for (tables_filtered.items) |*table| {
                    try print_table_info(output, tree_info, table);
                }
                break;
            }
        } else {
            try output.print("error: unknown tree_id={}\n", .{filter.tree_id});
        }
    }

    fn read_buffer(
        inspector: *Inspector,
        buffer: []align(constants.sector_size) u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) !void {
        inspector.storage.read_sectors(
            inspector_read_callback,
            &inspector.read,
            buffer,
            zone,
            offset_in_zone,
        );
        try inspector.work();
    }

    fn read_superblock(inspector: *const Inspector, superblock_copy: ?u8) !*const SuperBlockHeader {
        if (superblock_copy) |copy| {
            return inspector.superblock_headers[copy];
        } else {
            var copies: [constants.superblock_copies]SuperBlockHeader = undefined;
            for (&copies, inspector.superblock_headers) |*copy, header| copy.* = header.*;

            var quorums = SuperBlockQuorums{};
            const quorum = try quorums.working(&copies, .open);
            if (!quorum.valid) return error.SuperBlockQuorumInvalid;
            return inspector.superblock_headers[quorum.copies.first_set().?];
        }
    }

    fn read_block(
        inspector: *Inspector,
        buffer: BlockPtr,
        address: u64,
        checksum: ?u128,
    ) !void {
        try inspector.read_buffer(buffer, .grid, (address - 1) * constants.block_size);

        const header = std.mem.bytesAsValue(vsr.Header.Block, buffer[0..@sizeOf(vsr.Header)]);
        if (!header.valid_checksum()) {
            log.err(
                "read_block: invalid block address={} checksum_expect={?x:0>32} " ++
                    "checksum_actual={x:0>32} (bad checksum)",
                .{ address, checksum, header.checksum },
            );
            return error.InvalidChecksum;
        }

        if (!header.valid_checksum_body(buffer[@sizeOf(vsr.Header)..header.size])) {
            log.err(
                "read_block: invalid block address={} checksum_expect={?x:0>32} " ++
                    "checksum_actual={x:0>32} (bad checksum_body)",
                .{ address, checksum, header.checksum },
            );
            return error.InvalidChecksumBody;
        }

        if (checksum) |checksum_| {
            if (header.checksum != checksum_) {
                log.err(
                    "read_block: invalid block address={} checksum_expect={?x:0>32} " ++
                        "checksum_actual={x:0>32} (wrong block)",
                    .{ address, checksum, header.checksum },
                );
                return error.WrongBlock;
            }
        }
    }

    fn read_free_set_bitset(
        inspector: *Inspector,
        output: std.io.AnyWriter,
        superblock: *const SuperBlockHeader,
        bitset: vsr.FreeSet.BitsetKind,
        free_set_buffer: []align(@alignOf(vsr.FreeSet.Word)) u8,
        free_set_addresses: *std.ArrayList(u64),
    ) !void {
        const block = try allocate_block(inspector.allocator);
        defer inspector.allocator.free(block);

        const free_set_reference = superblock.free_set_reference(bitset);
        const free_set_size = free_set_reference.trailer_size;
        const free_set_checksum = free_set_reference.checksum;

        const free_set_block_count = stdx.div_ceil(
            free_set_size,
            constants.block_size - @sizeOf(vsr.Header),
        );

        var free_set_block_references = try std.ArrayList(vsr.BlockReference).initCapacity(
            inspector.allocator,
            free_set_block_count,
        );
        defer free_set_block_references.deinit();

        if (free_set_size > 0) {
            // Read free set from the grid by manually following the linked list of blocks.
            // Note that free set is written in direct order, and must be read backwards.
            var free_set_block_reference: ?vsr.BlockReference = .{
                .address = free_set_reference.last_block_address,
                .checksum = free_set_reference.last_block_checksum,
            };

            var free_set_cursor: usize = free_set_size;
            while (free_set_block_reference) |block_reference| {
                try inspector.read_block(
                    block,
                    block_reference.address,
                    block_reference.checksum,
                );

                assert(schema.header_from_block(block).checksum == block_reference.checksum);

                const encoded_words = schema.TrailerNode.body(block);
                free_set_cursor -= encoded_words.len;
                stdx.copy_disjoint(
                    .inexact,
                    u8,
                    free_set_buffer[free_set_cursor..],
                    encoded_words,
                );
                free_set_block_references.appendAssumeCapacity(block_reference);
                free_set_addresses.appendAssumeCapacity(block_reference.address);
                free_set_block_reference = schema.TrailerNode.previous(block);
            }
            assert(free_set_block_reference == null);
            assert(free_set_cursor == 0);
        } else {
            assert(free_set_reference.last_block_address == 0);
            assert(free_set_reference.last_block_checksum == 0);
        }

        assert(free_set_block_references.items.len == free_set_block_count);
        assert(free_set_addresses.items.len == free_set_block_count);
        assert(vsr.checksum(free_set_buffer[0..free_set_size]) == free_set_checksum);

        for (free_set_block_references.items, 0..) |reference, i| {
            try output.print(
                "free_set_trailer.blocks[{}]: address={} checksum={x:0>32}\n",
                .{ i, reference.address, reference.checksum },
            );
        }
    }

    const ClientSessions = extern struct {
        headers: [constants.clients_max]vsr.Header.Reply,
        sessions: [constants.clients_max]u64,
    };

    fn read_client_sessions(
        inspector: *Inspector,
        block: BlockPtr,
        superblock_copy: ?u8,
    ) !?*ClientSessions {
        const superblock = try inspector.read_superblock(superblock_copy);

        if (superblock.vsr_state.checkpoint.client_sessions_size == 0) {
            assert(superblock.vsr_state.checkpoint.client_sessions_last_block_address == 0);
            assert(superblock.vsr_state.checkpoint.client_sessions_last_block_checksum == 0);
            return null;
        }
        assert(superblock.vsr_state.checkpoint.client_sessions_size == @sizeOf(ClientSessions));

        try inspector.read_block(
            block,
            superblock.vsr_state.checkpoint.client_sessions_last_block_address,
            superblock.vsr_state.checkpoint.client_sessions_last_block_checksum,
        );

        const block_header = schema.header_from_block(block);
        assert(block_header.size == @sizeOf(vsr.Header) + @sizeOf(ClientSessions));
        assert(vsr.checksum(block[@sizeOf(vsr.Header)..block_header.size]) ==
            superblock.vsr_state.checkpoint.client_sessions_checksum);

        return std.mem.bytesAsValue(
            ClientSessions,
            block[@sizeOf(vsr.Header)..][0..@sizeOf(ClientSessions)],
        );
    }
};

fn print_struct(
    output: std.io.AnyWriter,
    label: []const u8,
    value: anytype,
) !void {
    comptime assert(@typeInfo(@TypeOf(value)) == .pointer);
    comptime assert(@typeInfo(@TypeOf(value)).pointer.size == .one);

    const Type = @typeInfo(@TypeOf(value)).pointer.child;
    // Print structs *without* a custom format() function.
    if (comptime @typeInfo(Type) == .@"struct" and !std.meta.hasFn(Type, "format")) {
        if (@typeInfo(Type).@"struct".is_tuple) {
            try output.writeAll(label);
            // Print tuples as a single line.
            inline for (std.meta.fields(Type), 0..) |field, i| {
                if (@typeInfo(field.type) == .pointer and
                    @typeInfo(@typeInfo(field.type).pointer.child) == .array)
                {
                    // Allow inline labels.
                    try output.writeAll(@field(value, field.name));
                } else {
                    try print_value(output, @field(value, field.name));
                    if (i != std.meta.fields(Type).len) try output.writeAll(" ");
                }
            }
            try output.writeAll("\n");
            return;
        } else {
            var label_buffer: [1024]u8 = undefined;
            inline for (std.meta.fields(Type)) |field| {
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().print("{s}.{s}", .{ label, field.name });
                try print_struct(output, label_stream.getWritten(), &@field(value, field.name));
            }
            return;
        }
    }

    if (Element: {
        const type_info = @typeInfo(Type);
        if (type_info == .array) {
            break :Element @as(?type, type_info.array.child);
        }
        break :Element null;
    }) |Element| {
        if (Element == u8) {
            if (stdx.zeroed(value)) {
                return output.print("{s}=[{}]u8{{0}}\n", .{ label, value.len });
            } else {
                return output.print("{s}=[{}]u8{{nonzero}}\n", .{ label, value.len });
            }
        } else {
            var label_buffer: [1024]u8 = undefined;
            for (value[0..], 0..) |*item, index| {
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().print("{s}[{}]", .{ label, index });
                try print_struct(output, label_stream.getWritten(), item);
            }
            return;
        }
    }

    try output.print("{s}=", .{label});
    try print_value(output, value.*);
    try output.writeAll("\n");
}

fn print_value(output: std.io.AnyWriter, value: anytype) !void {
    const Type = @TypeOf(value);
    if (@typeInfo(Type) == .@"struct") assert(std.meta.hasFn(Type, "format"));
    assert(@typeInfo(Type) != .array);

    if (Type == u128) return output.print("0x{x:0>32}", .{value});

    if (Type == vsr.Operation) {
        if (value.valid(StateMachine.Operation)) {
            return output.writeAll(value.tag_name(StateMachine.Operation));
        } else {
            return output.print("{}!", .{@intFromEnum(value)});
        }
    }

    if (@typeInfo(Type) == .@"enum") {
        if (std.enums.tagName(Type, value)) |value_string| {
            return output.print("{s}", .{value_string});
        } else {
            return output.print("{}!", .{@intFromEnum(value)});
        }
    }
    try output.print("{}", .{value});
}

fn print_block(writer: std.io.AnyWriter, block: BlockPtrConst) !void {
    const header = schema.header_from_block(block);
    try print_struct(writer, "header", header);

    inline for (.{
        .{ .block_type = .free_set, .Schema = schema.TrailerNode },
        .{ .block_type = .client_sessions, .Schema = schema.TrailerNode },
        .{ .block_type = .manifest, .Schema = schema.ManifestNode },
        .{ .block_type = .index, .Schema = schema.TableIndex },
        .{ .block_type = .value, .Schema = schema.TableValue },
    }) |pair| {
        if (header.block_type == pair.block_type) {
            try print_struct(writer, "header.metadata", pair.Schema.metadata(block));
            break;
        }
    } else {
        try writer.print("header.metadata: unknown block type\n", .{});
    }

    switch (header.block_type) {
        .manifest => {
            const manifest_node = schema.ManifestNode.from(block);
            for (manifest_node.tables_const(block), 0..) |*table_info, entry_index| {
                try writer.print(
                    "entry[{:_>4}]: {s} level={} address={} checksum={x:0>32} " ++
                        "tree_id={s} key={:0>64}..{:0>64} snapshot={}..{} values={}\n",
                    .{
                        entry_index,
                        @tagName(table_info.label.event),
                        table_info.label.level,
                        table_info.address,
                        table_info.checksum,
                        format_tree_id(table_info.tree_id),
                        std.fmt.fmtSliceHexLower(&table_info.key_min),
                        std.fmt.fmtSliceHexLower(&table_info.key_max),
                        table_info.snapshot_min,
                        table_info.snapshot_max,
                        table_info.value_count,
                    },
                );
            }
        },
        .index => {
            const index = schema.TableIndex.from(block);
            for (
                index.value_addresses_used(block),
                index.value_checksums_used(block),
                0..,
            ) |value_address, value_checksum, i| {
                try writer.print(
                    "value_blocks[{:_>3}]: address={} checksum={x:0>32}\n",
                    .{ i, value_address, value_checksum.value },
                );
            }
        },
        .value => {
            const value_block = schema.TableValue.from(block);
            const metadata = value_block.block_metadata(block);
            const value_bytes = value_block.block_values_used_bytes(block);

            var label_buffer: [256]u8 = undefined;
            inline for (StateMachine.Forest.tree_infos) |tree_info| {
                if (metadata.tree_id == tree_info.tree_id) {
                    for (
                        std.mem.bytesAsSlice(tree_info.Tree.Table.Value, value_bytes),
                        0..,
                    ) |*value, i| {
                        var label_stream = std.io.fixedBufferStream(&label_buffer);
                        try label_stream.writer().print("{s}[{}]", .{ tree_info.tree_name, i });
                        if (comptime is_composite_key(tree_info.Tree.Table.Value)) {
                            try label_stream.writer().writeAll(": ");
                            try print_struct(
                                writer,
                                label_stream.getWritten(),
                                &.{ value.field, value.timestamp },
                            );
                        } else {
                            try print_struct(writer, label_stream.getWritten(), value);
                        }
                    }
                    break;
                }
            } else {
                try writer.print("body: unknown tree id\n", .{});
            }
        },
        else => {
            try writer.print(
                "body: unimplemented for block_type={s}\n",
                .{@tagName(header.block_type)},
            );
        },
    }
}

fn format_tree_id(tree_id: u16) []const u8 {
    inline for (StateMachine.Forest.tree_infos) |tree_info| {
        if (tree_info.tree_id == tree_id) {
            return tree_info.tree_name;
        }
    } else {
        return "(unknown)";
    }
}

fn parse_tree_id(tree_label: []const u8) ?u16 {
    const tree_label_integer = std.fmt.parseInt(u16, tree_label, 10) catch null;
    inline for (StateMachine.Forest.tree_infos) |tree_info| {
        if (std.mem.eql(u8, tree_info.tree_name, tree_label)) {
            return tree_info.tree_id;
        }

        if (tree_label_integer) |tree_id| {
            if (tree_info.tree_id == tree_id) {
                return tree_id;
            }
        }
    }
    return null;
}

const operation_schemas = list: {
    const OperationSchema = struct {
        operation: vsr.Operation,
        Event: type,
        Result: type,
    };

    var list: []const OperationSchema = &[_]OperationSchema{};

    for (&[_]struct { vsr.Operation, type, type }{
        .{ .reserved, extern struct {}, extern struct {} },
        .{ .root, extern struct {}, extern struct {} },
        // TODO vsr.RegisterRequest once that is merged.
        .{ .register, extern struct {}, vsr.RegisterResult },
        .{ .reconfigure, vsr.ReconfigurationRequest, vsr.ReconfigurationResult },
        .{ .pulse, extern struct {}, extern struct {} },
        .{ .upgrade, vsr.UpgradeRequest, extern struct {} },
    }) |operation_schema| {
        list = list ++ [_]OperationSchema{.{
            .operation = operation_schema[0],
            .Event = operation_schema[1],
            .Result = operation_schema[2],
        }};
    }

    for (std.enums.values(StateMachine.Operation)) |operation| {
        if (operation == .pulse) continue;
        list = list ++ [_]OperationSchema{.{
            .operation = operation.to_vsr(),
            .Event = operation.EventType(),
            .Result = operation.ResultType(),
        }};
    }
    break :list list;
};

fn print_prepare_body(output: std.io.AnyWriter, prepare: []const u8) !void {
    const header = std.mem.bytesAsValue(vsr.Header.Prepare, prepare[0..@sizeOf(vsr.Header)]);
    inline for (operation_schemas) |operation_schema| {
        if (operation_schema.operation == header.operation) {
            const event_size = @sizeOf(operation_schema.Event);
            const body_size = header.size - @sizeOf(vsr.Header);
            if (body_size == 0) {
                try output.print("(no body)\n", .{});
            } else if (event_size != 0 and body_size % event_size == 0) {
                var label_buffer: [128]u8 = undefined;
                for (std.mem.bytesAsSlice(
                    operation_schema.Event,
                    prepare[@sizeOf(vsr.Header)..header.size],
                ), 0..) |*event, i| {
                    var label_stream = std.io.fixedBufferStream(&label_buffer);
                    try label_stream.writer().print("events[{}]: ", .{i});
                    try print_struct(output, label_stream.getWritten(), event);
                }
            } else {
                try output.print(
                    "error: unexpected body size={}, @sizeOf(Event)={}\n",
                    .{ header.size, event_size },
                );
            }
            return;
        }
    } else {
        try output.print("error: unimplemented operation={s}\n", .{@tagName(header.operation)});
    }
}

fn print_reply_body(output: std.io.AnyWriter, reply: []const u8) !void {
    const header = std.mem.bytesAsValue(vsr.Header.Reply, reply[0..@sizeOf(vsr.Header)]);
    inline for (operation_schemas) |operation_schema| {
        if (operation_schema.operation == header.operation) {
            const result_size = @sizeOf(operation_schema.Result);
            const body_size = header.size - @sizeOf(vsr.Header);
            if (body_size == 0) {
                try output.print("(no body)\n", .{});
            } else if (result_size != 0 and body_size % result_size == 0) {
                var label_buffer: [128]u8 = undefined;
                for (std.mem.bytesAsSlice(
                    operation_schema.Result,
                    reply[@sizeOf(vsr.Header)..header.size],
                ), 0..) |*result, i| {
                    var label_stream = std.io.fixedBufferStream(&label_buffer);
                    try label_stream.writer().print("results[{}]: ", .{i});
                    try print_struct(output, label_stream.getWritten(), result);
                }
            } else {
                try output.print(
                    "error: unexpected body size={}, @sizeOf(Result)={}\n",
                    .{ header.size, result_size },
                );
            }
            return;
        }
    } else {
        try output.print("error: unimplemented operation={s}\n", .{@tagName(header.operation)});
    }
}

fn print_table_info(
    output: std.io.AnyWriter,
    comptime tree_info: anytype,
    table: *const schema.ManifestNode.TableInfo,
) !void {
    try output.print("{c} T={s} L={}", .{
        @as(u8, switch (table.label.event) {
            .insert => 'I',
            .update => 'U',
            // These shouldn't be hit, but included just for completeness' sake:
            .remove => 'R',
            else => '?',
        }),
        format_tree_id(table.tree_id),
        table.label.level,
    });

    const Key = tree_info.Tree.Table.Key;
    const Value = tree_info.Tree.Table.Value;
    const key_min = std.mem.bytesAsValue(Key, table.key_min[0..@sizeOf(Key)]).*;
    const key_max = std.mem.bytesAsValue(Key, table.key_max[0..@sizeOf(Key)]).*;

    if (comptime is_composite_key(Value)) {
        const f: Value = undefined;
        const Field = @TypeOf(f.field);
        const key_min_timestamp: u64 = @truncate(key_min & std.math.maxInt(u64));
        const key_max_timestamp: u64 = @truncate(key_max & std.math.maxInt(u64));
        const key_min_field: Field = Value.key_prefix(key_min);
        const key_max_field: Field = Value.key_prefix(key_max);

        try output.print(" K={:_>6}:{}..{:_>6}:{}", .{
            key_min_field,
            key_min_timestamp,
            key_max_field,
            key_max_timestamp,
        });
    } else {
        try output.print(" K={}..{}", .{ key_min, key_max });
    }

    if (table.snapshot_max == std.math.maxInt(u64)) {
        try output.print(" S={}..max", .{table.snapshot_min});
    } else {
        try output.print(" S={}..{}", .{ table.snapshot_min, table.snapshot_max });
    }

    try output.print(" V={:_>6}/{} C={x:0>32} A={} O={}\n", .{
        table.value_count,
        tree_info.Tree.Table.value_count_max,
        table.checksum,
        table.address,
        vsr.Zone.offset(.grid, (table.address - 1) * constants.block_size),
    });
}

fn GroupByType(comptime count_max: usize) type {
    return struct {
        const GroupBy = @This();
        const BitSet = stdx.BitSetType(count_max);

        count: usize = 0,
        checksums: [count_max]?u128 = @splat(null),
        matches: [count_max]BitSet = undefined,

        pub fn compare(group_by: *GroupBy, bytes: []const u8) void {
            assert(group_by.count < count_max);
            defer group_by.count += 1;

            assert(group_by.checksums[group_by.count] == null);
            group_by.checksums[group_by.count] = vsr.checksum(bytes);
        }

        pub fn groups(group_by: *GroupBy) []const BitSet {
            assert(group_by.count == count_max);

            var distinct: usize = 0;
            for (&group_by.checksums, 0..) |checksum_a, a| {
                var matches: BitSet = .{};
                for (&group_by.checksums, 0..) |checksum_b, b| {
                    matches.set_value(b, checksum_a.? == checksum_b.?);
                }
                if (matches.first_set().? == a) {
                    group_by.matches[distinct] = matches;
                    distinct += 1;
                }
            }
            assert(distinct > 0);
            assert(distinct <= count_max);
            return group_by.matches[0..distinct];
        }
    };
}
