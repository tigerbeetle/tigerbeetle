//! Decode a TigerBeetle data file without running a replica or modifying the data file.
// TODO Client sessions, client replies
// TODO List tables (index blocks) by tree_id/level

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.inspect);

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const schema = vsr.lsm.schema;
const constants = vsr.constants;
const tb = @import("../tigerbeetle.zig");
const Storage = @import("../storage.zig").Storage;
const SuperBlockHeader = vsr.superblock.SuperBlockHeader;
const SuperBlock = vsr.SuperBlockType(Storage);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Grid = vsr.GridType(Storage);
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const is_composite_key = @import("../lsm/composite_key.zig").is_composite_key;

pub const CliArgs = union(enum) {
    superblock: struct {
        positional: struct { path: []const u8 },
    },
    wal: struct {
        slot: ?usize = null,
        positional: struct { path: []const u8 },
    },
    grid: struct {
        block: ?u64 = null,
        superblock_copy: u8 = 0,
        positional: struct { path: []const u8 },
    },
    manifest: struct {
        superblock_copy: u8 = 0,
        positional: struct { path: []const u8 },
    },
};

pub fn main(gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    var stdout_buffer = std.io.bufferedWriter(std.io.getStdOut().writer());

    const stdout = stdout_buffer.writer();

    const path = switch (cli_args) {
        inline else => |args| args.positional.path,
    };

    var inspector = try Inspector.init(gpa, path);
    defer inspector.deinit();

    switch (cli_args) {
        .superblock => try inspector.inspect_superblock(stdout),
        .wal => |args| {
            if (args.slot) |slot| {
                if (slot > constants.journal_slot_count) {
                    log.err("--slot: slot exceeds {}", .{constants.journal_slot_count - 1});
                    return error.InvalidSlot;
                }
                try inspector.inspect_wal_slot(stdout, slot);
            } else {
                try inspector.inspect_wal(stdout);
            }
        },
        .grid => |args| {
            if (args.superblock_copy >= constants.superblock_copies) {
                log.err(
                    "--superblock-copy: copy exceeds {}",
                    .{constants.superblock_copies - 1},
                );
                return error.InvalidSuperblockCopy;
            }

            if (args.block) |address| {
                try inspector.inspect_grid_block(stdout, address);
            } else {
                try inspector.inspect_grid(stdout, args.superblock_copy);
            }
        },
        .manifest => |args| {
            if (args.superblock_copy >= constants.superblock_copies) {
                log.err(
                    "--superblock-copy: copy exceeds {}",
                    .{constants.superblock_copies - 1},
                );
                return error.InvalidSuperblockCopy;
            }

            try inspector.inspect_manifest(stdout, args.superblock_copy);
        },
    }

    try stdout_buffer.flush();
}

const Inspector = struct {
    allocator: std.mem.Allocator,
    dir_fd: std.os.fd_t,
    fd: std.os.fd_t,
    io: vsr.io.IO,
    storage: Storage,

    busy: bool = false,
    read: Storage.Read = undefined,

    fn init(allocator: std.mem.Allocator, path: []const u8) !*Inspector {
        var inspector = try allocator.create(Inspector);
        errdefer allocator.destroy(inspector);

        inspector.* = .{
            .allocator = allocator,
            .dir_fd = undefined,
            .fd = undefined,
            .io = undefined,
            .storage = undefined,
        };

        const dirname = std.fs.path.dirname(path) orelse ".";
        inspector.dir_fd = try vsr.io.IO.open_dir(dirname);
        errdefer std.os.close(inspector.dir_fd);

        const basename = std.fs.path.basename(path);
        inspector.fd = try vsr.io.IO.open_file(
            inspector.dir_fd,
            basename,
            vsr.superblock.data_file_size_min,
            .open,
            .direct_io_optional,
        );
        errdefer std.os.close(inspector.fd);

        inspector.io = try vsr.io.IO.init(128, 0);
        errdefer inspector.io.deinit();

        inspector.storage = try Storage.init(&inspector.io, inspector.fd);
        errdefer inspector.storage.deinit();

        return inspector;
    }

    fn deinit(inspector: *Inspector) void {
        inspector.storage.deinit();
        std.os.close(inspector.fd);
        std.os.close(inspector.dir_fd);
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
        const inspector = @fieldParentPtr(Inspector, "read", read);
        assert(inspector.busy);

        inspector.busy = false;
    }

    fn inspect_superblock(inspector: *Inspector, output: anytype) !void {
        const buffer = try inspector.read_buffer(.superblock, 0, vsr.Zone.superblock.size().?);
        defer inspector.allocator.free(buffer);

        const copies = std.mem.bytesAsSlice(
            extern struct {
                header: SuperBlockHeader,
                padding: [vsr.superblock.superblock_copy_size - @sizeOf(SuperBlockHeader)]u8,
            },
            buffer,
        );
        assert(copies.len == constants.superblock_copies);

        var header_valid: [constants.superblock_copies]bool = undefined;
        for (copies, 0..) |*copy, i| header_valid[i] = copy.header.valid_checksum();

        inline for (std.meta.fields(SuperBlockHeader)) |field| {
            var group_by = GroupByType(constants.superblock_copies){};
            for (copies) |copy| group_by.compare(std.mem.asBytes(&@field(copy.header, field.name)));

            var label_buffer: [128]u8 = undefined;
            for (group_by.groups()) |group| {
                const header_index = group.findFirstSet().?;
                const header = copies[header_index].header;
                const header_mark: u8 = if (header_valid[header_index]) '|' else 'X';

                var label_stream = std.io.fixedBufferStream(&label_buffer);
                for (0..constants.superblock_copies) |j| {
                    try label_stream.writer().writeByte(if (group.isSet(j)) header_mark else ' ');
                }
                try label_stream.writer().writeByte(' ');
                try label_stream.writer().writeAll(field.name);

                try print_struct(output, label_stream.getWritten(), @field(header, field.name));
            }
        }
    }

    fn inspect_wal(inspector: *Inspector, output: anytype) !void {
        const headers_buffer =
            try inspector.read_buffer(.wal_headers, 0, constants.journal_size_headers);
        defer inspector.allocator.free(headers_buffer);

        for (std.mem.bytesAsSlice(vsr.Header.Prepare, headers_buffer), 0..) |*wal_header, slot| {
            const offset = slot * constants.message_size_max;
            const prepare_buffer =
                try inspector.read_buffer(.wal_prepares, offset, constants.message_size_max);
            defer inspector.allocator.free(prepare_buffer);

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
                const header = header_pair[group.findFirstSet().?];
                const header_valid = header.valid_checksum() and
                    (!group.isSet(1) or wal_prepare_body_valid);

                const mark: u8 = if (header_valid) '|' else 'X';
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().writeByte(if (group.isSet(0)) mark else ' ');
                try label_stream.writer().writeByte(if (group.isSet(1)) mark else ' ');
                try label_stream.writer().print("{:_>4}: ", .{slot});

                try print_struct(output, label_stream.getWritten(), .{
                    "checksum=",  header.checksum,
                    "release=",   header.release,
                    "view=",      header.view,
                    "op=",        header.op,
                    "operation=", header.operation,
                });
            }
        }
    }

    fn inspect_wal_slot(inspector: *Inspector, output: anytype, slot: usize) !void {
        assert(slot <= constants.journal_slot_count);

        const headers_buffer =
            try inspector.read_buffer(.wal_headers, 0, constants.journal_size_headers);
        defer inspector.allocator.free(headers_buffer);

        const prepare_buffer = try inspector.read_buffer(
            .wal_prepares,
            slot * constants.message_size_max,
            constants.message_size_max,
        );
        defer inspector.allocator.free(prepare_buffer);

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
            const header = copies[group.findFirstSet().?];
            const header_mark: u8 = if (header.valid_checksum()) '|' else 'X';
            label_buffer[0] = if (group.isSet(0)) header_mark else ' ';
            label_buffer[1] = if (group.isSet(1)) header_mark else ' ';

            try print_struct(output, &label_buffer, header.*);
        }
        try print_prepare_body(output, prepare_buffer);

        if (!prepare_body_valid) {
            try output.writeAll("error: invalid prepare body!");
        }
    }

    fn inspect_grid(inspector: *Inspector, output: anytype, superblock_copy: u8) !void {
        const superblock_buffer = try inspector.read_buffer(
            .superblock,
            @as(u64, superblock_copy) * vsr.superblock.superblock_copy_size,
            @sizeOf(SuperBlockHeader),
        );
        defer inspector.allocator.free(superblock_buffer);

        const superblock = std.mem.bytesAsValue(SuperBlockHeader, superblock_buffer);
        const free_set_size = superblock.vsr_state.checkpoint.free_set_size;
        const free_set_buffer =
            try inspector.allocator.alignedAlloc(u8, @alignOf(vsr.FreeSet.Word), free_set_size);
        defer inspector.allocator.free(free_set_buffer);

        var free_set_references = std.ArrayList(vsr.BlockReference).init(inspector.allocator);
        defer free_set_references.deinit();

        var free_set_addresses = std.ArrayList(u64).init(inspector.allocator);
        defer free_set_addresses.deinit();

        {
            var free_set_block: ?vsr.BlockReference = .{
                .address = superblock.vsr_state.checkpoint.free_set_last_block_address,
                .checksum = superblock.vsr_state.checkpoint.free_set_last_block_checksum,
            };

            var free_set_cursor: usize = free_set_size;
            while (free_set_block) |free_set_reference| {
                const block = try inspector.read_block(
                    free_set_reference.address,
                    free_set_reference.checksum,
                );
                defer inspector.allocator.free(block);

                const encoded_words = schema.TrailerNode.body(block);
                free_set_cursor -= encoded_words.len;
                stdx.copy_disjoint(.inexact, u8, free_set_buffer[free_set_cursor..], encoded_words);

                try free_set_references.append(free_set_reference);
                try free_set_addresses.append(free_set_reference.address);
                free_set_block = schema.TrailerNode.previous(block);
            }
            assert(free_set_cursor == 0);
        }

        // This is not exact, but is an overestimate:
        const free_set_blocks_max =
            @divFloor(constants.storage_size_limit_max, constants.block_size);
        var free_set = try vsr.FreeSet.init(inspector.allocator, free_set_blocks_max);
        defer free_set.deinit(inspector.allocator);
        free_set.open(.{
            .encoded = &.{free_set_buffer},
            .block_addresses = free_set_addresses.items,
        });

        const free_set_address_max = free_set.highest_address_acquired() orelse 0;
        const free_set_compression_ratio =
            @as(f64, @floatFromInt(stdx.div_ceil(free_set_address_max, 8))) /
            @as(f64, @floatFromInt(superblock.vsr_state.checkpoint.free_set_size));

        try output.print(
            \\free_set.blocks_free={}
            \\free_set.blocks_acquired={}
            \\free_set.blocks_released={}
            \\free_set.highest_address_acquired={?}
            \\free_set.size={}
            \\free_set.compression_ratio={d:0.4}
            \\
        ,
            .{
                free_set.count_free(),
                free_set.count_acquired(),
                free_set.count_released(),
                free_set.highest_address_acquired(),
                std.fmt.fmtIntSizeBin(superblock.vsr_state.checkpoint.free_set_size),
                free_set_compression_ratio,
            },
        );

        for (free_set_references.items, 0..) |reference, i| {
            try output.print(
                "free_set_trailer.blocks[{}]: address={} checksum={x:0>32}\n",
                .{ i, reference.address, reference.checksum },
            );
        }
    }

    fn inspect_grid_block(inspector: *Inspector, output: anytype, address: u64) !void {
        const block = try inspector.read_block(address, null);
        defer inspector.allocator.free(block);

        // If this is an unexpected (but valid) block, log an error but keep going.
        const header = schema.header_from_block(block);
        if (header.address != address) log.err("misdirected block", .{});

        try print_block(output, block);
    }

    fn inspect_manifest(inspector: *Inspector, output: anytype, superblock_copy: u8) !void {
        const superblock_buffer = try inspector.read_buffer(
            .superblock,
            @as(u64, superblock_copy) * vsr.superblock.superblock_copy_size,
            @sizeOf(SuperBlockHeader),
        );
        defer inspector.allocator.free(superblock_buffer);

        const superblock = std.mem.bytesAsValue(SuperBlockHeader, superblock_buffer);
        var manifest_block_address = superblock.vsr_state.checkpoint.manifest_newest_address;
        var manifest_block_checksum = superblock.vsr_state.checkpoint.manifest_newest_checksum;
        for (0..superblock.vsr_state.checkpoint.manifest_block_count) |i| {
            try output.print(
                "manifest_log.blocks[{}]: address={} checksum={x:0>32} ",
                .{ i, manifest_block_address, manifest_block_checksum },
            );

            const block = inspector.read_block(
                manifest_block_address,
                manifest_block_checksum,
            ) catch {
                try output.writeAll("(not found)");
                break;
            };
            defer inspector.allocator.free(block);

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

    fn read_buffer(
        inspector: *Inspector,
        zone: vsr.Zone,
        offset_in_zone: u64,
        comptime size: usize,
    ) !*align(constants.sector_size) const [size]u8 {
        const buffer = try inspector.allocator.alignedAlloc(u8, constants.sector_size, size);
        errdefer inspector.allocator.free(buffer);

        inspector.storage.read_sectors(
            inspector_read_callback,
            &inspector.read,
            buffer,
            zone,
            offset_in_zone,
        );
        try inspector.work();
        return buffer[0..size];
    }

    fn read_block(inspector: *Inspector, address: u64, checksum: ?u128) !BlockPtrConst {
        const buffer = try inspector.read_buffer(
            .grid,
            (address - 1) * constants.block_size,
            constants.block_size,
        );
        errdefer inspector.allocator.free(buffer);

        const header = std.mem.bytesAsValue(vsr.Header.Block, buffer[0..@sizeOf(vsr.Header)]);
        if (!header.valid_checksum()) {
            log.err(
                "read_block: invalid block address={} checksum={?x:0>32} (bad checksum)",
                .{ address, checksum },
            );
            return error.InvalidChecksum;
        }

        if (!header.valid_checksum_body(buffer[@sizeOf(vsr.Header)..header.size])) {
            log.err(
                "read_block: invalid block address={} checksum={?x:0>32} (bad checksum_body)",
                .{ address, checksum },
            );
            return error.InvalidChecksumBody;
        }

        if (checksum) |checksum_| {
            if (header.checksum != checksum_) {
                log.err(
                    "read_block: invalid block address={} checksum={?x:0>32} (wrong block)",
                    .{ address, checksum },
                );
                return error.WrongBlock;
            }
        }
        return buffer;
    }
};

fn print_struct(
    output: anytype,
    label: []const u8,
    value: anytype,
) !void {
    const Type = @TypeOf(value);

    // Print structs *without* a custom format() function.
    if (@typeInfo(Type) == .Struct and !comptime std.meta.trait.hasFn("format")(Type)) {
        if (@typeInfo(Type).Struct.is_tuple) {
            try output.writeAll(label);
            // Print tuples as a single line.
            inline for (std.meta.fields(@TypeOf(value)), 0..) |field, i| {
                if (@typeInfo(field.type) == .Pointer and
                    @typeInfo(@typeInfo(field.type).Pointer.child) == .Array)
                {
                    // Allow inline labels.
                    try output.writeAll(@field(value, field.name));
                } else {
                    try print_value(output, @field(value, field.name));
                    if (i != std.meta.fields(@TypeOf(value)).len) try output.writeAll(" ");
                }
            }
            try output.writeAll("\n");
            return;
        } else {
            var label_buffer: [1024]u8 = undefined;
            inline for (std.meta.fields(@TypeOf(value))) |field| {
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().print("{s}.{s}", .{ label, field.name });
                try print_struct(output, label_stream.getWritten(), @field(value, field.name));
            }
            return;
        }
    }

    if (Element: {
        const type_info = @typeInfo(Type);
        if (type_info == .Array) {
            break :Element @as(?type, type_info.Array.child);
        }
        break :Element null;
    }) |Element| {
        if (Element == u8) {
            if (stdx.zeroed(&value)) {
                return output.print("{s}=[{}]u8{{0}}\n", .{ label, value.len });
            } else {
                return output.print("{s}=[{}]u8{{nonzero}}\n", .{ label, value.len });
            }
        } else {
            var label_buffer: [1024]u8 = undefined;
            for (value[0..], 0..) |item, index| {
                var label_stream = std.io.fixedBufferStream(&label_buffer);
                try label_stream.writer().print("{s}[{}]", .{ label, index });
                try print_struct(output, label_stream.getWritten(), item);
            }
            return;
        }
    }

    try output.print("{s}=", .{label});
    try print_value(output, value);
    try output.writeAll("\n");
}

fn print_value(output: anytype, value: anytype) !void {
    const Type = @TypeOf(value);
    if (@typeInfo(Type) == .Struct) assert(std.meta.trait.hasFn("format")(Type));
    assert(@typeInfo(Type) != .Array);

    if (Type == u128) return output.print("{x:0>32}", .{value});

    if (Type == vsr.Operation) {
        if (value.valid(StateMachine)) {
            return output.writeAll(value.tag_name(StateMachine));
        } else {
            return output.print("{}!", .{@intFromEnum(value)});
        }
    }

    if (@typeInfo(Type) == .Enum) {
        if (std.enums.tagName(Type, value)) |value_string| {
            return output.print("{s}", .{value_string});
        } else {
            return output.print("{}!", .{@intFromEnum(value)});
        }
    }
    try output.print("{}", .{value});
}

fn print_block(writer: anytype, block: BlockPtrConst) !void {
    const header = schema.header_from_block(block);
    try print_struct(writer, "header", header.*);

    inline for (.{
        .{ .block_type = .free_set, .Schema = schema.TrailerNode },
        .{ .block_type = .client_sessions, .Schema = schema.TrailerNode },
        .{ .block_type = .manifest, .Schema = schema.ManifestNode },
        .{ .block_type = .index, .Schema = schema.TableIndex },
        .{ .block_type = .data, .Schema = schema.TableData },
    }) |pair| {
        if (header.block_type == pair.block_type) {
            try print_struct(writer, "header.metadata", pair.Schema.metadata(block).*);
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
                index.data_addresses_used(block),
                index.data_checksums_used(block),
                0..,
            ) |data_address, data_checksum, i| {
                try writer.print(
                    "data_blocks[{:_>3}]: address={} checksum={x:0>32}\n",
                    .{ i, data_address, data_checksum.value },
                );
            }
        },
        .data => {
            const data = schema.TableData.from(block);
            const metadata = data.block_metadata(block);
            const data_bytes = data.block_values_used_bytes(block);
            inline for (StateMachine.Forest.tree_infos) |tree_info| {
                if (metadata.tree_id == tree_info.tree_id) {
                    for (std.mem.bytesAsSlice(tree_info.Tree.Table.Value, data_bytes)) |value| {
                        if (comptime is_composite_key(tree_info.Tree.Table.Value)) {
                            try print_struct(writer, " ", .{ value.field, value.timestamp });
                        } else {
                            try print_struct(writer, " ", value);
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

fn print_prepare_body(output: anytype, prepare: []const u8) !void {
    const operation_events = comptime result: {
        const OperationEvent = struct {
            operation: vsr.Operation,
            Event: type,
            min: usize,
            max: usize,
        };

        var list: []const OperationEvent = &[_]OperationEvent{
            .{ .operation = .reserved, .Event = extern struct {}, .min = 1, .max = 1 },
            .{ .operation = .root, .Event = extern struct {}, .min = 1, .max = 1 },
            .{ .operation = .register, .Event = extern struct {}, .min = 1, .max = 1 },
            .{ .operation = .reconfigure, .Event = vsr.ReconfigurationRequest, .min = 1, .max = 1 },
            .{ .operation = .pulse, .Event = extern struct {}, .min = 1, .max = 1 },
            .{ .operation = .upgrade, .Event = vsr.UpgradeRequest, .min = 1, .max = 1 },
        };

        for (std.enums.values(StateMachine.Operation)) |operation| {
            if (operation == .pulse) continue;
            list = list ++ [_]OperationEvent{.{
                .operation = vsr.Operation.from(StateMachine, operation),
                .Event = StateMachine.Event(operation),
                .min = 0,
                .max = @field(StateMachine.constants.batch_max, @tagName(operation)),
            }};
        }
        break :result list;
    };

    const header = std.mem.bytesAsValue(vsr.Header.Prepare, prepare[0..@sizeOf(vsr.Header)]);
    inline for (operation_events) |operation_event| {
        if (header.operation == operation_event.operation) {
            const event_size = @sizeOf(operation_event.Event);
            const body_size = header.size - @sizeOf(vsr.Header);
            if ((event_size == 0 and body_size == 0) or
                (event_size != 0 and body_size % event_size == 0))
            {
                if (body_size == 0) {
                    try output.print("(no body)\n", .{});
                } else {
                    if (event_size == 0) unreachable;

                    var label_buffer: [128]u8 = undefined;
                    for (std.mem.bytesAsSlice(
                        operation_event.Event,
                        prepare[@sizeOf(vsr.Header)..header.size],
                    ), 0..) |event, i| {
                        var label_stream = std.io.fixedBufferStream(&label_buffer);
                        try label_stream.writer().print("events[{}]: ", .{i});
                        try print_struct(output, label_stream.getWritten(), event);
                    }
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

fn GroupByType(comptime count_max: usize) type {
    return struct {
        const GroupBy = @This();
        const BitSet = std.StaticBitSet(count_max);

        count: usize = 0,
        checksums: [count_max]?u128 = [_]?u128{null} ** count_max,
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
                var matches = BitSet.initEmpty();
                for (&group_by.checksums, 0..) |checksum_b, b| {
                    matches.setValue(b, checksum_a.? == checksum_b.?);
                }
                if (matches.findFirstSet().? == a) {
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
