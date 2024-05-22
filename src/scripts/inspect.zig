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

pub const CliArgs = union(enum) {
    superblock: struct {
        positional: struct { path: []const u8 },
    },
    wal: struct {
        slot: ?usize = null,
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
