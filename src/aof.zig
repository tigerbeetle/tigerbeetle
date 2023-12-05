const std = @import("std");
const assert = std.debug.assert;
const os = std.os;

const constants = @import("constants.zig");
const vsr = @import("vsr.zig");
const tb = @import("tigerbeetle.zig");

const stdx = @import("stdx.zig");
const IO = @import("io.zig").IO;
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;
const MessageBus = vsr.message_bus.MessageBusClient;
const Storage = vsr.storage.Storage;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);

const Header = vsr.Header;
const Account = tb.Account;
const Transfer = tb.Transfer;
const Client = vsr.Client(StateMachine, MessageBus);
const log = std.log.scoped(.aof);

const magic_number: u128 = 312960301372567410560647846651901451202;

/// On-disk format for AOF Metadata.
pub const AOFEntryMetadata = extern struct {
    primary: u64,
    replica: u64,
    // Use large padding here to align the message itself to the sector boundary.
    reserved: [4064]u8 = std.mem.zeroes([4064]u8),

    comptime {
        assert(stdx.no_padding(AOFEntryMetadata));
    }
};

pub const AOFEntry = extern struct {
    /// In case of extreme corruption, start each entry with a fixed random integer,
    /// to allow skipping over corrupted entries.
    magic_number: u128 = magic_number,

    /// Arbitrary metadata we want to record
    metadata: AOFEntryMetadata,

    /// The main Message to log. The actual length of the entire payload will be sector
    /// aligned, so we might write past what the VSR header in here indicates.
    message: [constants.message_size_max]u8 align(constants.sector_size),

    comptime {
        assert(stdx.no_padding(AOFEntry));
    }

    /// Calculate the actual length of the AOFEntry that needs to be written to disk,
    /// accounting for sector alignment.
    pub fn calculate_disk_size(self: *AOFEntry) u64 {
        const unaligned_size = @sizeOf(AOFEntry) - self.message.len + self.header().size;

        return vsr.sector_ceil(unaligned_size);
    }

    pub fn header(self: *AOFEntry) *Header.Prepare {
        return @ptrCast(&self.message);
    }

    /// Turn an AOFEntry back into a Message.
    pub fn to_message(self: *AOFEntry, target: *Message.Prepare) void {
        stdx.copy_disjoint(.inexact, u8, target.buffer, self.message[0..self.header().size]);
    }

    pub fn from_message(
        self: *AOFEntry,
        message: *const Message.Prepare,
        options: struct { replica: u64, primary: u64 },
        last_checksum: *?u128,
    ) void {
        assert(message.header.size <= self.message.len);

        // When writing, entries can backtrack / duplicate, so we don't necessarily have a valid
        // chain. Still, log when that happens. The `aof merge` command can generate a consistent
        // file from entries like these.
        log.debug("{}: from_message: parent {} (should == {?}) our checksum {}", .{
            options.replica,
            message.header.parent,
            last_checksum.*,
            message.header.checksum,
        });
        if (last_checksum.* == null or last_checksum.*.? != message.header.parent) {
            log.info("{}: from_message: parent {}, expected {?} instead", .{
                options.replica,
                message.header.parent,
                last_checksum.*,
            });
        }
        last_checksum.* = message.header.checksum;

        // The cluster identifier is in the VSR header so we don't need to store it explicitly.
        self.* = AOFEntry{
            .metadata = AOFEntryMetadata{
                .replica = options.replica,
                .primary = options.primary,
            },
            .message = undefined,
        };

        stdx.copy_disjoint(
            .exact,
            u8,
            self.message[0..message.header.size],
            message.buffer[0..message.header.size],
        );
        @memset(self.message[message.header.size..self.message.len], 0);
    }
};

/// The AOF itself is simple and deterministic - but it logs data like the client's id
/// which make things trickier. If you want to compare AOFs between runs, the `debug`
/// CLI command does it by hashing together all checksum_body, operation and timestamp
/// fields.
pub const AOF = struct {
    fd: os.fd_t,
    last_checksum: ?u128 = null,

    /// Create an AOF given an absolute path. Handles opening the
    /// dir_fd and ensuring everything (including the dir) is
    /// fsync'd appropriately.
    pub fn from_absolute_path(absolute_path: []const u8) !AOF {
        const dirname = std.fs.path.dirname(absolute_path) orelse ".";
        const dir_fd = try IO.open_dir(dirname);
        errdefer os.close(dir_fd);

        const basename = std.fs.path.basename(absolute_path);

        return AOF.init(dir_fd, basename);
    }

    fn init(dir_fd: os.fd_t, relative_path: []const u8) !AOF {
        const fd = try IO.open_file(dir_fd, relative_path, 0, .create_or_open);
        errdefer os.close(fd);

        try os.lseek_END(fd, 0);

        return AOF{ .fd = fd };
    }

    pub fn close(self: *AOF) void {
        os.close(self.fd);
    }

    /// Write a message to disk. Once this function returns, the data passed in
    /// is guaranteed to have been written using O_DIRECT and O_SYNC and
    /// can be considered safely persisted for recovery purposes once this
    /// call returns.
    ///
    /// We purposefully use standard disk IO here, and not IO_uring. It'll
    /// be slower and have syscall overhead, but it's considerably more
    /// battle tested.
    ///
    /// We don't bother returning a count of how much we wrote. Not being
    /// able to fully write the entire payload is an error, not an expected
    /// condition.
    pub fn write(self: *AOF, message: *const Message.Prepare, options: struct {
        replica: u64,
        primary: u64,
    }) !void {
        var entry: AOFEntry align(constants.sector_size) = undefined;
        entry.from_message(
            message,
            .{ .replica = options.replica, .primary = options.primary },
            &self.last_checksum,
        );

        const disk_size = entry.calculate_disk_size();

        const bytes = std.mem.asBytes(&entry);

        // We don't need writeAll logic here. write() on Linux can't be interrupted
        // by signals, and a single write supports up to 0x7ffff000 bytes, which is
        // much greater than the size of our struct could ever be. Zig handles EINTR
        // for us automatically.
        const bytes_written = try os.write(self.fd, bytes[0..disk_size]);

        assert(bytes_written == disk_size);
    }

    pub fn IteratorType(comptime File: type) type {
        return struct {
            const Self = @This();

            file: File,
            size: u64,
            offset: u64 = 0,

            validate_chain: bool = true,
            last_checksum: ?u128 = null,

            pub fn next(it: *Self, target: *AOFEntry) !?*AOFEntry {
                if (it.offset >= it.size) return null;

                try it.file.seekTo(it.offset);

                var buf = std.mem.asBytes(target);
                const bytes_read = try it.file.readAll(buf);

                if (bytes_read < target.calculate_disk_size()) {
                    return error.AOFShortRead;
                }

                if (target.magic_number != magic_number) {
                    return error.AOFMagicNumberMismatch;
                }

                const header = target.header();
                if (!header.valid_checksum()) {
                    return error.AOFChecksumMismatch;
                }

                if (!header.valid_checksum_body(target.message[@sizeOf(Header)..header.size])) {
                    return error.AOFBodyChecksumMismatch;
                }

                // Ensure this file has a consistent hash chain
                if (it.validate_chain) {
                    if (it.last_checksum != null and it.last_checksum.? != header.parent) {
                        return error.AOFChecksumChainMismatch;
                    }
                }

                it.last_checksum = header.checksum;

                it.offset += target.calculate_disk_size();

                return target;
            }

            pub fn reset(it: *Self) !void {
                it.offset = 0;
            }

            pub fn close(it: *Self) void {
                it.file.close();
            }

            /// Try skip ahead to the next entry in a potentially corrupted AOF file
            /// by searching from our current position for the next magic_number, seeking
            /// to it, and setting our internal position correctly.
            pub fn skip(it: *Self, allocator: std.mem.Allocator, count: usize) !void {
                var skip_buffer = try allocator.alloc(u8, 1024 * 1024);
                defer allocator.free(skip_buffer);

                try it.file.seekTo(it.offset);

                while (it.offset < it.size) {
                    const bytes_read = try it.file.readAll(skip_buffer);
                    const offset = std.mem.indexOfPos(
                        u8,
                        skip_buffer[0..bytes_read],
                        count,
                        std.mem.asBytes(&magic_number),
                    );

                    if (offset) |offset_bytes| {
                        it.offset += offset_bytes;
                        break;
                    } else {
                        it.offset += skip_buffer.len;
                    }
                }
            }
        };
    }

    pub const Iterator = IteratorType(std.fs.File);

    /// Return an iterator into an AOF, to read entries one by one. This also validates
    /// that both the header and body checksums of the read entry are valid, and that
    /// all checksums chain correctly.
    pub fn iterator(path: []const u8) !Iterator {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
        errdefer file.close();

        const size = (try file.stat()).size;

        return Iterator{ .file = file, .size = size };
    }
};

pub const AOFReplayClient = struct {
    const Self = @This();

    client: *Client,
    io: *IO,
    message_pool: *MessagePool,
    inflight_message: ?*Message.Request = null,

    pub fn init(allocator: std.mem.Allocator, raw_addresses: []const u8) !Self {
        var addresses = try vsr.parse_addresses(allocator, raw_addresses, constants.replicas_max);

        var io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        var message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        var client = try allocator.create(Client);
        errdefer allocator.destroy(client);

        io.* = try IO.init(32, 0);
        errdefer io.deinit();

        message_pool.* = try MessagePool.init(allocator, .client);
        errdefer message_pool.deinit(allocator);

        client.* = try Client.init(
            allocator,
            std.crypto.random.int(u128),
            0,
            @as(u8, @intCast(addresses.len)),
            message_pool,
            .{
                .configuration = addresses,
                .io = io,
            },
        );
        errdefer client.deinit(allocator);

        return Self{
            .io = io,
            .message_pool = message_pool,
            .client = client,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.client.deinit(allocator);
        self.message_pool.deinit(allocator);
        self.io.deinit();

        allocator.destroy(self.client);
        allocator.destroy(self.message_pool);
        allocator.destroy(self.io);
    }

    pub fn replay(self: *Self, aof: *AOF.Iterator) !void {
        var target: AOFEntry = undefined;

        while (try aof.next(&target)) |entry| {
            // Skip replaying reserved messages.
            const header = entry.header();
            if (header.operation.vsr_reserved()) continue;

            const message = self.client.get_message().build(.request);
            errdefer self.client.release(message.base());

            assert(self.inflight_message == null);
            self.inflight_message = message;

            entry.to_message(message.base().build(.prepare));

            message.header.* = .{
                .client = self.client.id,
                .cluster = self.client.cluster,
                .command = .request,
                .operation = header.operation,
                .size = header.size,
                .timestamp = header.timestamp,
                .view = 0,
                .parent = 0,
                .session = 0,
                .request = 0,
            };

            self.client.raw_request(@intFromPtr(self), AOFReplayClient.replay_callback, message);

            // Process messages one by one for now
            while (self.client.request_queue.count > 0) {
                self.client.tick();
                try self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
        }
    }

    fn replay_callback(
        user_data: u128,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        _ = operation;
        _ = result;

        const self = @as(*AOFReplayClient, @ptrFromInt(@as(u64, @intCast(user_data))));
        assert(self.inflight_message != null);
        self.inflight_message = null;
    }
};

pub fn aof_merge(
    allocator: std.mem.Allocator,
    input_paths: [][]const u8,
    output_path: []const u8,
) !void {
    const stdout = std.io.getStdOut().writer();

    var aofs: [constants.members_max]AOF.Iterator = undefined;
    var aof_count: usize = 0;
    defer for (aofs[0..aof_count]) |*it| it.close();

    assert(input_paths.len < aofs.len);

    for (input_paths) |input_path| {
        aofs[aof_count] = try AOF.iterator(input_path);
        aof_count += 1;
    }

    const EntryInfo = struct {
        aof: *AOF.Iterator,
        index: u64,
        size: u64,
        checksum: u128,
        parent: u128,
    };

    var message_pool = try MessagePool.init_capacity(allocator, 1);
    defer message_pool.deinit(allocator);

    var entries_by_parent = std.AutoHashMap(u128, EntryInfo).init(allocator);
    defer entries_by_parent.deinit();

    var target = try allocator.create(AOFEntry);
    defer allocator.destroy(target);

    var output_aof = try AOF.from_absolute_path(output_path);

    // First, iterate all AOFs and build a mapping between parent checksums and where the entry is
    // located.
    try stdout.print("Building checksum map...\n", .{});
    var current_parent: ?u128 = null;
    for (aofs[0..aof_count], 0..) |*aof, i| {
        // While building our checksum map, don't validate our hash chain. We might have a file that
        // has a broken chain, but still contains valid data that can be used for recovery with
        // other files.
        aof.validate_chain = false;

        while (true) {
            var entry = aof.next(target) catch |err| {
                switch (err) {
                    // If our magic number is corrupted, skip to the next entry.
                    error.AOFMagicNumberMismatch => {
                        try stdout.print(
                            "{s}: Skipping entry with corrupted magic number.\n",
                            .{input_paths[i]},
                        );
                        try aof.skip(allocator, 0);
                        continue;
                    },

                    // Otherwise, we need to skip over our valid magic number, to the next one
                    // (since the pointer is only updated after a successful read, calling .skip(0))
                    // will not do anything here.
                    error.AOFChecksumMismatch, error.AOFBodyChecksumMismatch => {
                        try stdout.print(
                            "{s}: Skipping entry with corrupted checksum.\n",
                            .{input_paths[i]},
                        );
                        try aof.skip(allocator, 1);
                        continue;
                    },

                    error.AOFShortRead => {
                        try stdout.print(
                            "{s}: Skipping truncated entry at EOF.\n",
                            .{input_paths[i]},
                        );
                        break;
                    },

                    else => @panic("Unexpected Error"),
                }
                break;
            };

            if (entry == null) {
                break;
            }

            const header = entry.?.header();
            const checksum = header.checksum;
            const parent = header.parent;

            if (current_parent == null) {
                try stdout.print(
                    "The root checksum will be {} from {s}.\n",
                    .{ parent, input_paths[i] },
                );
                current_parent = parent;
            }

            const v = try entries_by_parent.getOrPut(parent);
            if (v.found_existing) {
                // If the entry already exists in our mapping, and it's identical, that's OK. If
                // it's not however, it indicates the log has been forked somehow.
                assert(v.value_ptr.checksum == checksum);
            } else {
                v.value_ptr.* = .{
                    .aof = aof,
                    .index = aof.offset - entry.?.calculate_disk_size(),
                    .size = entry.?.calculate_disk_size(),
                    .checksum = checksum,
                    .parent = parent,
                };
            }
        }
        try stdout.print(
            "Finished processing {s} - extracted {} usable entries.\n",
            .{ input_paths[i], entries_by_parent.count() },
        );
    }

    // Next, start from our root checksum, walk down the hash chain until there's nothing left. We
    // currently take the root checksum as the first entry in the first AOF.
    while (entries_by_parent.count() > 0) {
        var message = message_pool.get_message(.prepare);
        defer message_pool.unref(message);

        assert(current_parent != null);
        const entry = entries_by_parent.getPtr(current_parent.?) orelse unreachable;

        try entry.aof.file.seekTo(entry.index);
        var buf = std.mem.asBytes(target)[0..entry.size];
        const bytes_read = try entry.aof.file.readAll(buf);

        // None of these conditions should happen, but double check them to prevent any TOCTOUs
        if (bytes_read != target.calculate_disk_size()) {
            @panic("unexpected short read while reading AOF entry");
        }

        const header = target.header();
        if (!header.valid_checksum()) {
            @panic("unexpected checksum error while merging");
        }

        if (!header.valid_checksum_body(target.message[@sizeOf(Header)..header.size])) {
            @panic("unexpected body checksum error while merging");
        }

        target.to_message(message);
        try output_aof.write(
            message,
            .{ .replica = target.metadata.replica, .primary = target.metadata.primary },
        );

        current_parent = entry.checksum;
        _ = entries_by_parent.remove(entry.parent);
    }

    output_aof.close();

    // Validate the newly created output file
    try stdout.print("Validating Output {s}\n", .{output_path});

    var it = try AOF.iterator(output_path);
    defer it.close();

    var first_checksum: ?u128 = null;
    var last_checksum: ?u128 = null;

    while (try it.next(target)) |entry| {
        const header = entry.header();
        if (first_checksum == null) {
            first_checksum = header.checksum;
        }

        last_checksum = header.checksum;
    }

    try stdout.print(
        "AOF {s} validated. Starting checksum: {?} Ending checksum: {?}\n",
        .{ output_path, first_checksum, last_checksum },
    );
}

const testing = std.testing;

test "aof write / read" {
    const aof_file = "./test.aof";
    std.fs.cwd().deleteFile(aof_file) catch {};
    defer std.fs.cwd().deleteFile(aof_file) catch {};

    const allocator = std.testing.allocator;

    var aof = try AOF.from_absolute_path(aof_file);

    var message_pool = try MessagePool.init_capacity(allocator, 2);
    defer message_pool.deinit(allocator);

    const demo_message = message_pool.get_message(.prepare);
    defer message_pool.unref(demo_message);

    var target = try allocator.create(AOFEntry);
    defer allocator.destroy(target);

    const demo_payload = "hello world";

    // The command / operation used here don't matter - we verify things bitwise.
    demo_message.header.* = .{
        .op = 0,
        .commit = 0,
        .view = 0,
        .client = 0,
        .request = 0,
        .parent = 0,
        .request_checksum = 0,
        .cluster = 0,
        .timestamp = 0,
        .checkpoint_id = 0,
        .command = .prepare,
        .operation = @as(vsr.Operation, @enumFromInt(4)),
        .size = @as(u32, @intCast(@sizeOf(Header) + demo_payload.len)),
    };

    stdx.copy_disjoint(.exact, u8, demo_message.body(), demo_payload);
    demo_message.header.set_checksum_body(demo_payload);
    demo_message.header.set_checksum();

    try aof.write(demo_message, .{ .replica = 1, .primary = 1 });
    aof.close();

    var it = try AOF.iterator(aof_file);
    defer it.close();

    const read_entry = (try it.next(target)).?;

    // Check that to_message also works as expected
    const read_message = message_pool.get_message(.prepare);
    defer message_pool.unref(read_message);

    read_entry.to_message(read_message);
    try testing.expect(std.mem.eql(
        u8,
        demo_message.buffer[0..demo_message.header.size],
        read_message.buffer[0..read_message.header.size],
    ));

    try testing.expect(read_entry.metadata.replica == 1);
    try testing.expect(read_entry.metadata.primary == 1);
    try testing.expect(std.mem.eql(
        u8,
        demo_message.buffer[0..demo_message.header.size],
        read_entry.message[0..read_entry.header().size],
    ));

    // Ensure our iterator works correctly and stops at EOF.
    try testing.expect((try it.next(target)) == null);
}

test "aof merge" {}

const usage =
    \\Usage:
    \\
    \\  aof [-h | --help]
    \\
    \\  aof recover <addresses> <path>
    \\
    \\  aof debug <path>
    \\
    \\  aof merge path.aof ... <path.aof n>
    \\
    \\
    \\Commands:
    \\
    \\  recover  Recover a recorded AOF file at <path> to a TigerBeetle cluster running
    \\           at <addresses>. Said cluster must be running with aof_recovery = true
    \\           and have the same cluster ID as the source. The AOF must have a consistent
    \\           hash chain, which can be ensured using the `merge` subcommand.
    \\
    \\  debug    Print all entries that have been recorded in the AOF file at <path>
    \\           to stdout. Checksums are verified, and aof will panic if an invalid
    \\           checksum is encountered, so this can be used to check the validity
    \\           of an AOF file. Prints a final hash of all data entries in the AOF.
    \\
    \\  merge    Walk through multiple AOF files, extracting entries from each one
    \\           that pass validation, and build a single valid AOF. The first entry
    \\           of the first specified AOF file will be considered the root hash.
    \\           Can also be used to merge multiple incomplete AOF files into one,
    \\           or re-order a single AOF file. Will output to `merged.aof`.
    \\
    \\           NB: Make sure to run merge with at least half of the replicas' AOFs,
    \\           otherwise entries might be lost.
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
    \\
;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    var action: ?[:0]const u8 = null;
    var addresses: ?[:0]const u8 = null;
    var paths: [constants.members_max][:0]const u8 = undefined;
    var count: usize = 0;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        }

        if (count == 1) {
            action = arg;
        } else if (count == 2 and std.mem.eql(u8, action.?, "recover")) {
            addresses = arg;
        } else if (count == 2 and std.mem.eql(u8, action.?, "debug")) {
            paths[0] = arg;
        } else if (count == 3 and std.mem.eql(u8, action.?, "recover")) {
            paths[0] = arg;
        } else if (count >= 2 and std.mem.eql(u8, action.?, "merge")) {
            paths[count - 2] = arg;
        }

        count += 1;
    }

    var target = try allocator.create(AOFEntry);
    defer allocator.destroy(target);

    if (action != null and std.mem.eql(u8, action.?, "recover") and count == 4) {
        var it = try AOF.iterator(paths[0]);
        defer it.close();

        var replay = try AOFReplayClient.init(allocator, addresses.?);
        defer replay.deinit(allocator);

        try replay.replay(&it);
    } else if (action != null and std.mem.eql(u8, action.?, "debug") and count == 3) {
        var it = try AOF.iterator(paths[0]);
        defer it.close();

        var data_checksum: [32]u8 = undefined;
        var blake3 = std.crypto.hash.Blake3.init(.{});

        const stdout = std.io.getStdOut().writer();
        while (try it.next(target)) |entry| {
            const header = entry.header();
            try stdout.print("{} {}\n", .{ header, entry.metadata });

            // The body isn't the only important information, there's also the operation
            // and the timestamp which are in the header. Include those in our hash too.
            if (@intFromEnum(header.operation) > constants.vsr_operations_reserved) {
                blake3.update(std.mem.asBytes(&header.checksum_body));
                blake3.update(std.mem.asBytes(&header.timestamp));
                blake3.update(std.mem.asBytes(&header.operation));
            }
        }
        blake3.final(data_checksum[0..]);
        try stdout.print(
            "\nData checksum chain: {}\n",
            .{@as(u128, @bitCast(data_checksum[0..@sizeOf(u128)].*))},
        );
    } else if (action != null and std.mem.eql(u8, action.?, "merge") and count >= 2) {
        try aof_merge(allocator, paths[0 .. count - 2], "prepared.aof");
    } else {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(1);
    }
}
