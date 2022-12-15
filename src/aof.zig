const std = @import("std");
const assert = std.debug.assert;
const os = std.os;

const constants = @import("constants.zig");
const demo = @import("demo.zig");
const vsr = @import("vsr.zig");
const tb = @import("tigerbeetle.zig");

const util = @import("util.zig");
const IO = @import("io.zig").IO;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = @import("message_pool.zig").MessagePool.Message;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const Storage = @import("storage.zig").Storage;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = constants.message_body_size_max,
});

const Header = vsr.Header;
const Account = tb.Account;
const Transfer = tb.Transfer;
const Client = vsr.Client(StateMachine, MessageBus);

const MAGIC_NUMBER = 312960301372567410560647846651901451202;

// Changing anything on the below two structs _will_ change the on-disk format. Be very careful
// when doing so. Alignment is also critical here.
// TODO: Explain why alignment is critical.
const AOFMetadata = packed struct {
    comptime {
        assert(@sizeOf(AOFMetadata) % 8 == 0);
    }

    vsr_header: Header,
    primary: u64,
    replica: u64,
};

const AOFEntry = packed struct {
    comptime {
        assert(@sizeOf(AOFEntry) % 8 == 0);
    }

    /// In case of extreme corruption, start each entry with a fixed random integer,
    /// to allow skipping over corrupted entries. Not currently used.
    magic_number: u128 = MAGIC_NUMBER,

    /// Arbitrary metadata we want to record
    metadata: AOFMetadata,

    /// The main Message to log. The actual length of the entire payload will be sector
    /// aligned, so it might continue past body_len.
    body_len: u64,
    body: [constants.message_size_max]u8,

    /// Reserved space at the end, in case we require padding past message_size_max.
    reserved: [constants.sector_size]u8 = std.mem.zeroes([constants.sector_size]u8),

    /// Calculate the actual length of the entry, from our fixed length struct. Make sure to
    /// exclude the reserved padding at the end, as we use this in case of large messages.
    pub fn calculate_padded_size(self: AOFEntry) u64 {
        const unpadded_size = @sizeOf(AOFEntry) - constants.message_size_max - constants.sector_size + self.body_len;
        const padding = (constants.sector_size - unpadded_size % constants.sector_size) % constants.sector_size;
        const padded_size = unpadded_size + padding;

        assert(padding < constants.sector_size);
        assert(padded_size % constants.sector_size == 0);

        return padded_size;
    }
};

/// The AOF itself is simple and deterministic - but it logs data like the client's id
/// which make things trickier. We could take a checksum of all checksum_body that
/// constitute one, and that would be able to compare between runs.
pub const AOF = struct {
    fd: os.fd_t,
    last_op: ?u64 = null,

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

    pub fn init(dir_fd: os.fd_t, relative_path: []const u8) !AOF {
        const fd = try IO.open_file(dir_fd, relative_path, 0, true);
        try os.lseek_END(fd, 0);

        return AOF{ .fd = fd };
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
    pub fn write(self: *AOF, message: *const Message, replica: u8, primary: u8) !void {
        const body = message.body();

        assert(body.len < constants.message_size_max);
        assert(self.last_op == null or message.header.op > self.last_op.?);

        // cluster is in the VSR header so we don't need to store it explcitly.
        var entry: AOFEntry align(constants.sector_size) = AOFEntry{
            .metadata = AOFMetadata{ .vsr_header = message.header.*, .replica = replica, .primary = primary },
            .body_len = body.len,
            .body = std.mem.zeroes([constants.message_size_max]u8),
        };

        std.mem.copy(u8, entry.body[0..body.len], body);

        const padded_size = entry.calculate_padded_size();
        std.log.debug("{}: aof: writing record of {} bytes", .{ replica, padded_size });

        // writeAll logic - in case we're interrupted by a signal. Needed for direct io? Alignment?
        const bytes = std.mem.asBytes(&entry);
        var index: usize = 0;
        while (index != padded_size) {
            index += try os.write(self.fd, bytes[index..padded_size]);
        }

        assert(index == padded_size);
    }

    pub const Iterator = struct {
        file: std.fs.File,

        // This really isn't an efficient way to loop through entries, but it works
        size: u64,
        read_to: u64 = 0,

        validate_header_checksum: bool = true,
        validate_body_checksum: bool = true,

        pub fn next(it: *Iterator) !?AOFEntry {
            if (it.read_to >= it.size) return null;

            try it.file.seekTo(it.read_to);

            var buf: [@sizeOf(AOFEntry)]u8 = undefined;
            _ = try it.file.readAll(buf[0..@sizeOf(AOFEntry)]);
            const entry = std.mem.bytesAsSlice(AOFEntry, buf[0..buf.len])[0];

            assert(entry.magic_number == MAGIC_NUMBER);

            const header = entry.metadata.vsr_header;

            if (it.validate_header_checksum) {
                const checksum = header.calculate_checksum();
                assert(header.checksum == checksum);
            }

            if (it.validate_body_checksum) {
                const checksum_body = header.calculate_checksum_body(entry.body[0..entry.body_len]);
                assert(header.checksum_body == checksum_body);
            }

            it.read_to += entry.calculate_padded_size();

            return entry;
        }

        pub fn reset(it: *Iterator) void {
            try it.file.seekTo(0);
            it.read_to = 0;
        }

        pub fn close(it: *Iterator) void {
            it.file.close();
        }
    };

    /// Return an iterator into an AOF, to read entries one by one. By default, this also
    /// validates that both the header and body checksums of the read row are valid.
    pub fn iterator(path: []const u8) !Iterator {
        const file = try std.fs.cwd().openFile(path, .{ .read = true });
        const size = (try file.stat()).size;

        return Iterator{ .file = file, .size = size };
    }
};

fn on_reply(
    _: u128,
    _: StateMachine.Operation,
    _: Client.Error![]const u8,
) void {}

/// Mostly lifted directly from demo.zig. We could reuse the same client, but for now
/// we create a new client for each replay (!) to get the same client IDs.
/// TODO: Currently broken
pub fn aof_replay(aof_entry: *const AOFEntry) !void {
    const allocator = std.heap.page_allocator;
    const client_id = aof_entry.metadata.vsr_header.client;
    const cluster_id: u32 = @intCast(u32, aof_entry.metadata.vsr_header.cluster);
    var addresses = [_]std.net.Address{try std.net.Address.parseIp4("127.0.0.1", constants.port)};

    var io = try IO.init(32, 0);
    defer io.deinit();

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, addresses.len),
        &message_pool,
        .{
            .configuration = &addresses,
            .io = &io,
        },
    );
    defer client.deinit(allocator);

    const message = client.get_message();
    defer client.unref(message);

    util.copy_disjoint(.inexact, u8, message.buffer[@sizeOf(Header)..], aof_entry.body[0..aof_entry.body_len]);

    const operation = aof_entry.metadata.vsr_header.operation.cast(StateMachine);

    client.request(
        0,
        on_reply,
        operation,
        message,
        aof_entry.body_len,
    );

    while (client.request_queue.count > 0) {
        client.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

const testing = std.testing;

test "aof write / read" {
    const aof_file = "./test.aof";
    std.fs.cwd().deleteFile(aof_file) catch {};
    defer std.fs.cwd().deleteFile(aof_file) catch {};

    var aof = try AOF.from_absolute_path(aof_file);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    const demo_message = message_pool.get_message();
    defer message_pool.unref(demo_message);

    const demo_payload = "hello world";

    // The command / operation used here don't matter - we verify things bitwise.
    demo_message.header.* = .{
        .client = 0,
        .request = 0,
        .cluster = 0,
        .command = vsr.Command.prepare,
        .operation = @intToEnum(vsr.Operation, 4),
        .size = @intCast(u32, @sizeOf(Header) + demo_payload.len),
    };

    const body = demo_message.body();
    std.mem.copy(u8, body, demo_payload);
    demo_message.header.set_checksum_body(demo_payload);
    demo_message.header.set_checksum();

    try aof.write(demo_message, 1, 1);

    var it = try AOF.iterator(aof_file);
    defer it.close();

    const read_message = (try it.next()).?;

    try testing.expect(read_message.metadata.replica == 1);
    try testing.expect(read_message.metadata.primary == 1);
    try testing.expect(std.mem.eql(u8, std.mem.asBytes(demo_message.header), std.mem.asBytes(&read_message.metadata.vsr_header)));
    try testing.expect(std.mem.eql(u8, demo_message.body(), read_message.body[0..read_message.body_len]));

    // Ensure our iterator works correctly and stops at EOF.
    try testing.expect((try it.next()) == null);
}

const usage =
    \\Usage:
    \\
    \\  aof [-h | --help]
    \\
    \\  aof recover <addresses> <path>
    \\
    \\  aof debug <path>
    \\
    \\Commands:
    \\
    \\  recover  Recover a recorded AOF file at <path> to a TigerBeetle cluster running
    \\           at <addresses>. Said cluster must be running with aof_recovery = true
    \\           and have the same cluster ID as the source.
    \\
    \\  debug    Print all entries that have been recoreded in the AOF file at <path>
    \\           to stdout. Checksums are verified, and aof will panic if an invalid
    \\           checksum is encountered, so this can be used to check the validity
    \\           of an AOF file.
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
;

pub fn main() !void {
    var args = std.process.args();

    var action: ?[:0]const u8 = null;
    var addresses: ?[:0]const u8 = null;
    var path: ?[:0]const u8 = null;
    var count: usize = 0;

    while (args.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        }

        if (count == 1) {
            action = arg;
        } else if (count == 2 and std.mem.eql(u8, action.?, "recover")) {
            addresses = arg;
        } else if (count == 2 and std.mem.eql(u8, action.?, "debug")) {
            path = arg;
        } else if (count == 3) {
            path = arg;
        }

        count += 1;
    }

    if (action != null and std.mem.eql(u8, action.?, "recover") and count == 4) {
        var it = try AOF.iterator(path.?);
        defer it.close();

        while (try it.next()) |entry| {
            aof_replay(&entry) catch @panic("error replaying");
        }
    } else if (action != null and std.mem.eql(u8, action.?, "debug") and count == 3) {
        var it = try AOF.iterator(path.?);
        defer it.close();

        const stdout = std.io.getStdOut().writer();
        while (try it.next()) |entry| {
            stdout.print("{}\n", .{entry.metadata}) catch @panic("error printing");
        }
    } else {
        std.io.getStdOut().writeAll(usage) catch os.exit(1);
        os.exit(1);
    }
}
