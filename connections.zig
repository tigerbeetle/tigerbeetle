const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const os = std.os;
const testing = std.testing;
const Allocator = mem.Allocator;

const config = @import("config.zig");

pub const Connection = struct {
    id: u32,
    fd: os.fd_t,
    references: usize,
    recv_size: usize,
    send_offset: usize,
    send_size: usize,
    // TODO See if we can get away with a single recv/send buffer using stack memory.
    recv: [config.tcp_connection_buffer_max]u8 = undefined,
    send: [config.tcp_connection_buffer_max]u8 = undefined,
};

/// This abstracts all our server connection management logic and static allocation.
/// For example, when a new connection comes in, this finds a free pre-allocated connection struct.
/// The reason why we don't simply map socket fds to an array of fixed buffers is that socket fds
/// are sparse numbers and we would waste too much memory, especially where we already have many
/// other fds open for journalling or replication.
pub const Connections = struct {
    allocator: *Allocator,
    accepting: bool,
    active: usize,
    array: []Connection,

    pub fn init(allocator: *Allocator, connection_count: usize) !Connections {
        if (connection_count == 0) return error.ConnectionCountCannotBeZero;
        if (connection_count > 128) return error.ConnectionCountRequiresExcessiveMemory;
        var array = try allocator.alloc(Connection, connection_count);
        for (array) |*connection, i| {
            connection.* = .{
                .id = 0,
                .fd = -1,
                .references = 0,
                .recv_size = 0,
                .send_offset = 0,
                .send_size = 0,
            };
            @memset(connection.recv[0..], 0, connection.recv.len);
            @memset(connection.send[0..], 0, connection.send.len);
        }
        return Connections {
            .allocator = allocator,
            .accepting = false,
            .active = 0,
            .array = array
        };
    }

    pub fn deinit(self: *Connections) void {
        assert(self.accepting == false);
        assert(self.active == 0);
        self.allocator.free(self.array);
    }
    
    pub fn available(self: *Connections) bool {
        assert(self.active <= self.array.len);
        return self.active < self.array.len;
    }

    pub fn get(self: *Connections, id: u32) !*Connection {
        if (id >= self.array.len) return error.ConnectionOutOfRange;
        if (self.active == 0) return error.NoActiveConnections;
        var connection = &self.array[id];
        if (connection.fd < 0) return error.ConnectionFileDescriptorInvalid;
        if (connection.references < 1) return error.ConnectionReferencesInvalid;
        return connection;
    }

    pub fn set(self: *Connections, fd: os.fd_t) !*Connection {
        assert(fd >= 0);
        // We expect the TigerBeetle client to maintain long-lived connections.
        // We therefore expect a thundering herd of N new connections at most every T seconds.
        // TODO Return an error if a high reconnection rate indicates the lack of client keep alive.
        if (!self.available()) return error.ConnectionLimitReached;
        for (self.array) |*connection, index| {
            if (connection.references == 0) {
                assert(connection.id == 0);
                assert(connection.fd == -1);
                assert(connection.recv_size == 0);
                assert(connection.send_offset == 0);
                assert(connection.send_size == 0);
                connection.id = @intCast(u32, index);
                connection.fd = fd;
                connection.references += 1;
                self.active += 1;
                assert(self.active <= self.array.len);
                return connection;
            }
        }
        unreachable;
    }

    pub fn unset(self: *Connections, id: u32) !void {
        const connection = try self.get(id);
        if (connection.references > 1) return error.ConnectionHasLiveReferences;
        // In release safe mode, 0xaa will also be written to the receive and send buffers:
        connection.* = .{
            .id = 0,
            .fd = -1,
            .references = 0,
            .recv_size = 0,
            .send_offset = 0,
            .send_size = 0
        };
        @memset(connection.recv[0..], 0, connection.recv.len);
        @memset(connection.send[0..], 0, connection.send.len);
        assert(self.active > 0);
        assert(self.active <= self.array.len);
        self.active -= 1;
    }
};

test "connections" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const count: usize = 4;
    var connections = try Connections.init(allocator, count);
    defer connections.deinit();

    testing.expectEqual(false, connections.accepting);
    testing.expectEqual(@as(usize, 0), connections.active);
    testing.expectEqual(count, connections.array.len);
    testing.expectEqual(true, connections.available());

    const zeroes = [_]u8{0} ** config.tcp_connection_buffer_max;
    for (connections.array) |*connection, index| {
        testing.expectEqual(@as(u32, 0), connection.id);
        testing.expectEqual(@as(os.fd_t, -1), connection.fd);
        testing.expectEqual(@as(usize, 0), connection.references);
        testing.expectEqual(@as(usize, 0), connection.recv_size);
        testing.expectEqual(@as(usize, 0), connection.send_offset);
        testing.expectEqual(@as(usize, 0), connection.send_size);
        testing.expectEqualSlices(u8, zeroes[0..], connection.recv[0..]);
        testing.expectEqualSlices(u8, zeroes[0..], connection.send[0..]);
    }

    const fd0: os.fd_t = 9;
    const id0 = (try connections.set(fd0)).id;
    testing.expectEqual(@as(u32, 0), id0);
    testing.expectEqual(@as(usize, 1), connections.active);
    testing.expectEqual(true, connections.available());
    const co0 = try connections.get(id0);
    testing.expectEqual(id0, co0.id);
    testing.expectEqual(fd0, co0.fd);
    co0.recv_size = 100;
    co0.send_offset = 0;
    co0.send_size = 100;

    const fd1: os.fd_t = 8;
    const id1 = (try connections.set(fd1)).id;
    testing.expectEqual(@as(u32, 1), id1);
    testing.expectEqual(@as(usize, 2), connections.active);
    testing.expectEqual(true, connections.available());
    const co1 = try connections.get(id1);
    testing.expectEqual(id1, co1.id);
    testing.expectEqual(fd1, co1.fd);
    
    const fd2: os.fd_t = 7;
    const id2 = (try connections.set(fd2)).id;
    testing.expectEqual(@as(u32, 2), id2);
    testing.expectEqual(@as(usize, 3), connections.active);
    testing.expectEqual(true, connections.available());
    const co2 = try connections.get(id2);
    testing.expectEqual(id2, co2.id);
    testing.expectEqual(fd2, co2.fd);
    
    const fd3: os.fd_t = 6;
    const id3 = (try connections.set(fd3)).id;
    testing.expectEqual(@as(u32, 3), id3);
    testing.expectEqual(@as(usize, 4), connections.active);
    testing.expectEqual(false, connections.available());
    const co3 = try connections.get(id3);
    testing.expectEqual(id3, co3.id);
    testing.expectEqual(fd3, co3.fd);
    
    testing.expectError(error.ConnectionLimitReached, connections.set(0));
    
    try connections.unset(id0);
    testing.expectEqual(@as(usize, 3), connections.active);
    testing.expectEqual(true, connections.available());

    try connections.unset(id1);
    testing.expectEqual(@as(usize, 2), connections.active);
    testing.expectEqual(true, connections.available());

    try connections.unset(id2);
    testing.expectEqual(@as(usize, 1), connections.active);
    testing.expectEqual(true, connections.available());

    try connections.unset(id3);
    testing.expectEqual(@as(usize, 0), connections.active);
    testing.expectEqual(true, connections.available());

    const fd4: os.fd_t = 5;
    const id4 = (try connections.set(fd4)).id;
    testing.expectEqual(@as(u32, 0), id4);
    testing.expectEqual(@as(usize, 1), connections.active);
    testing.expectEqual(true, connections.available());
    const co4 = try connections.get(id4);
    testing.expectEqual(id4, co4.id);
    testing.expectEqual(fd4, co4.fd);
    testing.expectEqual(@as(usize, 1), co4.references);
    testing.expectEqual(@as(usize, 0), co4.recv_size);
    testing.expectEqual(@as(usize, 0), co4.send_offset);
    testing.expectEqual(@as(usize, 0), co4.send_size);
    try connections.unset(id4);
}
