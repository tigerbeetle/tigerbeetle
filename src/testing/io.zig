const std = @import("std");
const os = std.os;
const posix = std.posix;
const mem = std.mem;
const assert = std.debug.assert;
const log = std.log.scoped(.io);
const Allocator = std.mem.Allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const FIFO = @import("../fifo.zig").FIFO;
const DirectIO = @import("../io.zig").DirectIO;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

// NOTE: Naming for consistency:
// - fd/sock                 : fd_t/socket_t
// - file_index/socket_index : Descriptor.Index
// - file/socket             : File/Socket

/// A very simple mock IO implementation that only implements what is needed to test Storage.
pub const IO = struct {
    pub const tag = .mock;

    /// Required so `close` can work on both files and sockets.
    ///
    /// Terminology:
    /// - Index: An index into either `files`/`sockets`.
    /// - TaggedIndex: An index into `files`/`sockets` based on the active tag.
    /// - Descriptor: "Opaque" handle to a file/socket that represents a TaggedIndex.
    pub const Descriptor = enum(Index) {
        pub const Index = u32;

        pub const TaggedIndex = union(enum) {
            file: Index,
            socket: Index,
        };

        file_start = 0,
        socket_start = std.math.maxInt(Index) / 2,
        invalid = std.math.maxInt(Index),
        _,

        pub fn from_tagged_index(index: TaggedIndex) Descriptor {
            switch (index) {
                .file => |file| {
                    assert(file < @intFromEnum(Descriptor.socket_start));
                    return @enumFromInt(file);
                },
                .socket => |socket| {
                    assert(socket + @intFromEnum(Descriptor.socket_start) <
                        @intFromEnum(Descriptor.invalid));
                    return @enumFromInt(socket + @intFromEnum(Descriptor.socket_start));
                },
            }
        }

        pub fn to_tagged_index(descriptor: Descriptor) TaggedIndex {
            return switch (@intFromEnum(descriptor)) {
                @intFromEnum(Descriptor.file_start)...@intFromEnum(Descriptor.socket_start) - 1,
                => |file| .{ .file = file },

                @intFromEnum(Descriptor.socket_start)...@intFromEnum(Descriptor.invalid) - 1,
                => |socket| .{ .socket = socket - @intFromEnum(Descriptor.socket_start) },

                @intFromEnum(Descriptor.invalid) => unreachable,
            };
        }
    };

    pub const fd_t = Descriptor;
    pub const INVALID_FILE: fd_t = .invalid;

    pub const socket_t = Descriptor;
    pub const socklen_t = u32;
    pub const INVALID_SOCKET: socket_t = .invalid;

    pub const File = struct {
        buffer: []u8,
        /// Each bit of the fault map represents a sector that will fault consistently.
        fault_map: ?[]const u8,
    };

    /// Represents a TCP socket.
    ///
    /// A TCP socket can be:
    /// - A newly created socket (nothing)
    /// - A newly created, bound socket (local address)
    /// - Bound to a local address and listening ("passive") (local address, accept queue)
    /// - Connected from a local address to a remote address via connect or accept (pair, buffers)
    pub const Socket = union(enum) {
        empty,
        bound: struct {
            address_local: std.net.Address,
        },
        listening: struct {
            address_local: std.net.Address,
            accept_queue: RingBuffer(Descriptor.Index, .slice),
        },
        connected: struct {
            address_local: std.net.Address,
            socket_remote_index: Descriptor.Index,
            buffer_receive: RingBuffer(u8, .slice),
        },

        /// Caller must assert socket is not empty.
        fn address_local(socket: Socket) std.net.Address {
            return switch (socket) {
                .empty => unreachable,
                inline else => |sock| sock.address_local,
            };
        }

        fn free_and_empty(socket: *Socket, allocator: Allocator) void {
            switch (socket.*) {
                .empty, .bound => {},
                .listening => |*listening| {
                    listening.accept_queue.deinit(allocator);
                },
                .connected => |*connected| {
                    connected.buffer_receive.deinit(allocator);
                },
            }

            socket.* = .empty;
        }
    };

    /// Options for fault injection during fuzz testing.
    pub const Options = struct {
        /// Seed for the storage PRNG.
        seed: u64 = 0,

        /// Chance out of 100 that a read larger than a logical sector
        /// will return an error.InputOutput.
        larger_than_logical_sector_read_fault_probability: u8 = 0,
    };

    const AddressContext = struct {
        pub fn hash(context: AddressContext, address: std.net.Address) u64 {
            _ = context;

            var hasher = std.hash.Wyhash.init(0);

            hasher.update(&std.mem.toBytes(address.any.family));
            hasher.update(&address.any.data);

            return hasher.final();
        }

        pub fn eql(context: AddressContext, a: std.net.Address, b: std.net.Address) bool {
            _ = context;
            return a.eql(b);
        }
    };

    allocator: Allocator,
    files: []const File,
    options: Options,
    prng: std.rand.DefaultPrng,

    sockets: std.ArrayListUnmanaged(Socket) = .{},
    sockets_listening: std.HashMapUnmanaged(
        std.net.Address,
        Descriptor.Index,
        AddressContext,
        std.hash_map.default_max_load_percentage,
    ) = .{},
    sockets_free_list: std.ArrayListUnmanaged(Descriptor.Index) = .{},

    next_bind_port: u16 = 4500,
    next_connect_port: u16 = 25000,

    completed: FIFO(Completion) = .{ .name = "io_completed" },

    pub fn init(
        allocator: Allocator,
        files: []const File,
        options: Options,
    ) IO {
        return .{
            .allocator = allocator,
            .options = options,
            .prng = std.rand.DefaultPrng.init(options.seed),
            .files = files,
        };
    }

    pub fn deinit(io: *IO) void {
        for (io.sockets.items) |socket| {
            socket.free_and_empty(io.allocator);
        }
        io.sockets.deinit(io.allocator);
        io.sockets_free_list.deinit(io.allocator);
        io.* = undefined;
    }

    pub fn tick(io: *IO) !void {
        var completed = io.completed;
        io.completed.reset();
        while (completed.pop()) |completion| {
            completion.callback(io, completion);
        }
    }

    /// This struct holds the data needed for a single IO operation.
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*anyopaque,
        callback: *const fn (*IO, *Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        read: struct {
            file_index: Descriptor.Index,
            buffer: []u8,
            offset: u64,
        },
        write: struct {
            file_index: Descriptor.Index,
            buffer: []const u8,
            offset: u64,
        },
        accept: struct {
            socket_index: Descriptor.Index,
        },
        send: struct {
            socket_index: Descriptor.Index,
            buffer: []const u8,
        },
        recv: struct {
            socket_index: Descriptor.Index,
            buffer: []u8,
        },
        timeout,
        connect: struct {
            socket_index: Descriptor.Index,
            address: std.net.Address,
        },
    };

    /// Return true with probability x/100.
    fn x_in_100(io: *IO, x: u8) bool {
        assert(x <= 100);
        return x > io.prng.random().uintLessThan(u8, 100);
    }

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: anytype,
        comptime OperationImpl: type,
    ) void {
        std.log.info("submitted {s}", .{@tagName(operation_tag)});

        const on_complete_fn = struct {
            fn on_complete(io: *IO, _completion: *Completion) void {
                // Perform the actual operation.
                const op_data = &@field(_completion.operation, @tagName(operation_tag));
                const result = OperationImpl.do_operation(io, op_data);

                // Requeue if error.WouldBlock
                switch (operation_tag) {
                    .accept => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => {
                                _completion.next = null;
                                io.completed.push(_completion);
                                return;
                            },
                            else => {},
                        };
                    },
                    else => {},
                }

                // Complete the Completion.
                return callback(
                    @ptrCast(@alignCast(_completion.context)),
                    _completion,
                    result,
                );
            }
        }.on_complete;

        completion.* = .{
            .next = null,
            .context = context,
            .callback = on_complete_fn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        self.completed.push(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
    } || posix.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        const file_index = fd.to_tagged_index().file;
        assert(file_index < self.files.len);

        self.submit(
            context,
            callback,
            completion,
            .read,
            .{
                .file_index = file_index,
                .buffer = buffer,
                .offset = offset,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) ReadError!usize {
                    const sector_marked_in_fault_map =
                        if (io.files[op.file_index].fault_map) |fault_map|
                        std.mem.readPackedIntNative(
                            u1,
                            fault_map,
                            @divExact(op.offset, constants.sector_size),
                        ) != 0
                    else
                        false;

                    const sector_has_larger_than_logical_sector_read_fault =
                        (op.buffer.len > constants.sector_size and
                        io.x_in_100(io.options.larger_than_logical_sector_read_fault_probability));

                    if (sector_marked_in_fault_map or
                        sector_has_larger_than_logical_sector_read_fault)
                    {
                        return error.InputOutput;
                    }

                    const data = io.files[op.file_index].buffer;
                    stdx.copy_disjoint(.exact, u8, op.buffer, data[op.offset..][0..op.buffer.len]);
                    return op.buffer.len;
                }
            },
        );
    }

    pub const WriteError = posix.PWriteError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        const file_index = fd.to_tagged_index().file;
        assert(file_index < self.files.len);

        self.submit(
            context,
            callback,
            completion,
            .write,
            .{
                .file_index = file_index,
                .buffer = buffer,
                .offset = offset,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) WriteError!usize {
                    const data = io.files[op.file_index].buffer;
                    if (op.offset + op.buffer.len >= data.len) {
                        @panic("write beyond simulated file size");
                    }
                    stdx.copy_disjoint(.exact, u8, data[op.offset..][0..op.buffer.len], op.buffer);
                    return op.buffer.len;
                }
            },
        );
    }

    const SetSockOptError = error{};
    pub fn setsockopt(
        io: *IO,
        sock: socket_t,
        level: i32,
        optname: u32,
        opt: []const u8,
    ) SetSockOptError!void {
        _ = sock; // autofix
        _ = io; // autofix
        _ = level; // autofix
        _ = optname; // autofix
        _ = opt; // autofix
    }

    /// Creates a socket that can be used for async operations with the IO instance.
    pub fn open_socket(io: *IO, family: u32, sock_type: u32, protocol: u32) !socket_t {
        assert(family == posix.AF.INET or family == posix.AF.INET6);
        assert(sock_type == posix.SOCK.STREAM);
        assert(protocol == posix.IPPROTO.TCP);

        const next_socket_index = try io.create_socket();
        io.sockets.items[next_socket_index] = .empty;

        return Descriptor.from_tagged_index(.{ .socket = @intCast(next_socket_index) });
    }

    fn create_socket(io: *IO) !Descriptor.Index {
        if (io.sockets_free_list.popOrNull()) |reused| {
            return reused;
        } else {
            _ = try io.sockets.addOne(io.allocator);
            return @intCast(io.sockets.items.len - 1);
        }
    }

    /// Closes a socket opened by the IO instance.
    pub fn close_socket(io: *IO, sock: socket_t) void {
        const socket_index = sock.to_tagged_index().socket;

        io.sockets.items[socket_index].free_and_empty(io.allocator);

        // TODO: How should this be handled? This is required to maintain
        // current our `errdefer close_socket`s.
        io.sockets_free_list.append(io.allocator, socket_index) catch unreachable;
    }

    pub fn getsockname(
        io: *IO,
        sock: socket_t,
        addr: *posix.sockaddr,
        addrlen: *socklen_t,
    ) posix.GetSockNameError!void {
        const socket_index = sock.to_tagged_index().socket;

        const socket = io.sockets.items[socket_index];
        assert(socket != .empty);

        const address = socket.address_local();
        addr.* = address.any;
        addrlen.* = address.getOsSockLen();
    }

    pub fn bind(
        io: *IO,
        sock: socket_t,
        addr: *const posix.sockaddr,
        len: socklen_t,
    ) posix.BindError!void {
        _ = len;
        const socket_index = sock.to_tagged_index().socket;

        var address = std.net.Address.initPosix(@alignCast(addr));
        if (address.getPort() == 0) {
            address.setPort(io.next_bind_port);
            io.next_bind_port += 1;
        }

        const socket = &io.sockets.items[socket_index];
        socket.free_and_empty(io.allocator);
        socket.* = .{
            .bound = .{
                .address_local = address,
            },
        };
    }

    pub const ListenError = error{OutOfMemory};
    pub fn listen(io: *IO, sock: socket_t, backlog: u31) ListenError!void {
        const socket_index = sock.to_tagged_index().socket;

        const socket = &io.sockets.items[socket_index];
        assert(socket.* == .bound);
        socket.* = .{
            .listening = .{
                .address_local = socket.bound.address_local,
                .accept_queue = try RingBuffer(Descriptor.Index, .slice).init(
                    io.allocator,
                    backlog + 1,
                ),
            },
        };

        try io.sockets_listening.put(io.allocator, socket.listening.address_local, socket_index);
    }

    pub const AcceptError = error{ WouldBlock, OutOfMemory };
    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!socket_t,
        ) void,
        completion: *Completion,
        sock: socket_t,
    ) void {
        const socket_index = sock.to_tagged_index().socket;

        self.submit(
            context,
            callback,
            completion,
            .accept,
            .{
                .socket_index = socket_index,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) AcceptError!socket_t {
                    const socket_listening = &io.sockets.items[op.socket_index];
                    assert(socket_listening.* == .listening);

                    const socket_local_index = socket_listening.listening.accept_queue.pop() orelse
                        return error.WouldBlock;

                    return Descriptor.from_tagged_index(.{
                        .socket = socket_local_index,
                    });
                }
            },
        );
    }

    pub const SendError = error{};
    pub fn send(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        sock: socket_t,
        buffer: []const u8,
    ) void {
        const socket_index = sock.to_tagged_index().socket;

        self.submit(
            context,
            callback,
            completion,
            .send,
            .{
                .socket_index = socket_index,
                .buffer = buffer,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) SendError!usize {
                    const socket_local = io.sockets.items[op.socket_index];
                    assert(socket_local == .connected);

                    const socket_remote_index = socket_local.connected.socket_remote_index;

                    const socket_remote = &io.sockets.items[socket_remote_index];
                    socket_remote.connected.buffer_receive.push_slice(op.buffer) catch return 0;

                    return op.buffer.len;
                }
            },
        );
    }

    pub const RecvError = error{};
    pub fn recv(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        sock: socket_t,
        buffer: []u8,
    ) void {
        const socket_index = sock.to_tagged_index().socket;

        self.submit(
            context,
            callback,
            completion,
            .recv,
            .{
                .socket_index = socket_index,
                .buffer = buffer,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) SendError!usize {
                    const socket_local = &io.sockets.items[op.socket_index];
                    assert(socket_local.* == .connected);

                    return socket_local.connected.buffer_receive.pop_slice(op.buffer).len;
                }
            },
        );
    }

    pub const CloseError = error{};
    pub fn close(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        _ = self; // autofix
        _ = context; // autofix
        _ = callback; // autofix
        _ = completion; // autofix
        _ = fd; // autofix
    }

    pub const TimeoutError = error{};

    /// `timeout` is allowed to callback early so this implementation calls back instantly.
    pub fn timeout(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        _ = nanoseconds;
        self.submit(
            context,
            callback,
            completion,
            .timeout,
            {},
            struct {
                fn do_operation(io: *IO, op: anytype) TimeoutError!void {
                    _ = io;
                    _ = op;
                }
            },
        );
    }

    pub const ConnectError = error{ ConnectionRefused, OutOfMemory };
    pub fn connect(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        sock: socket_t,
        address: std.net.Address,
    ) void {
        assert(address.any.family == posix.AF.INET or address.any.family == posix.AF.INET6);

        const socket_index = sock.to_tagged_index().socket;

        self.submit(
            context,
            callback,
            completion,
            .connect,
            .{
                .socket_index = socket_index,
                .address = address,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) ConnectError!void {
                    const socket_listening_index = io.sockets_listening.get(op.address) orelse
                        return error.ConnectionRefused;
                    const accept_queue =
                        &io.sockets.items[socket_listening_index].listening.accept_queue;

                    if (accept_queue.full()) {
                        return error.ConnectionRefused;
                    }

                    const socket_local_index = op.socket_index;
                    const socket_remote_index = try io.create_socket();

                    const socket_local = &io.sockets.items[socket_local_index];
                    const socket_remote = &io.sockets.items[socket_remote_index];

                    const address_local = switch (op.address.any.family) {
                        posix.AF.INET => std.net.Address.initIp4(
                            .{ 127, 0, 0, 1 },
                            io.next_connect_port,
                        ),
                        posix.AF.INET6 => std.net.Address.initIp6(
                            .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
                            io.next_connect_port,
                            0,
                            0,
                        ),
                        else => unreachable,
                    };
                    io.next_connect_port += 1;

                    var address_remote = op.address;
                    address_remote.setPort(io.next_connect_port);
                    io.next_connect_port += 1;

                    socket_local.* = .{
                        .connected = .{
                            .address_local = address_local,
                            .socket_remote_index = socket_remote_index,
                            .buffer_receive = try RingBuffer(u8, .slice).init(io.allocator, 1024),
                        },
                    };

                    socket_remote.* = .{
                        .connected = .{
                            .address_local = address_remote,
                            .socket_remote_index = socket_local_index,
                            .buffer_receive = try RingBuffer(u8, .slice).init(io.allocator, 1024),
                        },
                    };

                    accept_queue.push(socket_remote_index) catch unreachable;
                }
            },
        );
    }
};
