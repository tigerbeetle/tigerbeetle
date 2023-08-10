const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const log = std.log.scoped(.io_benchmark);

const Time = @import("../time.zig").Time;
const IO = @import("../io.zig").IO;

// 1 MB: larger than socket buffer so forces io_pending on darwin
// Configure this value to smaller amounts to test IO scheduling overhead
const buffer_size = 1 * 1024 * 1024;

// max time for the benchmark to run
const run_duration = 1 * std.time.ns_per_s;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const leaks = gpa.deinit();
        assert(!leaks);
    }

    const buffer = try allocator.alloc(u8, buffer_size * 2);
    defer allocator.free(buffer);
    @memset(buffer, 0);

    var self = Context{
        .io = try IO.init(32, 0),
        .tx = .{ .buffer = buffer[0 * buffer_size ..][0..buffer_size] },
        .rx = .{ .buffer = buffer[1 * buffer_size ..][0..buffer_size] },
    };
    defer self.io.deinit();

    var timer = Time{};
    const started = timer.monotonic();
    defer {
        const elapsed_ns = timer.monotonic() - started;
        const transferred_mb = @as(f64, @floatFromInt(self.transferred)) / 1024 / 1024;

        log.info("took {}ms @ {d:.2} MB/s\n", .{
            elapsed_ns / std.time.ns_per_ms,
            transferred_mb / (@as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s),
        });
    }

    // Setup the server socket
    self.server.fd = try self.io.open_socket(os.AF.INET, os.SOCK.STREAM, os.IPPROTO.TCP);
    defer os.closeSocket(self.server.fd);

    const address = try std.net.Address.parseIp4("127.0.0.1", 3131);
    try os.setsockopt(
        self.server.fd,
        os.SOL.SOCKET,
        os.SO.REUSEADDR,
        &std.mem.toBytes(@as(c_int, 1)),
    );
    try os.bind(self.server.fd, &address.any, address.getOsSockLen());
    try os.listen(self.server.fd, 1);

    // Start accepting the client
    self.io.accept(
        *Context,
        &self,
        Context.on_accept,
        &self.server.completion,
        self.server.fd,
    );

    // Setup the client connection
    self.tx.socket.fd = try self.io.open_socket(os.AF.INET, os.SOCK.STREAM, os.IPPROTO.TCP);
    defer os.closeSocket(self.tx.socket.fd);

    self.io.connect(
        *Context,
        &self,
        Context.on_connect,
        &self.tx.socket.completion,
        self.tx.socket.fd,
        address,
    );

    // Run the IO loop for the duration of the benchmark
    log.info("running for {}", .{std.fmt.fmtDuration(run_duration)});
    try self.io.run_for_ns(run_duration);

    // Assert that everything is connected
    assert(self.server.fd != IO.INVALID_SOCKET);
    assert(self.tx.socket.fd != IO.INVALID_SOCKET);
    assert(self.rx.socket.fd != IO.INVALID_SOCKET);

    // Close the accepted client socket.
    // The actual client socket + server socket are closed by defer
    os.closeSocket(self.rx.socket.fd);
}

const Context = struct {
    io: IO,
    tx: Pipe,
    rx: Pipe,
    server: Socket = .{},
    transferred: u64 = 0,

    const Socket = struct {
        fd: os.socket_t = IO.INVALID_SOCKET,
        completion: IO.Completion = undefined,
    };
    const Pipe = struct {
        socket: Socket = .{},
        buffer: []u8,
        transferred: usize = 0,
    };

    fn on_accept(
        self: *Context,
        completion: *IO.Completion,
        result: IO.AcceptError!os.socket_t,
    ) void {
        assert(self.rx.socket.fd == IO.INVALID_SOCKET);
        assert(&self.server.completion == completion);
        self.rx.socket.fd = result catch |err| std.debug.panic("accept error {}", .{err});

        // Start reading data from the accepted client socket
        assert(self.rx.transferred == 0);
        self.do_transfer("rx", .read, 0);
    }

    fn on_connect(
        self: *Context,
        completion: *IO.Completion,
        result: IO.ConnectError!void,
    ) void {
        _ = result catch unreachable;

        assert(self.tx.socket.fd != IO.INVALID_SOCKET);
        assert(&self.tx.socket.completion == completion);

        // Start sending data to the server's accepted client
        assert(self.tx.transferred == 0);
        self.do_transfer("tx", .write, 0);
    }

    const TransferType = enum {
        read,
        write,
    };

    fn do_transfer(
        self: *Context,
        comptime pipe_name: []const u8,
        comptime transfer_type: TransferType,
        bytes: usize,
    ) void {
        // The type of IO to perform and what type of IO to perform next (after the current one completes).
        const transfer_info = switch (transfer_type) {
            .read => .{
                .IoError = IO.RecvError,
                .io_func = "recv",
                .next = TransferType.write,
            },
            .write => .{
                .IoError = IO.SendError,
                .io_func = "send",
                .next = TransferType.read,
            },
        };

        assert(bytes <= buffer_size);
        self.transferred += bytes;

        // Select which connection (tx or rx) depending on the type of transfer
        const pipe = &@field(self, pipe_name);
        pipe.transferred += bytes;
        assert(pipe.transferred <= pipe.buffer.len);

        // There's still more data to transfer on the connection
        if (pipe.transferred < pipe.buffer.len) {
            // Callback which calls this function again when data is transferred.
            // Effectively loops back above.
            const on_transfer = struct {
                fn on_transfer(
                    _self: *Context,
                    completion: *IO.Completion,
                    result: transfer_info.IoError!usize,
                ) void {
                    const _bytes = result catch |err| {
                        std.debug.panic("{s} error: {}", .{ transfer_info.io_func, err });
                    };
                    assert(&@field(_self, pipe_name).socket.completion == completion);
                    _self.do_transfer(pipe_name, transfer_type, _bytes);
                }
            }.on_transfer;

            // Perform the IO with the callback for the completion
            return @field(self.io, transfer_info.io_func)(
                *Context,
                self,
                on_transfer,
                &pipe.socket.completion,
                pipe.socket.fd,
                pipe.buffer[pipe.transferred..],
            );
        }

        // This transfer type completed transferring all the bytes.
        // Now, switch the transfer type (transfer_info.next).
        // This means if we read to the buffer, now we write it out.
        // Inversely, if we wrote the buffer, now we read it back.
        // This is basically a modified echo benchmark.
        pipe.transferred = 0;
        self.do_transfer(pipe_name, transfer_info.next, 0);
    }
};
