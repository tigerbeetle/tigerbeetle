const std = @import("std");

const vsr = @import("vsr");
const tb_client = vsr.tb_client;
const tb = vsr.tigerbeetle;
const constants = vsr.constants;

const Options = struct {
    addresses: []const u8 = "127.0.0.1:3001",
    cluster_id: u128 = 0,
    requests: u64 = 1_000_000,
    batch_size: u32 = 1,
    concurrency: u32 = 256,
    warmup: u32 = 1024,

    fn parse() !Options {
        var options: Options = .{};

        var args = std.process.args();
        _ = args.skip();

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--help")) {
                usage();
                std.process.exit(0);
            } else if (std.mem.startsWith(u8, arg, "--addresses=")) {
                options.addresses = arg["--addresses=".len..];
            } else if (std.mem.startsWith(u8, arg, "--cluster-id=")) {
                options.cluster_id = try std.fmt.parseInt(u128, arg["--cluster-id=".len..], 10);
            } else if (std.mem.startsWith(u8, arg, "--requests=")) {
                options.requests = try std.fmt.parseInt(u64, arg["--requests=".len..], 10);
            } else if (std.mem.startsWith(u8, arg, "--batch-size=")) {
                options.batch_size = try std.fmt.parseInt(u32, arg["--batch-size=".len..], 10);
            } else if (std.mem.startsWith(u8, arg, "--concurrency=")) {
                options.concurrency = try std.fmt.parseInt(u32, arg["--concurrency=".len..], 10);
            } else if (std.mem.startsWith(u8, arg, "--warmup=")) {
                options.warmup = try std.fmt.parseInt(u32, arg["--warmup=".len..], 10);
            } else {
                std.debug.print("unknown argument: {s}\n\n", .{arg});
                usage();
                return error.InvalidArgument;
            }
        }

        if (options.requests == 0) return error.InvalidArgument;
        if (options.batch_size == 0) return error.InvalidArgument;
        if (options.concurrency == 0) return error.InvalidArgument;

        const batch_size_max = tb.Operation.lookup_accounts.event_max(constants.message_body_size_max);
        if (options.batch_size > batch_size_max) {
            std.debug.print("batch size {} exceeds maximum {}\n", .{
                options.batch_size,
                batch_size_max,
            });
            return error.InvalidArgument;
        }

        return options;
    }

    fn usage() void {
        std.debug.print(
            \\usage: client_zig_bench [options]
            \\
            \\  --addresses=<addresses>   replica addresses (default: 127.0.0.1:3001)
            \\  --cluster-id=<id>         cluster id (default: 0)
            \\  --requests=<count>        measured requests (default: 1000000)
            \\  --batch-size=<count>      lookup ids per request (default: 1)
            \\  --concurrency=<count>     packets kept in flight (default: 256)
            \\  --warmup=<count>          warmup requests before timing (default: 1024)
            \\
        , .{});
    }
};

const CompletionState = struct {
    const AtomicU64 = std.atomic.Value(u64);
    const AtomicU8 = std.atomic.Value(u8);
    const no_failure = std.math.maxInt(u8);

    completed: AtomicU64,
    failed: AtomicU8,
    reply_bytes: AtomicU64,

    fn init() CompletionState {
        return .{
            .completed = AtomicU64.init(0),
            .failed = AtomicU8.init(no_failure),
            .reply_bytes = AtomicU64.init(0),
        };
    }

    fn complete(
        self: *CompletionState,
        status: tb_client.PacketStatus,
        reply_size: u32,
    ) void {
        if (status != .ok) {
            _ = self.failed.cmpxchgStrong(
                no_failure,
                @intFromEnum(status),
                .release,
                .monotonic,
            );
        }
        _ = self.reply_bytes.fetchAdd(reply_size, .monotonic);
        _ = self.completed.fetchAdd(1, .release);
    }

    fn wait_for_next(self: *CompletionState, completed_previous: u64) !void {
        var spins: u32 = 0;
        while (self.completed_count() == completed_previous) {
            if (self.failure()) |status| {
                std.debug.print("packet failed: status={}\n", .{status});
                return error.PacketFailed;
            }

            if (spins < 1024) {
                std.atomic.spinLoopHint();
                spins += 1;
            } else {
                std.Thread.yield() catch {};
            }
        }

        if (self.failure()) |status| {
            std.debug.print("packet failed: status={}\n", .{status});
            return error.PacketFailed;
        }
    }

    fn completed_count(self: *CompletionState) u64 {
        return self.completed.load(.acquire);
    }

    fn failure(self: *CompletionState) ?tb_client.PacketStatus {
        const status = self.failed.load(.acquire);
        if (status == no_failure) return null;
        return @enumFromInt(status);
    }

    fn reply_bytes_count(self: *CompletionState) u64 {
        return self.reply_bytes.load(.monotonic);
    }
};

const Request = struct {
    state: *CompletionState,
    packet: tb_client.Packet = undefined,
    ids: []u128,
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const options = try Options.parse();

    var interface: tb_client.ClientInterface = undefined;
    try tb_client.init(
        allocator,
        &interface,
        options.cluster_id,
        options.addresses,
        0,
        on_completion,
    );
    defer interface.deinit() catch unreachable;

    var state = CompletionState.init();
    const requests = try allocator.alloc(Request, options.concurrency);
    defer allocator.free(requests);

    const ids_buffer = try allocator.alloc(u128, @as(usize, options.concurrency) * options.batch_size);
    defer allocator.free(ids_buffer);

    var id: u128 = 1;
    for (requests, 0..) |*request, index| {
        const ids = ids_buffer[index * options.batch_size ..][0..options.batch_size];
        for (ids) |*id_slot| {
            id_slot.* = id;
            id += 1;
        }
        request.* = .{
            .state = &state,
            .ids = ids,
        };
    }

    try run_requests(&interface, requests, options.warmup);

    state = CompletionState.init();
    var timer = try std.time.Timer.start();
    try run_requests(&interface, requests, options.requests);
    const elapsed_ns = timer.read();

    const events = options.requests * options.batch_size;
    const requests_per_second = @as(f64, @floatFromInt(options.requests)) /
        (@as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s);
    const events_per_second = @as(f64, @floatFromInt(events)) /
        (@as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s);

    try stdout.print(
        \\addresses={s}
        \\requests={}
        \\batch_size={}
        \\concurrency={}
        \\elapsed_ns={}
        \\requests_per_second={d:.3}
        \\events_per_second={d:.3}
        \\reply_bytes={}
        \\
    , .{
        options.addresses,
        options.requests,
        options.batch_size,
        options.concurrency,
        elapsed_ns,
        requests_per_second,
        events_per_second,
        state.reply_bytes_count(),
    });
}

fn run_requests(
    interface: *tb_client.ClientInterface,
    requests: []Request,
    total: u64,
) !void {
    if (total == 0) return;

    const state = requests[0].state;
    var submitted: u64 = 0;

    while (state.completed_count() < total) {
        const completed = state.completed_count();
        while (submitted < total and submitted - completed < requests.len) {
            const request = &requests[@intCast(submitted % requests.len)];
            request.packet = std.mem.zeroes(tb_client.Packet);
            request.packet.user_data = request;
            request.packet.operation = @intFromEnum(tb.Operation.lookup_accounts);
            request.packet.data = std.mem.sliceAsBytes(request.ids).ptr;
            request.packet.data_size = @intCast(std.mem.sliceAsBytes(request.ids).len);

            try interface.submit(&request.packet);
            submitted += 1;
        }

        if (state.completed_count() < total) {
            try state.wait_for_next(completed);
        }
    }
}

fn on_completion(
    completion_ctx: usize,
    packet: *tb_client.Packet,
    timestamp: u64,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.c) void {
    _ = completion_ctx;
    _ = result_ptr;
    _ = timestamp;

    const request: *Request = @ptrCast(@alignCast(packet.user_data.?));
    if (packet.status == .ok) {
        std.debug.assert(result_len % @sizeOf(tb.Account) == 0);
        std.debug.assert(result_len <= request.ids.len * @sizeOf(tb.Account));
    } else {
        std.debug.assert(result_len == 0);
    }

    request.state.complete(packet.status, result_len);
}
