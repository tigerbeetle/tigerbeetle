const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.test_client);
const expectEqual = std.testing.expectEqual;

const vsr = @import("vsr");
const tigerbeetle = @import("../../tigerbeetle.zig");
const Storage = vsr.storage.Storage;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const MessageBusClient = vsr.message_bus.MessageBusClient;
const MessageBusReplica = vsr.message_bus.MessageBusReplica;
const IO = vsr.io.IO;
const constants = vsr.constants;

const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBusClient);
const Operation = StateMachine.Operation;
const Account = tigerbeetle.Account;
const Transfer = tigerbeetle.Transfer;
const Message = vsr.message_pool.MessagePool.Message;
const MessagePool = vsr.message_pool.MessagePool;

const ticks_max = 50_000_000;

pub const std_options = struct {
    pub const log_level: std.log.Level = .info;
};

pub fn main() anyerror!void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = general_purpose_allocator.allocator();

    // CLI arguments.

    const cluster_arg = std.os.getenv("CLUSTER") orelse fatal("missing CLUSTER", .{});
    const cluster_addresses_arg = std.os.getenv("REPLICAS") orelse fatal("missing REPLICAS", .{});
    const client_host_arg = std.os.getenv("HOST") orelse fatal("missing HOST", .{});
    const client_port_arg = std.os.getenv("PORT") orelse fatal("missing PORT", .{});

    const cluster = try std.fmt.parseUnsigned(u32, cluster_arg, 10);
    const client_port = try std.fmt.parseUnsigned(u16, client_port_arg, 10);
    const client_address = std.net.Address.parseIp(client_host_arg, client_port) catch {
        fatal("invalid client address host={s} port={}", .{ client_host_arg, client_port });
    };

    const cluster_addresses = try vsr.parse_addresses(allocator, cluster_addresses_arg, constants.replicas_max);
    defer allocator.free(cluster_addresses);

    // Set up the client.

    const client_id = std.crypto.random.int(u128);
    var io = try IO.init(32, 0);
    defer io.deinit();

    var workload_message_pool = try MessagePool.init(allocator, .replica);
    defer workload_message_pool.deinit(allocator);

    var cluster_message_pool = try MessagePool.init(allocator, .client);
    defer cluster_message_pool.deinit(allocator);

    var context = Context{
        .cluster = cluster,
        .cluster_client = try Client.init(
            allocator,
            client_id,
            cluster,
            @as(u8, @intCast(cluster_addresses.len)),
            &cluster_message_pool,
            .{
                .configuration = cluster_addresses,
                .io = &io,
            },
        ),
        .workload_message_bus = try MessageBusReplica.init(
            allocator,
            cluster,
            .{ .replica = 0 },
            &workload_message_pool,
            Context.on_request_from_workload,
            .{
                .configuration = &[_]std.net.Address{client_address},
                .io = &io,
            },
        ),
    };
    defer context.cluster_client.deinit(allocator);
    defer context.workload_message_bus.deinit(allocator);

    // Relay requests from the workload to the cluster.
    // Relay responses from the cluster to the workload.
    // This loop doesn't have an exit condition in the typical case —
    // the API's process will be killed by Antithesis after the workload completes.
    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        context.workload_message_bus.tick();
        context.cluster_client.tick();
        io.tick() catch |err| {
            log.warn("io.tick error={s}", .{@errorName(err)});
        };
        std.time.sleep(constants.tick_ms * std.time.ns_per_ms);
    } else fatal("tick == ticks_max", .{});
}

const Context = struct {
    cluster: u32,
    cluster_client: Client,
    workload_message_bus: MessageBusReplica,

    fn send_reply_to_workload(context: *Context, request: *Message, body: []const u8) void {
        assert(request.header.command == .request);
        assert(request.header.client != context.cluster_client.id);
        assert(@sizeOf(Header) + body.len <= constants.message_size_max);

        var reply = context.workload_message_bus.get_message();
        defer context.workload_message_bus.unref(reply);

        std.mem.copy(u8, reply.buffer[@sizeOf(vsr.Header)..], body);

        reply.header.* = .{
            .parent = request.header.checksum,
            .client = request.header.client,
            .request = request.header.request,
            .cluster = context.cluster,
            // `op` and `commit` must match and be nonzero to pass vsr.Header's validation,
            // but are otherwise ignored by the workload.
            .op = 1,
            .commit = 1,
            .timestamp = 1,
            .size = @as(u32, @intCast(@sizeOf(Header) + body.len)),
            .command = .reply,
            .operation = request.header.operation,
        };
        reply.header.set_checksum_body(reply.body());
        reply.header.set_checksum();
        assert(reply.header.invalid() == null);

        log.info("send_reply_to_workload request={} operation={}", .{
            request.header.request,
            request.header.operation,
        });
        context.workload_message_bus.send_message_to_client(request.header.client, reply);
    }

    fn on_request_from_workload(message_bus: *MessageBusReplica, request: *Message) void {
        var context = @fieldParentPtr(Context, "workload_message_bus", message_bus);
        switch (request.header.command) {
            .ping_client => {
                const pong = context.cluster_client.get_message();
                defer context.cluster_client.unref(pong);

                pong.header.* = .{
                    .parent = request.header.checksum,
                    .client = request.header.client,
                    .request = request.header.request,
                    .cluster = context.cluster,
                    .command = .pong,
                };
                pong.header.set_checksum_body(pong.body());
                pong.header.set_checksum();
                context.workload_message_bus.send_message_to_client(request.header.client, pong);
                return;
            },
            .pong => return,
            .request => {}, // fall through
            else => unreachable,
        }

        assert(request.header.command == .request);
        assert(request.header.operation != .root);

        // The `register` message lets the workload `MessageBus` identify the peer type.
        if (request.header.operation == .register) {
            context.send_reply_to_workload(request, &[_]u8{});
            return;
        }

        log.info("on_request_from_workload operation={}", .{
            request.header.operation,
        });

        if (context.cluster_client.request_queue.empty()) {
            const message_out = context.cluster_client.get_message();
            defer context.cluster_client.unref(message_out);

            std.mem.copy(u8, message_out.buffer[@sizeOf(vsr.Header)..], request.body());
            context.cluster_client.request(
                std.mem.bytesToValue(u128, &std.mem.toBytes(RequestContext{
                    .context = context,
                    .prepare = request.ref(),
                })),
                client_request_callback,
                request.header.operation.cast(StateMachine),
                message_out,
                request.header.size - @sizeOf(Header),
            );
        } else {
            // At most the request includes a `.register` and a `.request`.
            assert(context.cluster_client.request_queue.count <= 2);
        }
    }
};

const RequestContext = struct {
    context: *Context,
    prepare: *Message,

    // `RequestContext` is cast into a `u128` as `Client.request`'s context.
    comptime {
        assert(@sizeOf(RequestContext) == @sizeOf(u128));
    }
};

fn client_request_callback(user_data: u128, operation: Operation, body: []const u8) void {
    const request_context = std.mem.bytesToValue(RequestContext, &std.mem.toBytes(user_data));
    const context = request_context.context;
    const prepare = request_context.prepare;
    defer context.workload_message_bus.unref(prepare);

    assert(prepare.header.command == .request);
    assert(prepare.header.operation.cast(StateMachine) == operation);

    const reply = context.workload_message_bus.get_message();
    defer context.workload_message_bus.unref(reply);

    context.send_reply_to_workload(prepare, body);
}

fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.os.exit(1);
}
