const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Message = @import("../message_pool.zig").MessagePool.Message;
const parse_table = @import("../test/table.zig").parse;
const Cluster = @import("../test/cluster.zig").Cluster;

const cluster_id = 0;

```
    pub fn fault_reset(storage: *Storage) void {
        while (storage.faults.toggleFirstSet() != null) {}
    }

    pub fn fault_area(storage: *Storage, area: Area) void {
        var sectors = area.sectors();
        while (sectors.next()) |sector| storage.fault_sector(sector);
    }
```



// TODO Explicit "const tigerbeetle_config ="?
// TODO test client eviction
// TODO test primary permutations {R1,R2}
// TODO crash/restart
// TODO test "wait for replica to rejoin before repair finishes"
// TODO use Workload

test "recover: TEST" {
    try check(
        .{ .replica_count = 3 },
        // // TODO view column? log_view? (client primary?)
        // // replicas                                                  clients
        // // ^ status  prep-ok  commit/prepare                         request reply
        // //   0 1 2   0 1 2    0-A 0-B 0-C  1-A 1-B 1-C  2-A 2-B 2-C  A B C   A B C
        // \\ 0 ✓ ✓ %   F F F    0 1 0 1 0 1  0 1 0 1 0 1  0 0 0 0 0 0  1 1 1   0 0 0
        // \\ 2 % ✓ ✓   T T T    0 1 0 1 0 1  1 1 1 1 1 1  1 1 1 1 1 1  _ _ _   1 1 1
        // \\ 2 ✓ ✓ ✓   T T T    1 1 1 1 1 1  1 1 1 1 1 1  1 1 1 1 1 1  _ _ _   1 1 1

        // // replicas                                                  clients
        // // ^ status  prep-ok  commit/prepare                         request reply
        // //   0 1 2   0 1 2    0-A 0-B 0-C  1-A 1-B 1-C  2-A 2-B 2-C  A B C   A B C
        // \\ 0 ✓ ✓ %   F F F    0 1 0 1 0 1  0 1 0 1 0 1  0 0 0 0 0 0  1 1 1   0 0 0
        // \\ 2 % ✓ ✓   T T T    0 1 0 1 0 1  1 1 1 1 1 1  1 1 1 1 1 1  _ _ _   1 1 1
        // \\ 2 ✓ ✓ ✓   T T T    1 1 1 1 1 1  1 1 1 1 1 1  1 1 1 1 1 1  _ _ _   1 1 1

        \\ client requests 64
        \\ client replies 64

        //\\ replica 0 down
        //\\ replica 1 down
        //\\ replica 2 down

        //\\ replica 0 wal ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
        //\\ replica 1 wal ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

        //\\ replica 0 up
        //\\ replica 1 up

        \\ client requests 65
        \\ client replies 64

        //// replica                                               client
        ////   status  net-ack  commit                               request reply
        //// ^ 0 1 2   0 1 2    0:A 1:A 2:A 0:B 1:B 2:B 2:C 2:C 2:C  A B C   A B C
        //\\ 0 ✓ ✓ %   F F F    0/1 0/1 0/1 0/1 0/1 0/1 0/0 0/0 0/0  1 1 1   0 0 0
        //\\ 2 % ✓ ✓   _ _ _    0/1 0/1 0/1 1/1 1/1 1/1 1/1 1/1 1/1  _ _ _   1 1 1
        //\\ 2 ✓ ✓ ✓   _ _ _    1/1 1/1 1/1 1/1 1/1 1/1 1/1 1/1 1/1  _ _ _   1 1 1
    );
}

const Options = struct {
    replica_count: u8,
    client_count: u8 = constants.clients_max,
};

const Action = union(enum) {
    replica: struct {
        index: u8,
        action: union(enum) {
            up: void,
            down: void,
            wal: [constants.journal_slot_count]?enum { @"─", @"┴", @"┬", @"┼" },
        },
    },
    client: union(enum) {
        requests: usize,
        replies: usize,
    },
};

//fn SceneType(comptime options: SceneOptions) type {
//    const replica_count = options.replica_count;
//    const client_count = options.client_count;
//    return struct {
//        replica_primary: u8,
//        replica_status: [replica_count]ReplicaStatus,
//        replica_network_ack: [replica_count]bool,
//        replica_ops: [replica_count][client_count]struct {
//            prepare: u64,
//            commit: u64,
//        },
//        client_requests: [client_count]?u64,
//        client_replies: [client_count]u64,
//    };
//}

fn check(comptime options: Options, comptime table: []const u8) !void {
    const allocator = std.testing.allocator;
    const actions = parse_table(Action, table);

    var prng = std.rand.DefaultPrng.init(123);
    const random = prng.random();

    const cluster = try Cluster.init(allocator, Context.on_client_reply, .{
        .cluster_id = cluster_id,
        .replica_count = options.replica_count,
        .client_count = options.client_count,
        .storage_size_limit = vsr.sector_floor(
            constants.storage_size_max - random.uintLessThan(u64, constants.storage_size_max / 10),
        ),
        .seed = random.int(u64),
        .network = .{
            .replica_count = options.replica_count,
            .client_count = options.client_count,
            .seed = random.int(u64),
            .one_way_delay_mean = 3 + random.uintLessThan(u16, 10),
            .one_way_delay_min = random.uintLessThan(u16, 3),

            .path_maximum_capacity = 128,
            .path_clog_duration_mean = 0,
            .path_clog_probability = 0,
        },
        .storage = .{
            .read_latency_min = 1,
            .read_latency_mean = 5,
            .write_latency_min = 1,
            .write_latency_mean = 5,
        },
        .storage_fault_atlas = .{
            .faulty_superblock = false,
            .faulty_wal_headers = false,
            .faulty_wal_prepares = false,
        },
        .state_machine = .{},
    });
    defer cluster.deinit();

    var context = try Context.init(allocator, cluster);
    defer context.deinit(allocator);
    cluster.context = &context;

    var requests_target: usize = 0;
    var replies_target: usize = 0;

    for (actions.constSlice()) |action| {
        switch (action) {
            .replica => |data| switch (data.action) {
                .up => cluster.restart_replica(data.index),
                .down => assert(try cluster.crash_replica(data.index)),
                .wal => {},
                //: [constants.journal_slot_count]?enum { "@─", @"┴", @"┬", @"┼" },
            },
            .client => |data| switch (data) {
                .requests => |count| requests_target = count,
                .replies => |count| replies_target = count,
            },
        }

        std.debug.print("req/res {}/{}\n", .{requests_target,replies_target});

        assert(context.client_replies <= replies_target);
        if (context.client_replies == replies_target) continue;

        const tick_max = 10_000;
        var tick: usize = 0;
        while (tick < tick_max) : (tick += 1) {
            var requests_total: usize = 0;
            for (cluster.clients) |*client| requests_total += client.request_number;
            assert(requests_total <= requests_target);

            const replies_total = context.client_replies;
            //var replies_total: usize = 0;
            //for (context.client_replies) |replies| replies_total += replies;
            assert(replies_total <= replies_target);

            //std.debug.print("TOTAL: {},{}\n", .{requests_total,replies_total});

            if (requests_total == requests_target and
                replies_total == replies_target)
            {
                break;
            }

            for (cluster.clients) |*client, client_index| {
                if (requests_total < requests_target and client.request_queue.empty()) {
                    const message = client.get_message();
                    defer client.unref(message);

                    std.debug.print("REQUEST total={}\n", .{requests_total});
                    random.bytes(message.buffer[@sizeOf(vsr.Header)..][0..constants.message_body_size_max]);
                    cluster.request(client_index, .echo, message, constants.message_body_size_max);
                    break;
                }
            }

            cluster.tick();
        } else {
            @panic("test did not finish within tick_max");
        }
    }
}

const Context = struct {
    cluster: *Cluster,
    /// The number of replies received by each client.
    /// Includes command=register messages.
    //client_replies: []usize,
    client_replies: usize = 0,

    fn init(allocator: std.mem.Allocator, cluster: *Cluster) !Context {
        //var client_requests = try std.ArrayList([]const u8).init(allocator);
        //errdefer client_requests.deinit();
        _=allocator;

        //var client_replies = try allocator.alloc(usize, cluster.options.client_count);
        //errdefer client_replies.deinit();
        //std.mem.set(usize, client_replies, 0);

        return Context{
            .cluster = cluster,
            //.client_replies = client_replies,
        };
    }

    fn deinit(context: *Context, allocator: std.mem.Allocator) void {
    _=context;
    _=allocator;
        //context.client_requests.deinit();
        //allocator.free(context.client_replies);
    }

    //fn setup_primary(context: *Context, primary: u8) void {
    //    if (context.cluster.options.replica_count == 1) {
    //        assert(primary == 0);
    //        return;
    //    }

    //    var view: u32 = 0;
    //    for (context.cluster.replicas) |replica| view = std.math.max(view, replica.view);
    //    while (@intCast(u8, view % context.cluster.options.replica_count) != primary) view += 1;

    //    for (context.cluster.replicas) |*replica, i| {
    //        const message = replica.message_bus.get_message();
    //        defer replica.message_bus.unref(message);

    //        message.header.* = .{
    //            .size = @sizeOf(vsr.Header),
    //            .command = .start_view_change,
    //            .cluster = context.cluster.options.cluster_id,
    //            .replica = @intCast(u8, (i + 1) % context.cluster.options.replica_count),
    //            .view = view,
    //        };
    //        replica.on_message(message);
    //    }
    //}

    //fn request(context: *Context, label: []const u8) !void {
    //    const client_index = context.client_indexes[request[0]];
    //    const client = &context.cluster.clients[client_index];
    //    const message = client.get_message();
    //    defer client.unref(message);

    //    util.copy_disjoint(u8, message.body(), request);
    //    context.cluster.request(client_index, .echo, message, request.len);
    //                //context.request(request);

    //    for (context.client_requests) |label_existing| {
    //        if (std.mem.eql(u8, label, label_existing)) break;
    //    } else {
    //        try context.client_requests.append(label);
    //        try context.client_replies.resize(context.client_requests.len);
    //    }
    //}

    fn on_client_reply(
        cluster: *Cluster,
        client: usize,
        request: *Message,
        reply: *Message,
    ) void {
        _ = client;
        assert(reply.header.operation == request.header.operation);

        std.debug.print("ON_REPLY client={}\n", .{client});

        const context = @ptrCast(*Context, @alignCast(@alignOf(Context), cluster.context.?));
        //context.client_replies[client] += 1;
        context.client_replies += 1;
    }
};
