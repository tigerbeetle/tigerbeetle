const std = @import("std");
const stdx = @import("../stdx.zig");
const testing = std.testing;
const maybe = stdx.maybe;
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = @import("../message_pool.zig").MessagePool.Message;
const IOPS = @import("../iops.zig").IOPS;
const FIFO = @import("../fifo.zig").FIFO;
const log = std.log.scoped(.client);

pub fn Client(comptime StateMachine_: type, comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        pub const StateMachine = StateMachine_;
        pub const Request = vsr.ClientRequest;

        allocator: mem.Allocator,

        message_bus: MessageBus,

        /// A universally unique identifier for the client (must not be zero).
        /// Used for routing replies back to the client via any network path (multi-path routing).
        /// The client ID must be ephemeral and random per process, and never persisted, so that
        /// lingering or zombie deployment processes cannot break correctness and/or liveness.
        /// A cryptographic random number generator must be used to ensure these properties.
        id: u128,

        /// The identifier for the cluster that this client intends to communicate with.
        cluster: u128,

        /// The number of replicas in the cluster.
        replica_count: u8,

        /// The total number of ticks elapsed since the client was initialized.
        ticks: u64 = 0,

        /// We hash-chain request/reply checksums to verify linearizability within a client session:
        /// * so that the parent of the next request is the checksum of the latest reply, and
        /// * so that the parent of the next reply is the checksum of the latest request.
        parent: u128 = 0,

        /// The session number for the client, zero when registering a session, non-zero thereafter.
        session: u64 = 0,

        /// The request number of the next request.
        request_number: u32 = 0,

        /// The highest view number seen by the client in messages exchanged with the cluster.
        /// Used to locate the current primary, and provide more information to a partitioned primary.
        view: u32 = 0,

        /// Message of the `*Request` that is currently being processed by the message_bus.
        /// Corresponds to `request_queue.peek().?` and remains refererenced for duration of Client.
        message_inflight: *Message.Request,
        
        /// Memory for the firt Request used by register().
        request_register: Request = undefined,

        /// A client is allowed at most one inflight request at a time at the protocol layer.
        /// We therefore queue any further concurrent requests made by the application layer.
        request_queue: FIFO(Request) = .{ .name = "request_queue" },

        /// The number of ticks without a reply before the client resends the inflight request.
        /// Dynamically adjusted as a function of recent request round-trip time.
        request_timeout: vsr.Timeout,

        /// The number of ticks before the client broadcasts a ping to the cluster.
        /// Used for end-to-end keepalive, and to discover a new primary between requests.
        ping_timeout: vsr.Timeout,

        /// Used to calculate exponential backoff with random jitter.
        /// Seeded with the client's ID.
        prng: std.rand.DefaultPrng,

        on_reply_context: ?*anyopaque = null,
        /// Used for testing. Called for replies to all operations (including `register`).
        on_reply_callback: ?*const fn (
            client: *Self,
            request: *Message.Request,
            reply: *Message.Reply,
        ) void = null,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u128,
            replica_count: u8,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
        ) !Self {
            assert(id > 0);
            assert(replica_count > 0);

            var message_bus = try MessageBus.init(
                allocator,
                cluster,
                .{ .client = id },
                message_pool,
                Self.on_message,
                message_bus_options,
            );
            errdefer message_bus.deinit(allocator);

            // Reserve an inflight message upfront.
            const message_inflight = message_bus.get_message(.request);

            var self = Self{
                .allocator = allocator,
                .message_bus = message_bus,
                .id = id,
                .cluster = cluster,
                .replica_count = replica_count,
                .message_inflight = message_inflight,
                .request_timeout = .{
                    .name = "request_timeout",
                    .id = id,
                    .after = constants.rtt_ticks * constants.rtt_multiple,
                },
                .ping_timeout = .{
                    .name = "ping_timeout",
                    .id = id,
                    .after = 30000 / constants.tick_ms,
                },
                .prng = std.rand.DefaultPrng.init(@as(u64, @truncate(id))),
            };

            self.ping_timeout.start();

            return self;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.message_bus.unref(self.message_inflight.base());
            self.message_bus.deinit(allocator);
        }

        pub fn on_message(message_bus: *MessageBus, message: *Message) void {
            const self = @fieldParentPtr(Self, "message_bus", message_bus);
            log.debug("{}: on_message: {}", .{ self.id, message.header });
            if (message.header.invalid()) |reason| {
                log.debug("{}: on_message: invalid ({s})", .{ self.id, reason });
                return;
            }
            if (message.header.cluster != self.cluster) {
                log.warn("{}: on_message: wrong cluster (cluster should be {}, not {})", .{
                    self.id,
                    self.cluster,
                    message.header.cluster,
                });
                return;
            }
            switch (message.into_any()) {
                .pong_client => |m| self.on_pong_client(m),
                .reply => |m| self.on_reply(m),
                .eviction => |m| self.on_eviction(m),
                else => {
                    log.warn("{}: on_message: ignoring misdirected {s} message", .{
                        self.id,
                        @tagName(message.header.command),
                    });
                    return;
                },
            }
        }

        pub fn tick(self: *Self) void {
            self.ticks += 1;

            self.message_bus.tick();

            self.ping_timeout.tick();
            self.request_timeout.tick();

            if (self.ping_timeout.fired()) self.on_ping_timeout();
            if (self.request_timeout.fired()) self.on_request_timeout();
        }

        pub fn submit(
            self: *Self,
            callback: Request.Callback,
            request: *Request,
            operation: StateMachine.Operation,
            event_data: []const u8,
        ) void {
            assert(@intFromEnum(operation) >= constants.vsr_operations_reserved);
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(StateMachine.Event(operation_comptime)),
            };

            assert(event_data.len <= constants.message_body_size_max);
            assert(@divExact(event_data.len, event_size) > 0);

            request.* = .{
                .next = null,
                .operation = vsr.Operation.from(StateMachine, operation),
                .callback = callback,
                .body_ptr = event_data.ptr,
                .body_len = @intCast(event_data.len),
                .queue_total = @intCast(event_data.len),
                .queue = .{ .name = null },
            };

            // Check-in with the StateMachine to see if this operation should even be batched.
            // If so, find an existing Request with the same Op that has room in its Message.
            if (StateMachine.batch_logical_allowed.get(operation)) {
                var it = self.request_queue.peek();
                
                // The request must not be the one currently inflight in VSR as its Message is
                // being sent over the MessageBus and waiting for a reasponse.
                if (it) |inflight| {
                    it = inflight.next;
                }

                while (it) |inflight| {
                    it = inflight.next;

                    if (inflight.operation != request.operation) continue;
                    if (inflight.queue_total + request.body_len > constants.message_body_size_max) continue;

                    inflight.queue_total += request.body_len;
                    inflight.queue.push(request);
                    return;
                }
            }

            // Make sure to register before pushing a new Request to the request_queue.
            self.register();

            // Push the request as a root node and try to send it out.
            const was_empty = self.request_queue.empty();
            self.request_queue.push(request);
            if (was_empty) self.send_request_for_the_first_time(request);
        }
        
        fn on_eviction(self: *Self, eviction: *const Message.Eviction) void {
            assert(eviction.header.command == .eviction);
            assert(eviction.header.cluster == self.cluster);

            if (eviction.header.client != self.id) {
                log.warn("{}: on_eviction: ignoring (wrong client={})", .{
                    self.id,
                    eviction.header.client,
                });
                return;
            }

            if (eviction.header.view < self.view) {
                log.debug("{}: on_eviction: ignoring (older view={})", .{
                    self.id,
                    eviction.header.view,
                });
                return;
            }

            assert(eviction.header.client == self.id);
            assert(eviction.header.view >= self.view);

            log.err("{}: session evicted: too many concurrent client sessions", .{self.id});
            @panic("session evicted: too many concurrent client sessions");
        }

        fn on_pong_client(self: *Self, pong: *const Message.PongClient) void {
            assert(pong.header.command == .pong_client);
            assert(pong.header.cluster == self.cluster);

            if (pong.header.view > self.view) {
                log.debug("{}: on_pong: newer view={}..{}", .{
                    self.id,
                    self.view,
                    pong.header.view,
                });
                self.view = pong.header.view;
            }

            // Now that we know the view number, it's a good time to register if we haven't already:
            self.register();
        }

        fn on_reply(self: *Self, reply: *Message.Reply) void {
            // We check these checksums again here because this is the last time we get to downgrade
            // a correctness bug into a liveness bug, before we return data back to the application.
            assert(reply.header.valid_checksum());
            assert(reply.header.valid_checksum_body(reply.body()));
            assert(reply.header.command == .reply);

            if (reply.header.client != self.id) {
                log.debug("{}: on_reply: ignoring (wrong client={})", .{
                    self.id,
                    reply.header.client,
                });
                return;
            }

            const inflight = self.request_queue.peek() orelse {
                log.debug("{}: on_reply: ignoring (no inflight request)", .{self.id});
                return;
            };
            
            if (reply.header.request < self.message_inflight.header.request) {
                log.debug("{}: on_reply: ignoring (request {} < {})", .{
                    self.id,
                    reply.header.request,
                    self.message_inflight.header.request,
                });
                return;
            }

            if (self.on_reply_callback) |on_reply_callback| {
                on_reply_callback(self, self.message_inflight, reply);
            }

            log.debug("{}: on_reply: request={} size={} {s}", .{
                self.id,
                reply.header.request,
                reply.header.size,
                reply.header.operation.tag_name(StateMachine),
            });

            assert(reply.header.request_checksum == self.parent);
            assert(reply.header.client == self.id);
            assert(reply.header.request == self.message_inflight.header.request);
            assert(reply.header.cluster == self.cluster);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.operation == inflight.operation);

            // The context of this reply becomes the parent of our next request:
            self.parent = reply.header.context;

            if (reply.header.view > self.view) {
                log.debug("{}: on_reply: newer view={}..{}", .{
                    self.id,
                    self.view,
                    reply.header.view,
                });
                self.view = reply.header.view;
            }

            self.request_timeout.stop();

            if (inflight.operation == .register) {
                assert(self.session == 0);
                assert(reply.header.commit > 0);
                self.session = reply.header.commit; // The commit number becomes the session number.
            }

            // We must start the next request before releasing control back to the callback(s).
            // Otherwise, requests may never run through send_request_for_the_first_time().
            assert(inflight == self.request_queue.pop().?);
            if (self.request_queue.peek()) |request| {
                self.send_request_for_the_first_time(request);
            }

            // Ignore callback processing if the Request was from register().
            if (inflight.operation == .register) {
                assert(inflight.callback == null);
                assert(inflight.body_len == 0);
                assert(inflight.queue.empty());
                return;
            }

            // Push the root as first to its batched list to keep demux processing simpler.
            // Use a copy of `batched` list as `inflight` is invalidated on the first callback.
            var batched = inflight.queue;
            batched.push_front(inflight);

            assert(!inflight.operation.vsr_reserved());
            switch (inflight.operation.cast(StateMachine)) {
                inline else => |operation| {
                    var demuxer = StateMachine.DemuxerType(operation).init(
                        std.mem.bytesAsSlice(StateMachine.Result(operation), reply.body()),
                    );

                    var body_offset: u32 = 0;
                    while (batched.pop()) |request| {
                        assert(request.callback != null);
                        assert(request.operation.cast(StateMachine) == operation);
                        assert(request.body_len > 0);

                        const event_size = @sizeOf(StateMachine.Event(operation));
                        const event_count = @divExact(request.body_len, event_size);
                        const event_offset = @divExact(body_offset, event_size);
                        body_offset += request.body_len;

                        // Extract/use the Demux node info to slice the results from the reply.
                        const decoded = demuxer.decode(@intCast(event_offset), @intCast(event_count));
                        const response = std.mem.sliceAsBytes(decoded);
                        if (!StateMachine.batch_logical_allowed.get(operation)) {
                            assert(response.len == reply.body().len);
                        }

                        const callback = request.callback orelse unreachable;
                        callback(request, response);
                    }
                },
            }
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            const ping = Header.PingClient{
                .command = .ping_client,
                .cluster = self.cluster,
                .client = self.id,
            };

            // TODO If we haven't received a pong from a replica since our last ping, then back off.
            self.send_header_to_replicas(ping.frame_const().*);
        }

        fn on_request_timeout(self: *Self) void {
            self.request_timeout.backoff(self.prng.random());

            const message = self.message_inflight;
            assert(self.request_queue.peek() != null);

            assert(message.header.command == .request);
            assert(message.header.request < self.request_number);
            assert(message.header.checksum == self.parent);
            assert(message.header.session == self.session);

            log.debug("{}: on_request_timeout: resending request={} checksum={}", .{
                self.id,
                message.header.request,
                message.header.checksum,
            });

            // We assume the primary is down and round-robin through the cluster:
            self.send_message_to_replica(
                @as(u8, @intCast((self.view + self.request_timeout.attempts) % self.replica_count)),
                message.base(),
            );
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_message_from_header(self: *Self, header: Header) *Message {
            assert(header.cluster == self.cluster);
            assert(header.size == @sizeOf(Header));

            const message = self.message_bus.get_message(null);
            defer self.message_bus.unref(message);

            message.header.* = header;
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        /// Registers a session with the cluster for the client, if this has not yet been done.
        fn register(self: *Self) void {
            // Incremented by send_request_for_the_first_time() down below.
            if (self.request_number > 0) return;

            log.debug("{}: register: registering a session with the cluster", .{self.id});
            
            const request = &self.request_register;
            request.* = .{
                .next = null,
                .operation = .register,
                .callback = null,
                .body_ptr = undefined,
                .body_len = 0,
                .queue_total = 0,
                .queue = .{ .name = null },
            };

            assert(self.request_queue.empty());
            self.request_queue.push(request);
            self.send_request_for_the_first_time(request);
        }

        fn send_header_to_replica(self: *Self, replica: u8, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            self.send_message_to_replica(replica, message);
        }

        fn send_header_to_replicas(self: *Self, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                self.send_message_to_replica(replica, message);
            }
        }

        fn send_message_to_replica(self: *Self, replica: u8, message: *Message) void {
            log.debug("{}: sending {s} to replica {}: {}", .{
                self.id,
                @tagName(message.header.command),
                replica,
                message.header,
            });

            assert(replica < self.replica_count);
            assert(message.header.valid_checksum());
            assert(message.header.cluster == self.cluster);

            switch (message.into_any()) {
                inline .request,
                .ping_client,
                => |m| assert(m.header.client == self.id),
                else => unreachable,
            }

            self.message_bus.send_message_to_replica(replica, message);
        }

        fn send_request_for_the_first_time(self: *Self, request: *Request) void {
            assert(self.request_queue.peek() == request);

            // Reuse the single message allocated on init for the current inflight request.
            const message = self.message_inflight;
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = request.operation,
                .size = @sizeOf(Header),
            };

            // Bump the request number for each newly sent request.
            assert(self.request_number > 0 or request.operation == .register);
            self.request_number += 1;

            // First, copy the root Request's body into the message.
            assert(request.body_len > 0 or request.operation == .register);
            message.header.size += @intCast(request.body_len);
            stdx.copy_disjoint(.exact, u8, message.body(), request.body_ptr[0..request.body_len]);

            // Then, copy the body's of batched Requests into the message.
            var body_offset: usize = request.body_len;
            var it = request.queue.peek();
            while (it) |batched| {
                it = batched.next;

                const body = batched.body_ptr[0..batched.body_len];
                assert(body_offset == message.body().len);

                message.header.size += @intCast(body.len);
                assert(message.header.size <= constants.message_size_max);

                stdx.copy_disjoint(.exact, u8, message.body()[body_offset..], body);
                body_offset += body.len;
            }

            assert(message.body().len == request.queue_total);
            assert(message.header.size <= constants.message_size_max);

            log.debug("{}: request: request={} size={} {s}", .{
                self.id,
                message.header.request,
                message.header.size,
                message.header.operation.tag_name(StateMachine),
            });

            // We set the message checksums only when sending the request for the first time,
            // which is when we have the checksum of the latest reply available to set as `parent`,
            // and similarly also the session number if requests were queued while registering:
            message.header.parent = self.parent;
            message.header.session = self.session;
            // We also try to include our highest view number, so we wait until the request is ready
            // to be sent for the first time. However, beyond that, it is not necessary to update
            // the view number again, for example if it should change between now and resending.
            message.header.view = self.view;
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            // The checksum of this request becomes the parent of our next reply:
            self.parent = message.header.checksum;

            log.debug("{}: send_request_for_the_first_time: request={} checksum={}", .{
                self.id,
                message.header.request,
                message.header.checksum,
            });

            assert(!self.request_timeout.ticking);
            self.request_timeout.start();

            // If our view number is out of date, then the old primary will forward our request.
            // If the primary is offline, then our request timeout will fire and we will round-robin.
            self.send_message_to_replica(
                @as(u8, @intCast(self.view % self.replica_count)),
                message.base(),
            );
        }
    };
}

// Stub StateMachine which supports one batchable and non-batchable Operation for testing.
const TestStateMachine = struct {
    const config = constants.state_machine_config;

    pub const Operation = enum(u8) {
        batched = config.vsr_operations_reserved + 0,
        serial = config.vsr_operations_reserved + 1,
    };

    pub fn Event(comptime operation: Operation) type {
        return switch (operation) {
            .batched => [128]u8,
            .serial => u128,
        };
    }

    pub fn Result(comptime operation: Operation) type {
        return switch (operation) {
            .batched => u128,
            .serial => u128,
        };
    }

    pub const batch_logical_allowed = std.enums.EnumArray(Operation, bool).init(.{
        .batched = true,
        .serial = false,
    });

    // Demuxer which gives each Event a Result 1:1
    pub fn DemuxerType(comptime operation: Operation) type {
        return struct {
            const Demuxer = @This();
            const alignment = @alignOf(Result(operation));

            results: []Result(operation),
            offset: u32 = 0,

            pub fn init(reply: []Result(operation)) Demuxer {
                return .{ .results = reply };
            }

            pub fn decode(self: *Demuxer, event_offset: u32, event_count: u32) []Result(operation) {
                assert(self.offset == event_offset);
                assert(self.results[event_offset..].len >= event_count);

                // .batched uses consumes via the demux count passed in while .serial consumes all.
                const demuxed: u32 = if (batch_logical_allowed.get(operation))
                    event_count
                else
                    @intCast(self.results.len);

                defer self.offset += @intCast(demuxed);
                return self.results[event_offset..][0..demuxed];
            }
        };
    }
};

test "Result Demuxer" {
    const StateMachine = TestStateMachine;
    const Result = StateMachine.Result(.batched);

    var results: [@divExact(constants.message_body_size_max, @sizeOf(Result))]Result = undefined;
    for (0..results.len) |i| {
        results[i] = i;
    }

    var prng = std.rand.DefaultPrng.init(42);
    for (0..1000) |_| {
        const events_total = @max(1, prng.random().uintAtMost(usize, results.len));
        var demuxer = StateMachine.DemuxerType(.batched).init(results[0..events_total]);

        var events_offset: usize = 0;
        while (events_offset < events_total) {
            const events_limit = events_total - events_offset;
            const events_count = @max(1, prng.random().uintAtMost(usize, events_limit));

            const reply = demuxer.decode(@intCast(events_offset), @intCast(events_count));
            try testing.expectEqual(&reply[0], &results[events_offset]);
            try testing.expectEqual(reply.len, results[events_offset..][0..events_count].len);

            for (reply, 0..) |result, i| {
                try testing.expectEqual(result, @as(Result, events_offset + i));
            }

            events_offset += events_count;
        }
    }
}

test "Client Batching" {
    const StateMachine = TestStateMachine;

    // Stub MessageBus which simulates replica that provides matching Results for StateMachine.
    // This avoids pulling in ReplicaType and having to implement StateMachine.prefetch/commit/etc.
    const MessageBus = struct {
        const Self = @This();

        pub const Options = struct {};
        const MessageQueue = RingBuffer(*Message.Request, .{ .array = constants.client_request_queue_max });

        pool: *MessagePool,
        commit: u64 = 1,
        timestamp: u64 = 1,
        on_message: *const fn (message_bus: *Self, message: *Message) void,
        message_queue: MessageQueue = MessageQueue.init(),

        pub fn init(
            allocator: mem.Allocator,
            cluster: u128,
            process: struct { client: u128 },
            message_pool: *MessagePool,
            on_message: *const fn (message_bus: *Self, message: *Message) void,
            options: Options,
        ) !Self {
            _ = .{ allocator, cluster, process, options };
            return Self{
                .pool = message_pool,
                .on_message = on_message,
            };
        }

        pub fn deinit(bus: *Self, allocator: mem.Allocator) void {
            _ = allocator;
            while (bus.message_queue.pop()) |message| {
                bus.unref(message);
            }
        }

        pub fn get_message(
            bus: *Self,
            comptime command: ?vsr.Command,
        ) MessagePool.GetMessageType(command) {
            return bus.pool.get_message(command);
        }

        pub fn unref(bus: *Self, message: anytype) void {
            bus.pool.unref(message);
        }

        pub fn tick(bus: *Self) void {
            const message = bus.message_queue.pop() orelse @panic("done");
            defer bus.unref(message);

            const reply = bus.get_message(.reply);
            defer bus.unref(reply);

            // Figure out how many zeroes to write to reply.
            // StateMachine requests get an equal amount of empty Results per Event.
            const body_size = if (message.header.operation.vsr_reserved()) blk: {
                assert(message.body().len == 0);
                break :blk 0;
            } else switch (message.header.operation.cast(StateMachine)) {
                inline else => |operation| result_size: {
                    const event_count = @divExact(
                        message.body().len,
                        @sizeOf(StateMachine.Event(operation)),
                    );
                    const result_size = @sizeOf(StateMachine.Result(operation));
                    break :result_size event_count * result_size;
                },
            };

            bus.timestamp += 1;
            assert(bus.timestamp != 0);

            bus.commit += @intFromBool(bus.timestamp % constants.lsm_batch_multiple == 0);
            assert(bus.commit != 0);

            reply.header.* = .{
                .cluster = message.header.cluster,
                .size = @intCast(@sizeOf(Header) + body_size),
                .view = message.header.view,
                .command = .reply,
                .replica = message.header.replica,
                .request_checksum = message.header.checksum,
                .client = message.header.client,
                .context = undefined, // computed below.
                .op = bus.commit,
                .commit = bus.commit,
                .timestamp = bus.timestamp,
                .request = message.header.request,
                .operation = message.header.operation,
            };

            // Zero out the body and compute the checksum.
            @memset(reply.body(), 0);
            reply.header.set_checksum_body(reply.body());

            // See Replica.send_reply_message_to_client() why checksum is computed twice.
            reply.header.context = reply.header.calculate_checksum();
            reply.header.set_checksum();

            if (reply.header.invalid()) |header_err| @panic(header_err);
            bus.on_message(bus, reply.base());
        }

        pub fn send_message_to_replica(bus: *Self, replica: u32, message: *Message) void {
            _ = replica;
            bus.message_queue.push_assume_capacity(message.ref().build(.request));
        }
    };

    const VSRClient = Client(StateMachine, MessageBus);
    const allocator = std.testing.allocator;

    const Event = StateMachine.Event;
    const events_serial_max = @divExact(constants.message_body_size_max, @sizeOf(Event(.serial)));
    const events_batched_max = @divExact(constants.message_body_size_max, @sizeOf(Event(.batched)));

    // Submits batches to the VSRClient.
    const Context = struct {
        client: VSRClient,
        requests_created: u32 = 0,

        /// Tracks highest Submission.user_id which completed in its callback & number completed.
        var highest_user_id: u128 = 0;
        var requests_completed: usize = 0;

        const Request = struct {
            vsr_request: VSRClient.Request,
            user_id: u128,
        };

        const Submission = struct {
            operation: StateMachine.Operation,
            event_count: usize,
            user_id: u128,
            requests_created: u32,
        };

        fn submit(self: *@This(), comptime submission: Submission) !void {
            const events = switch (submission.operation) {
                inline else => |operation| blk: {
                    const event_size = @sizeOf(Event(operation));
                    const event_max = @divExact(constants.message_body_size_max, event_size);
                    break :blk std.mem.zeroes([event_max]Event(operation));
                },
            };

            const callback = struct {
                pub fn callback(
                    vsr_request: *VSRClient.Request,
                    reply: []const u8,
                ) void {
                    const request = @fieldParentPtr(Request, "vsr_request", vsr_request);
                    const operation = vsr_request.operation.cast(VSRClient.StateMachine);
                    const user_id = request.user_id;
                    allocator.destroy(request);

                    assert(operation == submission.operation);
                    assert(user_id == submission.user_id);
                    highest_user_id = @max(highest_user_id, user_id);
                    requests_completed += 1;

                    const result_size = @sizeOf(StateMachine.Result(submission.operation));
                    assert(reply.len == result_size * submission.event_count);
                }
            }.callback;

            const request = try allocator.create(Request);
            errdefer allocator.destroy(request);

            request.* = .{
                .vsr_request = undefined,
                .user_id = submission.user_id,
            };

            self.client.submit(
                callback,
                &request.vsr_request,
                submission.operation,
                std.mem.sliceAsBytes(events[0..submission.event_count]),
            );

            self.requests_created += submission.requests_created;
            assert(self.client.request_queue.count == self.requests_created);
        }
    };

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    var ctx = Context{ .client = try VSRClient.init(allocator, 1, 0, 1, &message_pool, .{}) };
    defer ctx.client.deinit(allocator);

    // Make sure no requests are created the start.
    comptime var last_user_id: u128 = 0;
    assert(ctx.client.request_queue.empty());

    // New request with op that is batchable, with one more slot until it fills up.
    // Ensure this enqueues TWO requests (one for register(), one for .batched).
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = events_batched_max - 1,
        .user_id = last_user_id,
        .requests_created = 2,
    });

    // New request with op that is NOT batchable.
    // Ensure it enqueues a new request, unrelated to the previous .batched.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .serial,
        .event_count = events_serial_max - 1,
        .user_id = last_user_id,
        .requests_created = 1,
    });

    // New request with a single batchable op.
    // Ensure no new requests as it should get merged with the previous batchable op to fill it.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .requests_created = 0,
    });

    // Another new request with a batchable op.
    // Ensure this creates a NEW request as the previous batch should've been filled up.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .requests_created = 1,
    });

    // Another new request with an op that's NOT batchable.
    // Ensure this creates a NEW request, not batched with previous .serial that had extra room.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .serial,
        .event_count = 1,
        .user_id = last_user_id,
        .requests_created = 1,
    });

    // Final new request with a batchable op.
    // Ensure this gets merged with the other .batched instead of creating a new request.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .requests_created = 0,
    });

    while (Context.requests_completed < last_user_id) ctx.client.tick();
    assert(Context.requests_completed == last_user_id);
    assert(Context.highest_user_id == last_user_id);
}
