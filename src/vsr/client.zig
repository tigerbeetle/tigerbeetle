const std = @import("std");
const stdx = @import("../stdx.zig");
const testing = std.testing;
const maybe = stdx.maybe;
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = @import("../message_pool.zig").MessagePool.Message;
const IOPS = @import("../iops.zig").IOPS;
const FIFO = @import("../fifo.zig").FIFO;

const log = std.log.scoped(.client);

pub fn Client(comptime StateMachine_: type, comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        pub const StateMachine = StateMachine_;

        pub const Request = struct {
            pub const Callback = *const fn (
                user_data: u128,
                operation: StateMachine.Operation,
                results: []const u8,
            ) void;

            message: *Message.Request,
            user_data: u128,
            callback: Callback,
        };

        /// Custom Demuxer which passes through to StateMachine.Demuxer.
        pub fn DemuxerType(comptime operation: StateMachine.Operation) type {
            return struct {
                const Demuxer = @This();
                const DemuxerBase = StateMachine.DemuxerType(operation);

                base: DemuxerBase,

                pub fn init(reply: []u8) Demuxer {
                    const results = std.mem.bytesAsSlice(StateMachine.Result(operation), reply);
                    return Demuxer{ .base = DemuxerBase.init(@alignCast(results)) };
                }

                pub fn decode(self: *Demuxer, event_offset: u32, event_count: u32) []u8 {
                    const results = self.base.decode(event_offset, event_count);
                    return std.mem.sliceAsBytes(results);
                }
            };
        }

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

        /// Only tests should ever override the release.
        release: vsr.Release = constants.config.process.release,

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

        /// The number of messages available for requests.
        ///
        /// This budget is consumed by `get_message` and is replenished when a message is released.
        ///
        /// Note that `Client` sends a `.register` request automatically on behalf of the user, so,
        /// until the first response is received, at most `constants.client_request_queue_max - 1`
        /// requests can be submitted.
        messages_available: u32 = constants.client_request_queue_max,

        /// Tracks the currently processing register message if any.
        register_inflight: ?*Message.Request = null,

        /// Tracks a currently processing request message submitted by `raw_request`.
        request_inflight: ?Request = null,

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

            var self = Self{
                .allocator = allocator,
                .message_bus = message_bus,
                .id = id,
                .cluster = cluster,
                .replica_count = replica_count,
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
            if (self.register_inflight) |message| self.release_message(message.base());
            if (self.request_inflight) |inflight| self.release_message(inflight.message.base());

            assert(self.messages_available == constants.client_request_queue_max);
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

        /// Sends a request message with the operation and events payload to the replica.
        /// There must be no other request message currently inflight.
        pub fn request(
            self: *Self,
            callback: Request.Callback,
            user_data: u128,
            operation: StateMachine.Operation,
            events: []const u8,
        ) void {
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(StateMachine.Event(operation_comptime)),
            };
            assert(events.len <= constants.message_body_size_max);
            assert(events.len % event_size == 0);

            assert(self.request_inflight == null);
            maybe(self.register_inflight != null);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            message.header.* = .{
                .client = self.id,
                .request = undefined, // Set inside `raw_request` down below.
                .cluster = self.cluster,
                .command = .request,
                .release = self.release,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(Header) + events.len),
            };

            stdx.copy_disjoint(.exact, u8, message.body(), events);
            self.raw_request(callback, user_data, message);
        }

        /// Sends a request, only setting request_number in the header.
        /// There must be no other request message currently inflight.
        pub fn raw_request(
            self: *Self,
            callback: Request.Callback,
            user_data: u128,
            message: *Message.Request,
        ) void {
            assert(self.messages_available < constants.client_request_queue_max);
            assert(!message.header.operation.vsr_reserved());
            assert(message.header.client == self.id);
            assert(message.header.release.value == self.release.value);
            assert(message.header.cluster == self.cluster);
            assert(message.header.command == .request);
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= constants.message_size_max);

            // Register before appending to request_queue.
            self.register();
            assert(self.request_number > 0);
            message.header.request = self.request_number;
            self.request_number += 1;

            log.debug("{}: request: user_data={} request={} size={} {s}", .{
                self.id,
                user_data,
                message.header.request,
                message.header.size,
                message.header.operation.tag_name(StateMachine),
            });

            assert(self.request_inflight == null);
            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = callback,
            };

            // Start processing the message if a register() isn't currently pending.
            if (self.register_inflight == null) {
                self.send_request_for_the_first_time(message);
            }
        }

        /// Acquires a message from the message bus.
        /// The caller must ensure that a message is available.
        ///
        /// Either use it in `client.raw_request()` or discard via `client.release_message()`,
        /// the reference is not guaranteed to be valid after both actions.
        /// Do NOT use the reference counter function `message.ref()` for storing the message.
        pub fn get_message(self: *Self) *Message {
            assert(self.messages_available > 0);
            self.messages_available -= 1;

            return self.message_bus.get_message(null);
        }

        /// Releases a message back to the message bus.
        pub fn release_message(self: *Self, message: *Message) void {
            assert(self.messages_available < constants.client_request_queue_max);
            self.messages_available += 1;

            self.message_bus.unref(message);
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

            log.err("{}: session evicted: reason={s} (cluster_release={})", .{
                self.id,
                @tagName(eviction.header.reason),
                eviction.header.release,
            });
            @panic("session evicted");
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
            assert(reply.header.release.value == self.release.value);

            if (reply.header.client != self.id) {
                log.debug("{}: on_reply: ignoring (wrong client={})", .{
                    self.id,
                    reply.header.client,
                });
                return;
            }

            var inflight = if (self.register_inflight) |message|
                Request{ .message = message, .user_data = 0, .callback = undefined }
            else if (self.request_inflight) |inflight|
                inflight
            else {
                log.debug("{}: on_reply: ignoring (no inflight request)", .{self.id});
                return;
            };

            if (reply.header.request < inflight.message.header.request) {
                log.debug("{}: on_reply: ignoring (request {} < {})", .{
                    self.id,
                    reply.header.request,
                    inflight.message.header.request,
                });
                return;
            }

            assert(reply.header.request_checksum == inflight.message.header.checksum);
            const inflight_vsr_operation = inflight.message.header.operation;
            const inflight_request = inflight.message.header.request;

            // For non-register replies, consume the inflight request here before invoking callbacks
            // down below in case they wish to queue a new request_inflight.
            if (self.register_inflight == null) {
                assert(self.request_number > 0);
                assert(inflight.message == self.request_inflight.?.message);
                self.request_inflight = null;
            }

            if (self.on_reply_callback) |on_reply_callback| {
                on_reply_callback(self, inflight.message, reply);
            }

            // Eagerly release request message, to ensure that user's callback can submit a new one.
            self.release_message(inflight.message.base());
            assert(self.messages_available > 0);
            inflight.message = undefined;

            log.debug("{}: on_reply: user_data={} request={} size={} {s}", .{
                self.id,
                inflight.user_data,
                reply.header.request,
                reply.header.size,
                reply.header.operation.tag_name(StateMachine),
            });

            assert(reply.header.request_checksum == self.parent);
            assert(reply.header.client == self.id);
            assert(reply.header.request == inflight_request);
            assert(reply.header.cluster == self.cluster);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.operation == inflight_vsr_operation);

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

            if (inflight_vsr_operation == .register) {
                assert(self.session == 0);
                assert(reply.header.commit > 0);
                self.session = reply.header.commit; // The commit number becomes the session number.

                // register_inflight was kept non-null to prevent a potential raw_request() in
                // on_reply_callback() above from calling send_request_for_the_first_time().
                // Now, it can be consumed and any queued request_inflight can then be processed.
                assert(self.register_inflight != null);
                self.register_inflight = null;
                if (self.request_inflight) |inflight_next| {
                    self.send_request_for_the_first_time(inflight_next.message);
                }
            } else {
                // The message is the result of raw_request(), so invoke the user callback.
                inflight.callback(
                    inflight.user_data,
                    inflight_vsr_operation.cast(StateMachine),
                    reply.body(),
                );
            }
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            const ping = Header.PingClient{
                .command = .ping_client,
                .cluster = self.cluster,
                .release = self.release,
                .client = self.id,
            };

            // TODO If we haven't received a pong from a replica since our last ping, then back off.
            self.send_header_to_replicas(ping.frame_const().*);
        }

        fn on_request_timeout(self: *Self) void {
            self.request_timeout.backoff(self.prng.random());

            const message = self.register_inflight orelse self.request_inflight.?.message;
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
            if (self.request_number > 0) return;

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            // We will set parent, session, view and checksums only when sending for the first time:
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = .register,
                .release = self.release,
            };

            assert(self.request_number == 0);
            self.request_number += 1;

            log.debug("{}: register: registering a session with the cluster", .{self.id});

            assert(self.request_inflight == null);
            assert(self.register_inflight == null);
            self.register_inflight = message;

            self.send_request_for_the_first_time(message);
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

        fn send_request_for_the_first_time(self: *Self, message: *Message.Request) void {
            if (self.register_inflight) |inflight| {
                maybe(self.request_inflight != null);
                assert(inflight == message);
            } else {
                assert(self.register_inflight == null);
                assert(self.request_inflight.?.message == message);
            }

            assert(message.header.command == .request);
            assert(message.header.parent == 0);
            assert(message.header.session == 0);
            assert(message.header.request < self.request_number);
            assert(message.header.view == 0);
            assert(message.header.size <= constants.message_size_max);
            assert(self.messages_available < constants.client_request_queue_max);

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

    pub fn operation_from_vsr(operation: vsr.Operation) ?Operation {
        if (operation.vsr_reserved()) return null;

        return vsr.Operation.to(TestStateMachine, operation);
    }

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

    const MessageBus = @import("../message_bus.zig").MessageBusClient;
    const VSRClient = Client(StateMachine, MessageBus);

    var results: [@divExact(constants.message_body_size_max, @sizeOf(Result))]Result = undefined;
    for (0..results.len) |i| {
        results[i] = i;
    }

    var prng = std.rand.DefaultPrng.init(42);
    for (0..1000) |_| {
        const events_total = @max(1, prng.random().uintAtMost(usize, results.len));
        var demuxer = VSRClient.DemuxerType(.batched).init(std.mem.sliceAsBytes(results[0..events_total]));

        var events_offset: usize = 0;
        while (events_offset < events_total) {
            const events_limit = events_total - events_offset;
            const events_count = @max(1, prng.random().uintAtMost(usize, events_limit));

            const reply_bytes = demuxer.decode(@intCast(events_offset), @intCast(events_count));
            const reply: []Result = @alignCast(std.mem.bytesAsSlice(Result, reply_bytes));
            try testing.expectEqual(&reply[0], &results[events_offset]);
            try testing.expectEqual(reply.len, results[events_offset..][0..events_count].len);

            for (reply, 0..) |result, i| {
                try testing.expectEqual(result, @as(Result, events_offset + i));
            }

            events_offset += events_count;
        }
    }
}
