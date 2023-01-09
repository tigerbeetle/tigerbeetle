const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = @import("../message_pool.zig").MessagePool.Message;

const log = std.log.scoped(.client);

pub fn Client(comptime StateMachine_: type, comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        pub const StateMachine = StateMachine_;

        pub const Error = error{
            TooManyOutstandingRequests,
        };

        pub const Request = struct {
            pub const Callback = fn (
                user_data: u128,
                operation: StateMachine.Operation,
                results: Error![]const u8,
            ) void;
            user_data: u128,
            callback: Callback,
            message: *Message,
        };

        allocator: mem.Allocator,
        message_bus: MessageBus,

        /// A universally unique identifier for the client (must not be zero).
        /// Used for routing replies back to the client via any network path (multi-path routing).
        /// The client ID must be ephemeral and random per process, and never persisted, so that
        /// lingering or zombie deployment processes cannot break correctness and/or liveness.
        /// A cryptographic random number generator must be used to ensure these properties.
        id: u128,

        /// The identifier for the cluster that this client intends to communicate with.
        cluster: u32,

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

        /// A client is allowed at most one inflight request at a time at the protocol layer.
        /// We therefore queue any further concurrent requests made by the application layer.
        request_queue: RingBuffer(Request, constants.client_request_queue_max, .array) = .{},

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
        on_reply_callback: ?fn (
            client: *Self,
            request: *Message,
            reply: *Message,
        ) void = null,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u32,
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
                .prng = std.rand.DefaultPrng.init(@truncate(u64, id)),
            };

            self.ping_timeout.start();

            return self;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            while (self.request_queue.pop()) |inflight| {
                self.message_bus.unref(inflight.message);
            }
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
            switch (message.header.command) {
                .pong => self.on_pong(message),
                .reply => self.on_reply(message),
                .eviction => self.on_eviction(message),
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

        pub fn request(
            self: *Self,
            user_data: u128,
            callback: Request.Callback,
            operation: StateMachine.Operation,
            message: *Message,
            message_body_size: usize,
        ) void {
            assert(operation != .reserved);
            assert(operation != .root);
            assert(operation != .register);

            self.register();

            // We will set parent, context, view and checksums only when sending for the first time:
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(u32, @sizeOf(Header) + message_body_size),
            };

            assert(self.request_number > 0);
            self.request_number += 1;

            log.debug("{}: request: user_data={} request={} size={} {s}", .{
                self.id,
                user_data,
                message.header.request,
                message.header.size,
                @tagName(operation),
            });

            if (self.request_queue.full()) {
                callback(user_data, operation, error.TooManyOutstandingRequests);
                return;
            }

            const was_empty = self.request_queue.empty();

            self.request_queue.push_assume_capacity(.{
                .user_data = user_data,
                .callback = callback,
                .message = message.ref(),
            });

            // If the queue was empty, then there is no request inflight and we must send this one:
            if (was_empty) self.send_request_for_the_first_time(message);
        }

        /// Acquires a message from the message bus if one is available.
        pub fn get_message(self: *Self) *Message {
            return self.message_bus.get_message();
        }

        /// Releases a message back to the message bus.
        pub fn unref(self: *Self, message: *Message) void {
            self.message_bus.unref(message);
        }

        fn on_eviction(self: *Self, eviction: *const Message) void {
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

        fn on_pong(self: *Self, pong: *const Message) void {
            assert(pong.header.command == .pong);
            assert(pong.header.cluster == self.cluster);

            if (pong.header.client != 0) {
                log.debug("{}: on_pong: ignoring (client != 0)", .{self.id});
                return;
            }

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

        fn on_reply(self: *Self, reply: *Message) void {
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

            if (self.request_queue.head_ptr()) |inflight| {
                if (reply.header.request < inflight.message.header.request) {
                    log.debug("{}: on_reply: ignoring (request {} < {})", .{
                        self.id,
                        reply.header.request,
                        inflight.message.header.request,
                    });
                    return;
                }
            } else {
                log.debug("{}: on_reply: ignoring (no inflight request)", .{self.id});
                return;
            }

            const inflight = self.request_queue.pop().?;
            defer self.message_bus.unref(inflight.message);

            log.debug("{}: on_reply: user_data={} request={} size={} {s}", .{
                self.id,
                inflight.user_data,
                reply.header.request,
                reply.header.size,
                @tagName(reply.header.operation.cast(StateMachine)),
            });

            assert(reply.header.parent == self.parent);
            assert(reply.header.client == self.id);
            assert(reply.header.context == 0);
            assert(reply.header.request == inflight.message.header.request);
            assert(reply.header.cluster == self.cluster);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.operation == inflight.message.header.operation);

            // The checksum of this reply becomes the parent of our next request:
            self.parent = reply.header.checksum;

            if (reply.header.view > self.view) {
                log.debug("{}: on_reply: newer view={}..{}", .{
                    self.id,
                    self.view,
                    reply.header.view,
                });
                self.view = reply.header.view;
            }

            self.request_timeout.stop();

            if (inflight.message.header.operation == .register) {
                assert(self.session == 0);
                assert(reply.header.commit > 0);
                self.session = reply.header.commit; // The commit number becomes the session number.
            }

            // We must process the next request before releasing control back to the callback.
            // Otherwise, requests may run through send_request_for_the_first_time() more than once.
            if (self.request_queue.head_ptr()) |next_request| {
                self.send_request_for_the_first_time(next_request.message);
            }

            if (self.on_reply_callback) |on_reply_callback| {
                on_reply_callback(self, inflight.message, reply);
            }

            if (inflight.message.header.operation != .register) {
                inflight.callback(
                    inflight.user_data,
                    inflight.message.header.operation.cast(StateMachine),
                    reply.body(),
                );
            }
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            const ping = Header{
                .command = .ping,
                .cluster = self.cluster,
                .client = self.id,
            };

            // TODO If we haven't received a pong from a replica since our last ping, then back off.
            self.send_header_to_replicas(ping);
        }

        fn on_request_timeout(self: *Self) void {
            self.request_timeout.backoff(self.prng.random());

            const message = self.request_queue.head_ptr().?.message;
            assert(message.header.command == .request);
            assert(message.header.request < self.request_number);
            assert(message.header.checksum == self.parent);
            assert(message.header.context == self.session);

            log.debug("{}: on_request_timeout: resending request={} checksum={}", .{
                self.id,
                message.header.request,
                message.header.checksum,
            });

            // We assume the primary is down and round-robin through the cluster:
            self.send_message_to_replica(
                @intCast(u8, (self.view + self.request_timeout.attempts) % self.replica_count),
                message,
            );
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_message_from_header(self: *Self, header: Header) *Message {
            assert(header.client == self.id);
            assert(header.cluster == self.cluster);
            assert(header.size == @sizeOf(Header));

            const message = self.message_bus.pool.get_message();
            defer self.message_bus.unref(message);

            message.header.* = header;
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        /// Registers a session with the cluster for the client, if this has not yet been done.
        fn register(self: *Self) void {
            if (self.request_number > 0) return;

            const message = self.message_bus.get_message();
            defer self.message_bus.unref(message);

            // We will set parent, context, view and checksums only when sending for the first time:
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = .register,
            };

            assert(self.request_number == 0);
            self.request_number += 1;

            log.debug("{}: register: registering a session with the cluster", .{self.id});

            assert(self.request_queue.empty());

            self.request_queue.push_assume_capacity(.{
                .user_data = 0,
                .callback = undefined,
                .message = message.ref(),
            });

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
            assert(message.header.client == self.id);
            assert(message.header.cluster == self.cluster);

            self.message_bus.send_message_to_replica(replica, message);
        }

        fn send_request_for_the_first_time(self: *Self, message: *Message) void {
            assert(self.request_queue.head_ptr().?.message == message);

            assert(message.header.command == .request);
            assert(message.header.parent == 0);
            assert(message.header.context == 0);
            assert(message.header.request < self.request_number);
            assert(message.header.view == 0);
            assert(message.header.size <= constants.message_size_max);

            // We set the message checksums only when sending the request for the first time,
            // which is when we have the checksum of the latest reply available to set as `parent`,
            // and similarly also the session number if requests were queued while registering:
            message.header.parent = self.parent;
            message.header.context = self.session;
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
            self.send_message_to_replica(@intCast(u8, self.view % self.replica_count), message);
        }
    };
}
