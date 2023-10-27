const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

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

        pub const Request = struct {
            pub const Callback = *const fn (
                user_data: u128,
                operation: StateMachine.Operation,
                results: []const u8,
            ) void;

            const Info = struct {
                user_data: u128,
                callback: ?Callback, // Null iff operation=register.
            };

            const Demux = struct {
                next: ?*Demux = null,
                info: Info,
                event_count: u16,
                event_offset: u16,
            };

            info: Info,
            message: *Message.Request,
            demux_queue: FIFO(Demux) = .{ .name = null },
        };

        const DemuxPool = IOPS(Request.Demux, constants.client_request_queue_max);
        const RequestQueue = RingBuffer(Request, .{ .array = constants.client_request_queue_max });

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

        /// The number of messages available for requests.
        ///
        /// This budget is consumed by `get_message` and is replenished when a message is released.
        ///
        /// Note that `Client` sends a `.register` request automatically on behalf of the user, so,
        /// until the first response is received, at most `constants.client_request_queue_max - 1`
        /// requests can be submitted.
        messages_available: u32 = constants.client_request_queue_max,

        /// Pool of nodes which represent batched Infos on a given Request.
        demux_pool: DemuxPool = .{},

        /// StateMachine can't take advantage of Request.demux_queue order for in-place demuxing,
        /// so instead it demuxes into a scratch buffer and that's passed into Request.Callback.
        demux_buffer: *[constants.message_body_size_max]u8,

        /// A client is allowed at most one inflight request at a time at the protocol layer.
        /// We therefore queue any further concurrent requests made by the application layer.
        request_queue: RequestQueue = RequestQueue.init(),

        /// Tracks which Request in the request_queue is currently inflight in VSR.
        request_inflight: ?*Request = null,

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

            const demux_buffer = try allocator.create([constants.message_body_size_max]u8);
            errdefer allocator.free(demux_buffer);

            var self = Self{
                .allocator = allocator,
                .message_bus = message_bus,
                .id = id,
                .cluster = cluster,
                .replica_count = replica_count,
                .demux_buffer = demux_buffer,
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
            while (self.request_queue.pop()) |inflight| {
                self.release(inflight.message.base());
            }
            assert(self.messages_available == constants.client_request_queue_max);
            allocator.destroy(self.demux_buffer);
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

        pub const BatchError = error{
            // There are not enough internal resources (Messages or otherwise) to allocate
            // a batched Request.
            TooManyOutstanding,
        };

        /// Allocate space in a vsr Message to write [event_count]StateMachine.Event(operation).
        /// Unlike `raw_request`, a Batch will attempt to piggy-back onto an existing Message.
        /// The returned Batch provides a slice of memory to write the events to. Once populated,
        /// use `batch_submit` to commit the writes to the Message and send it through the Client.
        pub fn batch_get(
            self: *Self,
            operation: StateMachine.Operation,
            event_count: usize,
        ) BatchError!Batch {
            assert(@intFromEnum(operation) >= constants.vsr_operations_reserved);
            const event_size: usize = switch (operation) {
                inline else => |op| @sizeOf(StateMachine.Event(op)),
            };

            const body_size = event_size * event_count;
            assert(body_size <= constants.message_body_size_max);

            // Check-in with the StateMachine to see if this operation can even be batched.
            // If so, find an existing Request with the same Op that has room in its Message.
            // The request must not be the one currently inflight in VSR as its Message is sealed.
            if (StateMachine.batch_logical(operation)) {
                var it = self.request_queue.iterator_mutable();
                while (it.next_ptr()) |request| {
                    if (request == self.request_inflight) continue;
                    if (request.message.header.command != .request) continue;
                    if (request.message.header.operation.cast(StateMachine) != operation) continue;
                    if (request.message.header.size + body_size > constants.message_size_max) continue;

                    // Reserve a Demux node for attaching this Batch onto the existing Request.
                    // TODO: Should this return TooManyOutstanding? Currently, it falls back to new.
                    const demux = self.demux_pool.acquire() orelse break;
                    errdefer self.demux_pool.release(demux);

                    demux.* = .{
                        .info = undefined, // Set during batch_submit().
                        .event_count = @intCast(event_count),
                        // Offset from either the last queued Demux or the request initial size.
                        .event_offset = blk: {
                            if (request.demux_queue.peek_last()) |tail| {
                                break :blk tail.event_offset + tail.event_count;
                            }
                            const request_body_size = request.message.header.size - @sizeOf(Header);
                            break :blk @intCast(@divExact(request_body_size, event_size));
                        },
                    };

                    // Extend the message size to contain the Batch data as if already written.
                    // This allows Batch.slice() to simply use message.body().
                    request.message.header.size += @intCast(body_size);
                    assert(request.message.header.size <= constants.message_size_max);

                    request.demux_queue.push(demux);
                    return Batch{
                        .request = request,
                        .demux = demux,
                    };
                }
            }

            // Unable to batch the events to an existing Message so reserve a new one.
            if (self.messages_available == 0) return error.TooManyOutstanding;
            const message = self.get_message();
            errdefer self.release(message);

            // We will set parent, session, view and checksums only when sending for the first time:
            const message_request = message.build(.request);
            message_request.header.* = .{
                .client = self.id,
                .request = undefined,
                .cluster = self.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(Header) + body_size),
            };

            // Register before appending to request_queue.
            self.register();
            assert(self.request_number > 0);
            message_request.header.request = self.request_number;
            self.request_number += 1;

            // If we were able to reserve a Message, we can also reserve/enqueue a new Request.
            assert(!self.request_queue.full());
            self.request_queue.push_assume_capacity(.{
                .info = undefined, // Set during batch_submit().
                .message = message_request,
            });

            return Batch{
                .request = self.request_queue.tail_ptr().?,
                .demux = null,
            };
        }

        pub const Batch = struct {
            request: *Request,
            demux: ?*Request.Demux, // Null iff batch_get() newly created `request`

            pub fn slice(batch: Batch) []u8 {
                const body_offset = if (batch.demux) |demux| blk: {
                    const operation = batch.request.message.header.operation.cast(StateMachine);
                    break :blk switch (operation) {
                        inline else => |op| @sizeOf(StateMachine.Event(op)) * demux.event_offset,
                    };
                } else blk: {
                    assert(batch.request.demux_queue.empty());
                    break :blk 0;
                };

                const body = batch.request.message.body();
                return body[body_offset..];
            }
        };

        /// After writing Event to batch.slice(), commit the batched
        pub fn batch_submit(
            self: *Self,
            user_data: u128,
            callback: Request.Callback,
            batch: Batch,
        ) void {
            const info_ptr = if (batch.demux) |demux| &demux.info else &batch.request.info;
            info_ptr.* = .{
                .user_data = user_data,
                .callback = callback,
            };

            // Newly made Requests are populated and potentially pushed through VSR if no inflight.
            // NOTE: This sets the request.message.request number.
            if (batch.demux == null) {
                self.request_submit(batch.request);
            }
        }

        /// Sends a request, only setting request_number in the header. Currently only used by
        /// AOF replay support to replay messages with timestamps.
        pub fn raw_request(
            self: *Self,
            user_data: u128,
            callback: Request.Callback,
            message: *Message.Request,
        ) void {
            assert(!message.header.operation.vsr_reserved());
            assert(self.messages_available < constants.client_request_queue_max);

            // Register before appending to request_queue.
            self.register();
            assert(self.request_number > 0);
            message.header.request = self.request_number;
            self.request_number += 1;

            // If there was a reserved message, we should also be able to reserve a Request.
            assert(!self.request_queue.full());
            self.request_queue.push_assume_capacity(.{
                .info = .{ .user_data = user_data, .callback = callback },
                .message = message,
            });

            // Populate the newly made Request and potentially push it through VSR if no inflight.
            const request = self.request_queue.tail_ptr().?;
            self.request_submit(request);
        }

        /// Try to submit a well-prepared Request in request_queue to VSR.
        fn request_submit(self: *Self, request: *Request) void {
            assert(!self.request_queue.empty());
            assert(self.request_queue.tail_ptr().? == request);

            assert(request.info.callback != null);
            assert(request.demux_queue.empty());

            const message = request.message;
            assert(self.messages_available < constants.client_request_queue_max);
            assert(message.header.client == self.id);
            assert(message.header.request > 0);
            assert(message.header.request < self.request_number);
            assert(message.header.cluster == self.cluster);
            assert(message.header.command == .request);
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= constants.message_size_max);

            log.debug("{}: request: user_data={} request={} size={} {s}", .{
                self.id,
                request.info.user_data,
                message.header.request,
                message.header.size,
                message.header.operation.tag_name(StateMachine),
            });

            if (self.request_inflight == null) {
                self.request_inflight = self.request_queue.head_ptr().?;
                self.send_request_for_the_first_time();
            }
        }

        /// Acquires a message from the message bus.
        /// The caller must ensure that a message is available.
        ///
        /// Either use it in `client.request()` or discard via `client.release()`,
        /// the reference is not guaranteed to be valid after both actions.
        /// Do NOT use the reference counter function `message.ref()` for storing the message.
        pub fn get_message(self: *Self) *Message {
            assert(self.messages_available > 0);
            self.messages_available -= 1;

            return self.message_bus.get_message(null);
        }

        /// Releases a message back to the message bus.
        pub fn release(self: *Self, message: *Message) void {
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

            assert(self.request_inflight == self.request_queue.head_ptr());
            if (self.request_inflight) |inflight| {
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

            var inflight = self.request_queue.pop().?;
            const inflight_request = inflight.message.header.request;
            const inflight_operation = inflight.message.header.operation;

            if (self.on_reply_callback) |on_reply_callback| {
                on_reply_callback(self, inflight.message, reply);
            }

            // Eagerly release request message, to ensure that user's callback can submit a new
            // request.
            self.release(inflight.message.base());
            assert(self.messages_available > 0);

            // Even though we release our reference to the message, we might have another one
            // retained by the send queue in case of timeout.
            maybe(inflight.message.references > 0);
            inflight.message = undefined;

            log.debug("{}: on_reply: user_data={} request={} size={} {s}", .{
                self.id,
                inflight.info.user_data,
                reply.header.request,
                reply.header.size,
                reply.header.operation.tag_name(StateMachine),
            });

            assert(reply.header.parent == self.parent);
            assert(reply.header.client == self.id);
            assert(reply.header.request == inflight_request);
            assert(reply.header.cluster == self.cluster);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.operation == inflight_operation);

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

            if (inflight_operation == .register) {
                assert(self.session == 0);
                assert(reply.header.commit > 0);
                self.session = reply.header.commit; // The commit number becomes the session number.
            }

            // We must start the next request before releasing control back to the callback(s).
            // Otherwise, requests may never run through send_request_for_the_first_time().
            self.request_inflight = self.request_queue.head_ptr();
            if (self.request_inflight != null) {
                self.send_request_for_the_first_time();
            }

            // Ignore callback processing if the Request was from register().
            if (inflight.info.callback == null) {
                assert(inflight_operation == .register);
                return;
            }

            assert(!inflight_operation.vsr_reserved());
            const operation = inflight_operation.cast(StateMachine);

            // Simple case: the Request is just itself with no other batched with it.
            if (inflight.demux_queue.empty()) {
                return (inflight.info.callback.?)(
                    inflight.info.user_data,
                    operation,
                    reply.body(),
                );
            }

            // The Request was batched with others and it has to be demuxed.
            // To simplify handling, push the initial Request as a Demux node its demux_queue.
            var inflight_demux = Request.Demux{
                .info = inflight.info,
                .event_offset = 0,
                .event_count = blk: {
                    // The first Demux node contains the initial Request body size as event_offset.
                    const head = inflight.demux_queue.peek().?;
                    break :blk head.event_offset;
                },
            };
            inflight.demux_queue.push(&inflight_demux);

            while (inflight.demux_queue.pop()) |demux| {
                // Extract/use the Demux node info to accumulate the actual reply into demux_buffer.
                const info = demux.info;
                const demuxed = StateMachine.batch_demux(operation, reply.body(), self.demux_buffer, .{
                    .index = demux.event_offset,
                    .count = demux.event_count,
                });

                // Free the Demux node before invoking the callback in-case it calls batch_get() and
                // wants a Demux node as well.
                if (demux != &inflight_demux) self.demux_pool.release(demux);
                (info.callback.?)(
                    inflight.info.user_data,
                    operation,
                    self.demux_buffer[0..demuxed],
                );
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

            const inflight = self.request_inflight.?;
            assert(inflight == self.request_queue.head_ptr().?);

            const message = inflight.message;
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
            errdefer self.release(message);

            // We will set parent, session, view and checksums only when sending for the first time:
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
                .info = .{ .user_data = 0, .callback = null },
                .message = message,
            });

            assert(self.request_inflight == null);
            self.request_inflight = self.request_queue.head_ptr().?;
            self.send_request_for_the_first_time();
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

        fn send_request_for_the_first_time(self: *Self) void {
            const inflight = self.request_inflight.?;
            assert(inflight == self.request_queue.head_ptr().?);

            const message = inflight.message;
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
