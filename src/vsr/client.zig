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

        pub const Request = struct {
            pub const Callback = *const fn (
                user_data: u128,
                operation: StateMachine.Operation,
                results: []const u8,
            ) void;

            pub const Demux = struct {
                next: ?*Demux = null,
                user_data: u128,
                callback: ?Callback, // Null iff operation=register.
                event_count: u16,
                event_offset: u16,
            };

            message: *Message.Request,
            demux_queue: FIFO(Demux) = .{ .name = null },
        };

        pub const Batch = struct {
            request: *Request,
            demux: *Request.Demux,

            pub fn slice(batch: Batch) []u8 {
                // Ensure batch.demux is either the one inlined in the Request or is in demux_queue.
                assert(batch.request.demux_queue.count > 0);
                if (constants.verify) {
                    if (batch.request.demux_queue.count == 1) {
                        assert(batch.request.demux_queue.peek() == batch.demux);
                    } else {
                        assert(batch.request.demux_queue.contains(batch.demux));
                    }
                }

                const message = batch.request.message;
                assert(message.header.command == .request);

                const event_size: u32 = switch (message.header.operation.cast(StateMachine)) {
                    inline else => |operation| @sizeOf(StateMachine.Event(operation)),
                };

                const body_offset = event_size * batch.demux.event_offset;
                const body = message.body()[body_offset..];
                assert(@divExact(body.len, event_size) > 0);
                return body;
            }
        };

        /// A pool of Request.Demux nodes used for batching client events.
        /// This uses the lazy FIFO pattern over IOPS due to the max Demux node count being large.
        const DemuxPool = struct {
            free: ?*Request.Demux = null,
            memory_allocated: usize = 0,
            memory: []Request.Demux,

            /// The maximum amount of logical batches that may be queued on a client
            /// (spread across all requests).
            const batch_logical_max = blk: {
                var max: usize = 1;
                inline for (std.enums.values(StateMachine.Operation)) |operation| {
                    if (@intFromEnum(operation) < constants.vsr_operations_reserved) continue;
                    if (!StateMachine.batch_logical_allowed.get(operation)) continue;
                    max = @max(max, @sizeOf(StateMachine.Result(operation)));
                    max = @max(max, @sizeOf(StateMachine.Event(operation)));
                }
                break :blk max * constants.client_request_queue_max;
            };

            fn init(allocator: mem.Allocator) !DemuxPool {
                return DemuxPool{
                    .memory = try allocator.alloc(Request.Demux, batch_logical_max),
                };
            }

            fn deinit(pool: *DemuxPool, allocator: mem.Allocator) void {
                allocator.free(pool.memory);
            }

            fn acquire(pool: *DemuxPool) ?*Request.Demux {
                if (pool.free) |demux| {
                    pool.free = demux.next;
                    return demux;
                }
                if (pool.memory_allocated < pool.memory.len) {
                    defer pool.memory_allocated += 1;
                    return &pool.memory[pool.memory_allocated];
                }
                return null;
            }

            fn release(pool: *DemuxPool, demux: *Request.Demux) void {
                assert(@intFromPtr(demux) < @intFromPtr(pool.memory.ptr + pool.memory_allocated));
                assert(@intFromPtr(demux) >= @intFromPtr(&pool.memory[0]));
                demux.next = pool.free;
                pool.free = demux;
            }
        };

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
        demux_pool: DemuxPool,

        /// A client is allowed at most one inflight request at a time at the protocol layer.
        /// We therefore queue any further concurrent requests made by the application layer.
        request_queue: RequestQueue = RequestQueue.init(),

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

            var demux_pool = try DemuxPool.init(allocator);
            errdefer demux_pool.deinit(allocator);

            var self = Self{
                .allocator = allocator,
                .message_bus = message_bus,
                .id = id,
                .cluster = cluster,
                .replica_count = replica_count,
                .demux_pool = demux_pool,
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
            self.demux_pool.deinit(allocator);
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
            // There are not enough internal resources (Messages or Demux) to allocate
            // a batched Request.
            TooManyOutstanding,
        };

        /// Allocate space in a vsr Message to write [event_count]StateMachine.Event(operation).
        /// Unlike `raw_request`, a Batch will attempt to piggy-back onto an existing Message.
        /// The returned Batch provides a slice of memory to write the events to. Once populated,
        /// use `batch_submit` to commit the writes to the Message and send it through the Client.
        /// NOTE: A Batch cannot be stored and must be passed to batch_submit() before calling
        /// any other methods on the Client.
        pub fn batch_get(
            self: *Self,
            operation: StateMachine.Operation,
            event_count: usize,
        ) BatchError!Batch {
            assert(@intFromEnum(operation) >= constants.vsr_operations_reserved);
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(StateMachine.Event(operation_comptime)),
            };

            const body_size = event_size * event_count;
            assert(body_size <= constants.message_body_size_max);

            // A demux node is always reserved, either for the initial Request or a tag-along demux.
            const demux = self.demux_pool.acquire() orelse return error.TooManyOutstanding;
            errdefer self.demux_pool.release(demux);

            demux.* = .{
                .user_data = undefined, // Set during batch_submit().
                .callback = undefined, // Set during batch_submit().
                .event_count = @intCast(event_count),
                // Assume the first demux here. Updated if enqueued onto an existing Request.
                .event_offset = 0,
            };

            // Check-in with the StateMachine to see if this operation can even be batched.
            // If so, find an existing Request with the same Op that has room in its Message.
            if (StateMachine.batch_logical_allowed.get(operation)) {
                var it = self.request_queue.iterator_mutable();

                // The request must not be the one currently inflight in VSR as its Message is
                // being sent over the MessageBus and waiting for a reasponse.
                _ = it.next_ptr();

                while (it.next_ptr()) |request| {
                    if (request.message.header.operation.cast(StateMachine) != operation) continue;
                    if (request.message.header.size + body_size > constants.message_size_max) continue;

                    // Set the demux offset to be at the end of the current request's Message body.
                    demux.event_offset = @intCast(@divExact(request.message.body().len, event_size));
                    assert(demux.event_offset > 0);

                    // Then, extend the message to contain the Batch data as if already written.
                    // This allows Batch.slice() to simply use message.body().
                    request.message.header.size += @intCast(body_size);
                    assert(request.message.header.size <= constants.message_size_max);

                    assert(request.demux_queue.count >= 1);
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

            // If we were able to reserve a Message, we can also enqueue a new Request.
            assert(!self.request_queue.full());
            self.request_queue.push_assume_capacity(.{
                .message = message_request,
            });

            const request = self.request_queue.tail_ptr().?;
            assert(request.demux_queue.empty());
            request.demux_queue.push(demux);
            return Batch{
                .request = request,
                .demux = demux,
            };
        }

        /// After writing Events to batch.slice(), mark the batched data as sendable with a callback
        /// and user_data for when it completes. This may also start sending the Batch's message
        /// through VSR.
        pub fn batch_submit(
            self: *Self,
            user_data: u128,
            callback: Request.Callback,
            batch: Batch,
        ) void {
            assert(batch.request.demux_queue.count > 0);
            assert(batch.request.message.body().len > 0);
            assert(batch.request.message.header.command == .request);

            batch.demux.user_data = user_data;
            batch.demux.callback = callback;

            // Newly made Requests are populated and potentially pushed through VSR if there exists
            // no currently inflight request.
            // A Batch is newly made if its Request only contains its Demux.
            if (batch.request.demux_queue.count == 1) {
                assert(batch.request.demux_queue.peek() == batch.demux);
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

            const event_size: usize = switch (message.header.operation.cast(StateMachine)) {
                inline else => |operation| @sizeOf(StateMachine.Event(operation)),
            };

            // Assume that the caller ensures there will be Demux nodes available for the message.
            const demux = self.demux_pool.acquire() orelse unreachable;
            errdefer self.demux_pool.release(demux);

            demux.* = .{
                .user_data = user_data,
                .callback = callback,
                .event_count = @intCast(@divExact(message.body().len, event_size)),
                .event_offset = 0,
            };

            // Register before appending to request_queue.
            self.register();
            assert(self.request_number > 0);
            message.header.request = self.request_number;
            self.request_number += 1;

            // If there was a reserved message, we should also be able to reserve a Request.
            assert(!self.request_queue.full());
            self.request_queue.push_assume_capacity(.{
                .message = message,
            });

            // Populate the newly made Request and potentially push it through VSR if no inflight.
            const request = self.request_queue.tail_ptr().?;
            request.demux_queue.push(demux);
            self.request_submit(request);
        }

        /// Try to submit a well-prepared Request in request_queue to VSR.
        fn request_submit(self: *Self, request: *Request) void {
            assert(!self.request_queue.empty());
            assert(self.request_queue.tail_ptr().? == request);

            assert(request.demux_queue.count > 0);

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
                request.demux_queue.peek().?.user_data,
                message.header.request,
                message.header.size,
                message.header.operation.tag_name(StateMachine),
            });

            // Send the Request's message through VSR if there are no other Requests pending.
            if (self.request_queue.head_ptr().? == request) {
                self.send_request_for_the_first_time(message);
            }
        }

        /// Acquires a message from the message bus.
        /// The caller must ensure that a message is available.
        ///
        /// Either use it in `client.raw_request()` or discard via `client.release()`,
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

            var inflight = self.request_queue.pop().?;
            const inflight_request = inflight.message.header.request;
            const inflight_vsr_operation = inflight.message.header.operation;

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
                inflight.demux_queue.peek().?.user_data,
                reply.header.request,
                reply.header.size,
                reply.header.operation.tag_name(StateMachine),
            });

            assert(reply.header.parent == self.parent);
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
            }

            // We must start the next request before releasing control back to the callback(s).
            // Otherwise, requests may never run through send_request_for_the_first_time().
            if (self.request_queue.head_ptr()) |request| {
                self.send_request_for_the_first_time(request.message);
            }

            // Ignore callback processing if the Request was from register().
            if (inflight_vsr_operation == .register) {
                const demux = inflight.demux_queue.pop().?;
                assert(inflight.demux_queue.peek() == null);
                assert(demux.user_data == 0);
                assert(demux.callback == null);
                self.demux_pool.release(demux);
                return;
            }

            assert(!inflight_vsr_operation.vsr_reserved());
            switch (inflight_vsr_operation.cast(StateMachine)) {
                inline else => |operation| {
                    var demuxer = StateMachine.DemuxerType(operation).init(
                        std.mem.bytesAsSlice(StateMachine.Result(operation), reply.body()),
                    );

                    while (inflight.demux_queue.pop()) |demux| {
                        // Extract/use the Demux node info to slice the results from the reply.
                        const decoded = demuxer.decode(demux.event_offset, demux.event_count);
                        const response = std.mem.sliceAsBytes(decoded);
                        if (!StateMachine.batch_logical_allowed.get(operation)) {
                            assert(response.len == reply.body().len);
                        }

                        // Free the Demux node before invoking the callback in case it calls
                        // batch_get() and wants a Demux node as well.
                        const user_data = demux.user_data;
                        const callback = demux.callback.?;
                        self.demux_pool.release(demux);

                        callback(user_data, operation, response);
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

            const inflight = self.request_queue.head_ptr().?;

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

            const demux = self.demux_pool.acquire().?;
            errdefer self.demux_pool.release(demux);

            demux.* = .{
                .user_data = 0,
                .callback = null,
                .event_count = 0,
                .event_offset = 0,
            };

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
                .message = message,
            });

            const request = self.request_queue.head_ptr().?;
            assert(self.request_queue.tail_ptr() == request);
            request.demux_queue.push(demux);

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
            assert(self.request_queue.head_ptr().?.message == message);

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
                .parent = message.header.checksum,
                .client = message.header.client,
                .context = undefined, // computed below.
                .op = bus.commit,
                .commit = bus.commit,
                .timestamp = bus.timestamp,
                .request = message.header.request,
                .operation = message.header.operation,
            };

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

    // Submits batches to the VSRClient and checks messages_available based on Submissions.
    const Context = struct {
        client: VSRClient,
        messages_reserved: u32 = 0,

        /// Tracks highest Submission.user_id which completed in its callback.
        var highest_user_id: u128 = 0;

        const Submission = struct {
            operation: StateMachine.Operation,
            event_count: usize,
            user_id: u128,
            messages_reserved: u32,
        };

        fn submit(self: *@This(), comptime submission: Submission) !void {
            const batch = try self.client.batch_get(
                submission.operation,
                @intCast(submission.event_count),
            );
            const events = switch (submission.operation) {
                inline else => |operation| blk: {
                    const event_size = @sizeOf(Event(operation));
                    const event_max = @divExact(constants.message_body_size_max, event_size);
                    break :blk std.mem.zeroes([event_max]Event(operation));
                },
            };
            @memcpy(batch.slice(), std.mem.sliceAsBytes(events[0..submission.event_count]));

            const callback = struct {
                pub fn callback(user_id: u128, op: StateMachine.Operation, reply: []const u8) void {
                    assert(op == submission.operation);
                    assert(user_id == submission.user_id);
                    highest_user_id = @max(highest_user_id, user_id);

                    const result_size = @sizeOf(StateMachine.Result(submission.operation));
                    assert(reply.len == result_size * submission.event_count);
                }
            }.callback;
            self.client.batch_submit(submission.user_id, callback, batch);

            self.messages_reserved += submission.messages_reserved;
            const messages_available = constants.client_request_queue_max - self.messages_reserved;
            assert(self.client.messages_available == messages_available);
        }
    };

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    var ctx = Context{ .client = try VSRClient.init(allocator, 1, 0, 1, &message_pool, .{}) };
    defer ctx.client.deinit(allocator);

    // Make sure all messages are available at the start.
    comptime var last_user_id: u128 = 0;
    assert(ctx.client.messages_available == constants.client_request_queue_max);

    // New request with op that is batchable, with one more slot until it fills up.
    // Ensure this reserves TWO messages (one for register(), one for .batched).
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = events_batched_max - 1,
        .user_id = last_user_id,
        .messages_reserved = 2,
    });

    // New request with op that is NOT batchable.
    // Ensure it reserves a new message, unrelated to the previous .batched.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .serial,
        .event_count = events_serial_max - 1,
        .user_id = last_user_id,
        .messages_reserved = 1,
    });

    // New request with a single batchable op.
    // Ensure no new messages as it should get merged with the previous batchable op to fill it.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .messages_reserved = 0,
    });

    // Another new request with a batchable op.
    // Ensure this creates a NEW message as the previous batch should've been filled up.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .messages_reserved = 1,
    });

    // Another new request with an op that's NOT batchable.
    // Ensure this creates a NEW message, not batched with previous .sreial that had extra room.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .serial,
        .event_count = 1,
        .user_id = last_user_id,
        .messages_reserved = 1,
    });

    // Final new request with a batchable op.
    // Ensure this gets merged with the other .batched instead of creating a new message.
    last_user_id += 1;
    try ctx.submit(.{
        .operation = .batched,
        .event_count = 1,
        .user_id = last_user_id,
        .messages_reserved = 0,
    });

    while (Context.highest_user_id != last_user_id) {
        ctx.client.tick();
    }
}
