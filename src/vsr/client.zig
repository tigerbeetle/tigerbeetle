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
            BatchSizeZero,
            TooMuchData,
            TooManyOutstandingRequests,
        };

        pub const Callback = fn (
            user_data: u128,
            operation: StateMachine.Operation,
            results: Error![]const u8,
        ) void;

        pub const Request = struct {
            // Null iif operation=register.
            demux_list: ?DemuxList,
            message: *Message,
        };

        pub const BatchLogical = struct {
            operation: StateMachine.Operation,
            size: u32,
            batch_physical: union(enum) {
                new: *Message,
                append: struct {
                    request: *Request,
                    offset: u32,
                },
            },

            pub fn slice(self: *@This()) []u8 {
                const body = switch (self.batch_physical) {
                    .new => |value| value.body(),
                    .append => |value| value.request.message.body()[value.offset..],
                };
                return body[0..self.size];
            }

            pub fn offset(self: *const @This()) u32 {
                return switch (self.batch_physical) {
                    .new => 0,
                    .append => |value| value.offset,
                };
            }
        };

        const DemuxList = struct {
            head: *Demux,
            tail: *Demux,
        };

        const Demux = struct {
            user_data: u128,
            callback: Callback,
            offset: u32,
            size: u32,
            next: ?*Demux = null,
        };

        const DemuxPool = struct {
            buffer: []Demux,
            stack: ?*Demux,

            pub fn init(allocator: mem.Allocator) error{OutOfMemory}!@This() {
                // Max number of logical batches that need to be demuxed is
                // client_request_queue_max * (max of operations per request):
                const demux_max = constants.client_request_queue_max * comptime blk: {
                    var batch_max: usize = 0;
                    inline for (std.meta.declarations(StateMachine.constants.batch_max)) |decl| {
                        if (decl.is_pub) {
                            batch_max = std.math.max(
                                batch_max,
                                @field(StateMachine.constants.batch_max, decl.name),
                            );
                        }
                    }
                    assert(batch_max > 0);
                    break :blk batch_max;
                };

                return try init_capacity(allocator, demux_max);
            }

            pub fn init_capacity(allocator: mem.Allocator, demux_max: usize) error{OutOfMemory}!@This() {
                var buffer = try allocator.alloc(Demux, demux_max);
                var list = blk: {
                    var head: ?*Demux = null;
                    var tail: ?*Demux = null;

                    for (buffer) |*current| {
                        current.next = null;
                        if (tail) |previous| {
                            previous.next = current;
                        } else {
                            head = current;
                        }
                        tail = current;
                    }

                    break :blk head;
                };

                return @This(){
                    .buffer = buffer,
                    .stack = list,
                };
            }

            pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
                allocator.free(self.buffer);
                self.stack = null;
            }

            pub fn acquire_demux(self: *@This()) *Demux {
                const first = self.stack.?;
                self.stack = first.next;
                first.next = null;
                return first;
            }

            pub fn release_demux(self: *@This(), demux: *Demux) void {
                demux.next = self.stack;
                self.stack = demux;
            }
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

        demux_pool: DemuxPool,

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
            self.demux_pool.deinit(allocator);
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
                .pong_client => self.on_pong_client(message),
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

        pub fn get_batch(
            self: *Self,
            operation: StateMachine.Operation,
            size: u32,
        ) Error!BatchLogical {
            assert(operation != .reserved);
            assert(operation != .root);
            assert(operation != .register);

            if (size == 0) return error.BatchSizeZero;
            if (size > constants.message_body_size_max) return error.TooMuchData;

            if (!self.request_queue.empty()) {
                var it = self.request_queue.iterator_mutable();
                while (it.next_ptr()) |request_ptr| {
                    const request_operation = request_ptr.message.header.operation.cast(StateMachine);
                    if (operation == request_operation) {
                        const message_size = request_ptr.message.header.size + size;
                        if (message_size <= constants.message_size_max) {

                            // Appending this batch to an existing request:
                            const offset = @intCast(u32, request_ptr.message.body().len);
                            request_ptr.message.header.size += size;
                            return BatchLogical{
                                .operation = operation,
                                .size = size,
                                .batch_physical = .{
                                    .append = .{
                                        .request = request_ptr,
                                        .offset = offset,
                                    },
                                },
                            };
                        }
                    }
                }
            }

            // Creating a new request for this batch:
            if (self.request_queue.full()) return error.TooManyOutstandingRequests;

            // We will set request when submiting the batch,
            // and set parent, context, view and checksums only when sending for the first time:
            const message = self.get_message();
            message.header.* = .{
                .client = self.id,
                .cluster = self.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(u32, @sizeOf(Header) + size),
            };

            return BatchLogical{
                .operation = operation,
                .size = size,
                .batch_physical = .{ .new = message },
            };
        }

        pub fn submit_batch(
            self: *Self,
            user_data: u128,
            callback: Callback,
            batch_logical: BatchLogical,
        ) void {
            assert(batch_logical.operation != .reserved);
            assert(batch_logical.operation != .root);
            assert(batch_logical.operation != .register);

            var demux = self.demux_pool.acquire_demux();
            demux.* = .{
                .user_data = user_data,
                .callback = callback,
                .offset = batch_logical.offset(),
                .size = batch_logical.size,
            };

            switch (batch_logical.batch_physical) {
                .new => |message| {
                    // Dispatch this batch in a new request
                    defer self.unref(message);
                    self.request(demux, batch_logical.operation, message);
                },
                .append => |append| {
                    if (append.request.demux_list) |*demux_list| {
                        // Append this batch in a queued request
                        demux_list.tail.next = demux;
                        demux_list.tail = demux;
                    } else assert(false);
                },
            }
        }

        fn request(
            self: *Self,
            demux: *Demux,
            operation: StateMachine.Operation,
            message: *Message,
        ) void {
            assert(operation != .reserved);
            assert(operation != .root);
            assert(operation != .register);

            self.register();
            assert(self.request_number > 0);
            message.header.request = self.request_number;

            // This function must be called only for the first logical batch
            assert(demux.offset == 0);

            log.debug("{}: request: user_data={} request={} size={} {s}", .{
                self.id,
                demux.user_data,
                message.header.request,
                message.header.size,
                @tagName(operation),
            });

            // This was checked during the batch acquisition,
            // it's not expected to change between get_batch and submit_batch calls.
            assert(!self.request_queue.full());
            const was_empty = self.request_queue.empty();

            self.request_number += 1;
            self.request_queue.push_assume_capacity(blk: {
                // TODO: Compiler glitch if .demux_list is initialized inside Request,
                // both pointer are set as null.
                const demux_list = DemuxList{
                    .head = demux,
                    .tail = demux,
                };
                break :blk Request{
                    .demux_list = demux_list,
                    .message = message.ref(),
                };
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

        fn on_pong_client(self: *Self, pong: *const Message) void {
            assert(pong.header.command == .pong_client);
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
                0, //PENDING:: Adjust logs.
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

            if (inflight.demux_list) |demux_list| {
                assert(inflight.message.header.operation != .register);

                const operation = inflight.message.header.operation.cast(StateMachine);
                switch (operation) {
                    .reserved, .root, .register => unreachable,
                    .create_accounts => self.decode_reply(.create_accounts, demux_list, reply),
                    .create_transfers => self.decode_reply(.create_transfers, demux_list, reply),
                    .lookup_accounts => self.decode_reply(.lookup_accounts, demux_list, reply),
                    .lookup_transfers => self.decode_reply(.lookup_transfers, demux_list, reply),
                }
            } else {
                assert(inflight.message.header.operation == .register);
            }
        }

        /// Decoding the state machine reply into multiple logical batches,
        /// Example for create_accounts and create_transfers:
        ///
        /// Response (index,result):
        /// (0,x),(1,x),(3,x),(5,x),(20,x)
        ///
        /// Demux list (offset,size):
        /// (0,2)(2,10),(12,8)(20,1)
        ///
        /// Demux process:
        /// Reply        | Demux  | Demuxed reply
        /// -------------|--------|---------------
        /// (0,x),(1,x)  | (0,2)  | (0,x),(1,x)
        /// (3,x),(5,x)  | (2,10) | (1,x),(3,x)
        /// ()           | (12,8) | ()
        /// (20,x)       | (20,1) | (0,x)
        fn decode_reply(self: *Self, comptime operation: StateMachine.Operation, demux_list: DemuxList, reply: *Message) void {

            // We don't support batching on lookup operations yet.
            if (operation == .lookup_accounts or operation == .lookup_transfers) {
                assert(demux_list.head.next == null);
                demux_list.head.callback(
                    demux_list.head.user_data,
                    operation,
                    reply.body(),
                );
                return;
            }

            const Event = StateMachine.Event(operation);
            const Result = StateMachine.Result(operation);
            const results = std.mem.bytesAsSlice(Result, reply.body());

            var head: ?*Demux = demux_list.head;
            var index_current: u32 = 0;
            while (head) |demux| {
                if (index_current >= results.len) {
                    // There is no more results to process,
                    // therefore this is an empty result (all success).
                    demux.callback(demux.user_data, operation, &.{});
                } else {
                    const demux_index_start = @divExact(demux.offset, @sizeOf(Event));
                    const demux_index_end = demux_index_start + @divExact(demux.size, @sizeOf(Event));

                    const index_start = index_current;
                    for (results[index_start..]) |*result| {
                        // If this result is related to a next logical batch
                        // breaks the loop, calling the callback with the range processed so far.
                        if (result.index >= demux_index_end) break;
                        assert(result.index >= demux_index_start);

                        // Adjusts the index relative to the logical batch's offset:
                        result.index -= demux_index_start;
                        index_current += 1;
                    }

                    demux.callback(
                        demux.user_data,
                        operation,
                        std.mem.sliceAsBytes(results[index_start..index_current]),
                    );
                }

                head = demux.next;
                self.demux_pool.release_demux(demux);
            }
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            const ping = Header{
                .command = .ping_client,
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
                .demux_list = null,
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
