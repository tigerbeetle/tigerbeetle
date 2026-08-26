const std = @import("std");
const assert = std.debug.assert;

const tb_client = @import("../tb_client.zig");
const vsr = tb_client.vsr;
const constants = vsr.constants;
const multi_batch = vsr.multi_batch;
const MultiBatchEncoder = multi_batch.MultiBatchEncoder;
const MultiBatchDecoder = multi_batch.MultiBatchDecoder;

const stdx = vsr.stdx;
const maybe = stdx.maybe;

const QueueType = vsr.queue.QueueType;

pub const Packet = extern struct {
    user_data: ?*anyopaque,
    data: ?*const anyopaque,
    data_size: u32,
    user_tag: u16,
    operation: u8,
    status: Status,

    link: Queue.Link,

    multi_batch_time_monotonic: u64,
    multi_batch_next: ?*Packet,
    multi_batch_tail: ?*Packet,
    multi_batch_count: u16,
    multi_batch_event_count: u16,
    multi_batch_result_count_expected: u16,
    phase: Phase,
    reserved: [25]u8 = @splat(0),

    pub const Error = error{
        InvalidOperation,
        InvalidDataSize,
        TooMuchData,
    };

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        client_evicted,
        client_release_too_low,
        client_release_too_high,
        client_shutdown,
        invalid_operation,
        invalid_data_size,
    };

    /// External packet type exposed to the user.
    pub const Extern = extern struct {
        user_data: ?*anyopaque,
        data: ?*const anyopaque,
        data_size: u32,
        user_tag: u16,
        operation: u8,
        status: Status,
        @"opaque": [64]u8 = @splat(0),

        pub fn cast(self: *Extern) *Packet {
            return @ptrCast(self);
        }
    };

    const Phase = enum(u8) {
        submitted,
        pending,
        batched,
        sent,
        complete,
    };

    pub const Queue = QueueType(Packet);

    pub fn init(packet_extern: *const Extern) Packet {
        return .{
            .user_data = packet_extern.user_data,
            .data = packet_extern.data,
            .data_size = packet_extern.data_size,
            .user_tag = packet_extern.user_tag,
            .operation = packet_extern.operation,
            .status = .ok,
            .link = .{},
            .multi_batch_time_monotonic = 0,
            .multi_batch_next = null,
            .multi_batch_tail = null,
            .multi_batch_count = 0,
            .multi_batch_event_count = 0,
            .multi_batch_result_count_expected = 0,
            .phase = .submitted,
        };
    }

    pub fn cast(self: *Packet) *Extern {
        return @ptrCast(self);
    }

    pub fn slice(packet: *const Packet) []const u8 {
        if (packet.data_size == 0) {
            // It may be an empty array (null pointer)
            // or a buffer with no elements (valid pointer and size == 0).
            stdx.maybe(packet.data == null);
            return &[0]u8{};
        }

        const data: [*]const u8 = @ptrCast(packet.data.?);
        return data[0..packet.data_size];
    }

    /// Validates the batch's operation, data size, and expected result count.
    pub fn batch_validate(
        packet: *const Packet,
        comptime Operation: type,
        comptime operations_allowed: []const Operation,
        options: struct {
            batch_size_limit: u32,
        },
    ) Error!struct {
        operation: Operation,
        event_size: u32,
        result_size: u32,
        event_count: u32,
        result_count_expected: u32,
    } {
        comptime {
            assert(operations_allowed.len > 0);
            assert(@typeInfo(Operation) == .@"enum");
            assert(std.meta.Tag(Operation) == std.meta.Tag(vsr.Operation));
            for (operations_allowed, 0..) |operation, index| {
                const operation_vsr = vsr.Operation.from(Operation, operation);
                assert(!operation_vsr.vsr_reserved());

                assert(std.mem.indexOfScalar(
                    Operation,
                    operations_allowed[index + 1 ..],
                    operation,
                ) == null);
            }
        }

        assert(options.batch_size_limit > 0);
        assert(options.batch_size_limit <= constants.message_body_size_max);

        const operation: Operation = operation: {
            if (packet.operation < constants.vsr_operations_reserved) {
                return error.InvalidOperation;
            }

            inline for (operations_allowed) |operation| {
                if (packet.operation == @intFromEnum(operation)) {
                    break :operation operation;
                }
            }
            return error.InvalidOperation;
        };

        // Make sure the packet.data wouldn't overflow a request,
        // and that the corresponding results won't overflow a reply.
        const event_size: u32 = operation.event_size();
        assert(event_size > 0);

        const result_size: u32 = operation.result_size();
        assert(result_size > 0);

        const data: []const u8 = packet.slice();
        assert(data.len == packet.data_size);
        maybe(data.len == 0);
        if (operation.is_batchable()) {
            if (data.len % event_size != 0) return error.InvalidDataSize;
        } else {
            if (data.len != event_size) return error.InvalidDataSize;
        }

        const event_count: u32 = @intCast(@divExact(data.len, event_size));
        maybe(event_count == 0);
        assert(data.len == event_count * event_size);

        const event_max: u32 = operation.event_max(options.batch_size_limit);
        if (event_count > event_max) {
            return error.TooMuchData;
        }
        assert(data.len <= options.batch_size_limit);

        const result_max: u32 = operation.result_max(options.batch_size_limit);
        const result_count_expected: u32 = operation.result_count_expected(data);
        maybe(result_count_expected == 0);
        if (result_count_expected > result_max) {
            return error.TooMuchData;
        }

        return .{
            .operation = operation,
            .event_size = event_size,
            .result_size = result_size,
            .event_count = event_count,
            .result_count_expected = result_count_expected,
        };
    }

    /// Enqueues a packet into the target queue,
    /// grouping it into a multibatch list when applicable.
    pub fn batch_enqueue(
        packet: *Packet,
        comptime Operation: type,
        comptime operations_allowed: []const Operation,
        options: struct {
            target: *Packet.Queue,
            batch_size_limit: u32,
            time: vsr.time.Time,
        },
    ) Error!void {
        packet.assert_phase(.submitted);

        assert(options.batch_size_limit > 0);
        assert(options.batch_size_limit <= constants.message_body_size_max);
        maybe(options.target.empty());

        const batch = try packet.batch_validate(Operation, operations_allowed, .{
            .batch_size_limit = options.batch_size_limit,
        });
        errdefer comptime unreachable;
        defer assert(!options.target.empty());

        if (batch.operation.is_multi_batch()) {
            var it = options.target.iterate();
            while (it.next()) |root| {
                root.assert_phase(.pending);

                if (root.operation != packet.operation) continue;

                // Check if the message has enough space for the submitted number of events:
                const request_size: u32 = size: {
                    const trailer_size = multi_batch.trailer_total_size(.{
                        .element_size = batch.event_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch.event_count +
                        root.multi_batch_event_count;
                    break :size (event_count * batch.event_size) + trailer_size;
                };
                if (request_size > options.batch_size_limit) continue;

                // Check if the reply has enough space for the maximum expected number of results:
                const reply_size_expected: u32 = size: {
                    const trailer_size = multi_batch.trailer_total_size(.{
                        .element_size = batch.result_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch.result_count_expected +
                        root.multi_batch_result_count_expected;
                    break :size (event_count * batch.result_size) + trailer_size;
                };
                if (reply_size_expected > constants.message_body_size_max) continue;

                packet.phase = .batched;
                if (root.multi_batch_next == null) {
                    assert(root.multi_batch_tail == null);
                    assert(root.multi_batch_count == 1);
                    root.multi_batch_next = packet;
                    root.multi_batch_tail = packet;
                } else {
                    assert(root.multi_batch_tail != null);
                    assert(root.multi_batch_count > 1);
                    root.multi_batch_tail.?.multi_batch_next = packet;
                    root.multi_batch_tail = packet;
                }
                root.multi_batch_count += 1;
                root.multi_batch_event_count += @intCast(batch.event_count);
                root.multi_batch_result_count_expected += @intCast(batch.result_count_expected);
                return;
            }
        }

        // Couldn't batch with existing packet so push to pending directly.
        packet.phase = .pending;
        packet.multi_batch_time_monotonic = options.time.monotonic().ns;
        packet.multi_batch_count = 1;
        packet.multi_batch_event_count = @intCast(batch.event_count);
        packet.multi_batch_result_count_expected = @intCast(batch.result_count_expected);
        options.target.push(packet);
    }

    /// Writes the contents of `packet.data` to the output buffer,
    /// encoding them as a multibatch if necessary.
    pub fn batch_write(
        packet_list: *Packet,
        comptime Operation: type,
        comptime operations_allowed: []const Operation,
        options: struct {
            output_buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8,
            batch_size_limit: u32,
        },
    ) struct {
        operation: Operation,
        request_size: u32,
    } {
        packet_list.assert_phase(.pending);

        assert(options.batch_size_limit > 0);
        assert(options.batch_size_limit <= constants.message_body_size_max);

        const batch = packet_list.batch_validate(
            Operation,
            operations_allowed,
            .{
                .batch_size_limit = options.batch_size_limit,
            },
        ) catch unreachable; // Already validated.

        if (!batch.operation.is_multi_batch()) {
            assert(packet_list.multi_batch_next == null);
            const source: []const u8 = packet_list.slice();
            assert(source.len % batch.event_size == 0);
            assert(source.len <= options.batch_size_limit);
            stdx.copy_disjoint(
                .inexact,
                u8,
                options.output_buffer,
                source,
            );
            return .{
                .operation = batch.operation,
                .request_size = @intCast(source.len),
            };
        }
        assert(batch.operation.is_multi_batch());

        var message_encoder = MultiBatchEncoder.init(options.output_buffer, .{
            .element_size = batch.event_size,
        });

        var it: ?*Packet = packet_list;
        var multi_batch_event_count: u32 = 0;
        var multi_batch_result_count_expected: u32 = 0;
        while (it) |packet_next| {
            if (packet_next != packet_list) packet_next.assert_phase(.batched);
            assert(packet_next.operation == packet_list.operation);
            it = packet_next.multi_batch_next;

            const batch_next = packet_next.batch_validate(
                Operation,
                operations_allowed,
                .{
                    .batch_size_limit = options.batch_size_limit,
                },
            ) catch unreachable; // Already validated.

            const source: []const u8 = packet_next.slice();
            assert(source.len % batch_next.event_size == 0);
            const target = message_encoder.writable().?;
            assert(target.len >= source.len);
            stdx.copy_disjoint(
                .exact,
                u8,
                target[0..source.len],
                source,
            );
            message_encoder.add(@intCast(source.len));
            multi_batch_event_count += batch_next.event_count;
            multi_batch_result_count_expected += batch_next.result_count_expected;
        }
        assert(message_encoder.batch_count == packet_list.multi_batch_count);
        assert(multi_batch_event_count == packet_list.multi_batch_event_count);
        assert(multi_batch_result_count_expected == packet_list.multi_batch_result_count_expected);

        // Check if the reply has enough space for the maximum expected number of results.
        const trailer_size = multi_batch.trailer_total_size(.{
            .element_size = batch.result_size,
            .batch_count = packet_list.multi_batch_count,
        });
        const reply_size_max: u32 = (batch.result_size *
            packet_list.multi_batch_result_count_expected) + trailer_size;
        assert(reply_size_max % batch.result_size == 0);
        assert(reply_size_max <= constants.message_body_size_max);

        const request_size = message_encoder.finish();
        assert(request_size % batch.event_size == 0);
        assert(request_size <= options.batch_size_limit);

        return .{
            .operation = batch.operation,
            .request_size = request_size,
        };
    }

    /// Asserts the internal state of the packet according to its expected phase.
    /// Inline function, so `expected` can be comptime known.
    pub inline fn assert_phase(packet: *const Packet, expected: Phase) void {
        assert(packet.phase == expected);
        assert(packet.data_size == 0 or packet.data != null);
        assert(stdx.zeroed(&packet.reserved));
        maybe(packet.user_data == null);
        maybe(packet.user_tag == 0);

        switch (expected) {
            .submitted => {
                assert(packet.link.next == null);
                assert(packet.multi_batch_next == null);
                assert(packet.multi_batch_tail == null);
                assert(packet.multi_batch_count == 0);
                assert(packet.multi_batch_event_count == 0);
                assert(packet.multi_batch_result_count_expected == 0);
                assert(packet.multi_batch_time_monotonic == 0);
            },
            .pending => {
                assert(packet.multi_batch_count >= 1);
                assert(packet.multi_batch_next == null or packet.multi_batch_count > 1);
                assert((packet.multi_batch_next == null) == (packet.multi_batch_tail == null));
                maybe(packet.data_size == 0);
                maybe(packet.multi_batch_event_count == 0);
                maybe(packet.multi_batch_result_count_expected == 0);
                maybe(packet.link.next == null);
                assert(packet.multi_batch_time_monotonic != 0);
            },
            .batched => {
                assert(packet.link.next == null);
                assert(packet.multi_batch_tail == null);
                assert(packet.multi_batch_count == 0);
                assert(packet.multi_batch_event_count == 0);
                assert(packet.multi_batch_result_count_expected == 0);
                maybe(packet.multi_batch_next != null);
                assert(packet.multi_batch_time_monotonic == 0);
            },
            .sent => {
                assert(packet.link.next == null);
                assert(packet.multi_batch_count > 0);
                assert(packet.multi_batch_next == null or packet.multi_batch_count > 1);
                assert((packet.multi_batch_next == null) == (packet.multi_batch_tail == null));
                maybe(packet.multi_batch_event_count == 0);
                maybe(packet.multi_batch_result_count_expected == 0);
                assert(packet.multi_batch_time_monotonic != 0);
            },
            .complete => {
                // The packet pointer isn't available after completed,
                // it may be deallocated by the user;
                unreachable;
            },
        }
    }

    comptime {
        assert(@sizeOf(Extern) % @alignOf(Extern) == 0);
        assert(@alignOf(Extern) == 8);

        assert(@sizeOf(Packet) == @sizeOf(Extern));
        assert(@alignOf(Packet) == @alignOf(Extern));

        // Asserting the fields are identical.
        for (std.meta.fields(Extern)) |field_extern| {
            if (std.mem.eql(u8, field_extern.name, "opaque")) continue;
            const field_packet = std.meta.fields(Packet)[
                std.meta.fieldIndex(
                    Packet,
                    field_extern.name,
                ).?
            ];
            assert(field_packet.type == field_extern.type);
            assert(field_packet.alignment == field_extern.alignment);
            assert(@offsetOf(Packet, field_extern.name) ==
                @offsetOf(Extern, field_extern.name));
        }
    }
};

const testing = std.testing;
const fixtures = @import("../../../testing/fixtures.zig");

const TestOperation = enum(u8) {
    deprecated = constants.vsr_operations_reserved + 0,
    create = constants.vsr_operations_reserved + 1,
    query = constants.vsr_operations_reserved + 2,

    pub fn EventType(comptime operation: TestOperation) type {
        return switch (operation) {
            .deprecated, .create => extern struct { reserved: [128]u8 = @splat(0) },
            .query => extern struct { limit: u32 },
        };
    }

    pub fn ResultType(comptime operation: TestOperation) type {
        return switch (operation) {
            .deprecated, .create => extern struct { reserved: [64]u8 = @splat(0) },
            .query => extern struct { reserved: [128]u8 = @splat(0) },
        };
    }

    pub fn is_batchable(operation: TestOperation) bool {
        return switch (operation) {
            .deprecated, .create => true,
            .query => false,
        };
    }

    pub fn is_multi_batch(operation: TestOperation) bool {
        return switch (operation) {
            .deprecated => false,
            .create, .query => true,
        };
    }

    pub fn event_size(operation: TestOperation) u32 {
        return switch (operation) {
            inline else => |operation_comptime| @sizeOf(operation_comptime.EventType()),
        };
    }

    pub fn result_size(operation: TestOperation) u32 {
        return switch (operation) {
            inline else => |operation_comptime| @sizeOf(operation_comptime.ResultType()),
        };
    }

    pub fn event_max(operation: TestOperation, batch_size_limit: u32) u32 {
        return multi_batch.event_max(TestOperation, operation, batch_size_limit);
    }

    pub fn result_max(operation: TestOperation, batch_size_limit: u32) u32 {
        return multi_batch.result_max(TestOperation, operation, batch_size_limit);
    }

    pub inline fn result_count_expected(operation: TestOperation, batch: []const u8) u32 {
        return switch (operation) {
            .deprecated,
            .create,
            => @intCast(@divExact(batch.len, operation.event_size())),
            .query => count: {
                const filter = std.mem.bytesAsValue(EventType(.query), batch);
                break :count filter.limit;
            },
        };
    }

    pub fn from_vsr(operation: vsr.Operation) ?TestOperation {
        if (operation.vsr_reserved()) return null;

        return vsr.Operation.to(TestOperation, operation);
    }

    pub fn to_vsr(operation: TestOperation) vsr.Operation {
        return vsr.Operation.from(TestOperation, operation);
    }
};

test "batch_validate" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    // Event count == Result count.
    // Single event.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        );
        try testing.expectEqual(TestOperation.create, result.operation);
        try testing.expectEqual(@as(u32, 128), result.event_size);
        try testing.expectEqual(@as(u32, 64), result.result_size);
        try testing.expectEqual(@as(u32, 1), result.event_count);
        try testing.expectEqual(@as(u32, 1), result.result_count_expected);
    }

    // Event count == Result count.
    // Many events.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        );
        try testing.expectEqual(TestOperation.create, result.operation);
        try testing.expectEqual(@as(u32, 128), result.event_size);
        try testing.expectEqual(@as(u32, 64), result.result_size);
        try testing.expectEqual(@as(u32, 10), result.event_count);
        try testing.expectEqual(@as(u32, 10), result.result_count_expected);
    }

    // Event count == Result count.
    // Zero events.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        );
        try testing.expectEqual(TestOperation.create, result.operation);
        try testing.expectEqual(@as(u32, 128), result.event_size);
        try testing.expectEqual(@as(u32, 64), result.result_size);
        try testing.expectEqual(@as(u32, 0), result.event_count);
        try testing.expectEqual(@as(u32, 0), result.result_count_expected);
    }

    // Event count != Result count.
    // `batch_size_limit` don't affect the reply.
    {
        const QueryFilter = TestOperation.query.EventType();
        const filter: QueryFilter = .{
            .limit = 20,
        };
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        const result = try packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = TestOperation.query.result_size() * 10 },
        );
        try testing.expectEqual(TestOperation.query, result.operation);
        try testing.expectEqual(@as(u32, 4), result.event_size);
        try testing.expectEqual(@as(u32, 128), result.result_size);
        try testing.expectEqual(@as(u32, 1), result.event_count);
        try testing.expectEqual(filter.limit, result.result_count_expected);
    }

    // Event count == Result count.
    // `data_size` larger than `batch_size_limit`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try testing.expectError(error.TooMuchData, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = TestOperation.create.event_size() * 9 },
        ));
    }

    // Event count != Result count.
    // `result_count_expected` larger than the `batch_size_limit`.
    {
        const QueryFilter = TestOperation.query.EventType();
        const filter: QueryFilter = .{
            .limit = 100_000,
        };
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.TooMuchData, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    // More than one event per batch when `is_batchable == false`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() * 2,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    // Zero events when `is_batchable == false`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Reserved operation.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 0,
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid operation.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 199,
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Operation not allowed.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.deprecated),
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, packet.batch_validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }
}

test "batch_enqueue: multibatch event_count" {
    const operations_allowed: []const TestOperation = &.{.create};

    // batch_size_limit is 10 events,
    // but with the multibatch encoding, it's 9 events at most.
    const batch_size_limit = TestOperation.create.event_size() * 10;

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Packet.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 event.
    var packet_1: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 1,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });

    try packet_1.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_result_count_expected);
    try testing.expect(packet_1.multi_batch_next == null);
    try testing.expect(packet_1.multi_batch_tail == null);
    try testing.expect(packet_1.multi_batch_time_monotonic != 0);

    // Next batch, 4 events.
    // Merged with the previous batch.
    var packet_2: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 4,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try packet_2.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.batched, packet_2.phase);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_result_count_expected);
    try testing.expect(packet_2.multi_batch_next == null);
    try testing.expect(packet_2.multi_batch_tail == null);
    try testing.expect(packet_2.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 2), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 4), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 4), packet_1.multi_batch_result_count_expected);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_next.?);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_tail.?);

    // Next batch, 5 events.
    // There's no room to merge.
    var packet_3: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 5,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try packet_3.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_3.phase);
    try testing.expectEqual(@as(u32, 1), packet_3.multi_batch_count);
    try testing.expectEqual(@as(u32, 5), packet_3.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 5), packet_3.multi_batch_result_count_expected);
    try testing.expect(packet_3.multi_batch_next == null);
    try testing.expect(packet_3.multi_batch_tail == null);
    try testing.expect(packet_3.multi_batch_time_monotonic != 0);

    // Next batch, 2 events.
    // It can be merged with the first batch.
    var packet_4: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 2,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try packet_4.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.batched, packet_4.phase);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_result_count_expected);
    try testing.expect(packet_4.multi_batch_next == null);
    try testing.expect(packet_2.multi_batch_tail == null);
    try testing.expect(packet_4.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 3), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 4 + 2), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 4 + 2), packet_1.multi_batch_result_count_expected);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_next.?);
    try testing.expectEqual(&packet_4, packet_2.multi_batch_next.?);
    try testing.expectEqual(&packet_4, packet_1.multi_batch_tail.?);
}

test "batch_enqueue: multibatch result_expected_count" {
    const QueryFilter = TestOperation.query.EventType();
    const operations_allowed: []const TestOperation = &.{.query};

    // The maximum number of results:
    const result_max = TestOperation.query.result_max(constants.message_body_size_max);

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Packet.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 result.
    var packet_1: Packet = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = 1 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });

    try packet_1.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_result_count_expected);
    try testing.expect(packet_1.multi_batch_next == null);
    try testing.expect(packet_1.multi_batch_tail == null);
    try testing.expect(packet_1.multi_batch_time_monotonic != 0);

    // Next batch, 2 results.
    // Merged with the previous batch.
    var packet_2: Packet = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = 2 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try packet_2.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.batched, packet_2.phase);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), packet_2.multi_batch_result_count_expected);
    try testing.expect(packet_2.multi_batch_next == null);
    try testing.expect(packet_2.multi_batch_tail == null);
    try testing.expect(packet_2.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 2), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 1), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 2), packet_1.multi_batch_result_count_expected);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_next.?);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_tail.?);

    // Next batch, max results.
    // There's no room to merge.
    var packet_3: Packet = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = result_max },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try packet_3.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_3.phase);
    try testing.expectEqual(@as(u32, 1), packet_3.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), packet_3.multi_batch_event_count);
    try testing.expectEqual(result_max, packet_3.multi_batch_result_count_expected);
    try testing.expect(packet_3.multi_batch_next == null);
    try testing.expect(packet_3.multi_batch_tail == null);
    try testing.expect(packet_3.multi_batch_time_monotonic != 0);

    // Next batch, the remaining results.
    // It can be merged with the first batch.
    var packet_4: Packet = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = result_max - 3 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try packet_4.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.batched, packet_4.phase);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), packet_4.multi_batch_result_count_expected);
    try testing.expect(packet_4.multi_batch_next == null);
    try testing.expect(packet_2.multi_batch_tail == null);
    try testing.expect(packet_4.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 3), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 1 + 1), packet_1.multi_batch_event_count);
    try testing.expectEqual(result_max, packet_1.multi_batch_result_count_expected);
    try testing.expectEqual(&packet_2, packet_1.multi_batch_next.?);
    try testing.expectEqual(&packet_4, packet_2.multi_batch_next.?);
    try testing.expectEqual(&packet_4, packet_1.multi_batch_tail.?);
}

test "batch_enqueue: no multibatch" {
    const operations_allowed: []const TestOperation = &.{.deprecated};

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Packet.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 event.
    var packet_1: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.deprecated.event_size(),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try packet_1.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_1.phase);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), packet_1.multi_batch_result_count_expected);
    try testing.expect(packet_1.multi_batch_next == null);
    try testing.expect(packet_1.multi_batch_tail == null);
    try testing.expect(packet_1.multi_batch_time_monotonic != 0);

    // Next batch, 1 event.
    // This operation does not support multibatching.
    var packet_2: Packet = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.deprecated.event_size(),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try packet_2.batch_enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        },
    );
    try testing.expectEqual(Packet.Phase.pending, packet_2.phase);
    try testing.expectEqual(@as(u32, 1), packet_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), packet_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), packet_2.multi_batch_result_count_expected);
    try testing.expect(packet_2.multi_batch_next == null);
    try testing.expect(packet_2.multi_batch_tail == null);
    try testing.expect(packet_2.multi_batch_time_monotonic != 0);
}

test "batch_enqueue: batch_validate" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    // Event count == Result count.
    // `data_size` larger than `batch_size_limit`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.TooMuchData, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = TestOperation.create.event_size() * 9,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Event count != Result count.
    // `result_count_expected` larger than the `batch_size_limit`.
    {
        const QueryFilter = TestOperation.query.EventType();
        const filter: QueryFilter = .{
            .limit = 100_000,
        };
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.TooMuchData, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    // More than one event per batch when `is_batchable == false`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() * 2,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    // Zero events when `is_batchable == false`.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Reserved operation.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 0,
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid operation.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 199,
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Operation not allowed.
    {
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.deprecated),
            .status = .ok,
        });

        var queue: Packet.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, packet.batch_enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.time(),
            },
        ));
        try testing.expect(queue.empty());
    }
}

test "batch_write: multibatch" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8 =
        @ptrCast(try testing.allocator.alignedAlloc(
            u8,
            constants.cache_line_size,
            constants.message_body_size_max,
        ));
    defer testing.allocator.free(buffer);

    // Single batch.
    {
        var queue: Packet.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        const Event = TestOperation.create.EventType();
        const event: Event = .{ .reserved = @splat(42) };
        var packet: Packet = .init(&.{
            .user_data = null,
            .data = &event,
            .data_size = @sizeOf(Event),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try packet.batch_enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        });

        const result = packet.batch_write(TestOperation, operations_allowed, .{
            .output_buffer = buffer,
            .batch_size_limit = constants.message_body_size_max,
        });

        try testing.expectEqual(TestOperation.create, result.operation);
        var decoder: MultiBatchDecoder = try .init(buffer[0..result.request_size], .{
            .element_size = TestOperation.create.event_size(),
        });

        try testing.expectEqual(@as(u32, 1), decoder.batch_count());
        try testing.expectEqualSlices(u8, std.mem.asBytes(&event), decoder.pop().?);
        try testing.expect(decoder.pop() == null);
    }

    // Multiple batches.
    {
        var queue: Packet.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        const QueryFilter = TestOperation.query.EventType();
        const event_1: QueryFilter = .{ .limit = 4 };
        var packet_1: Packet = .init(&.{
            .user_data = null,
            .data = &event_1,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try packet_1.batch_enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        });
        try testing.expectEqual(Packet.Phase.pending, packet_1.phase);

        const event_2: QueryFilter = .{ .limit = 5 };
        var packet_2: Packet = .init(&.{
            .user_data = null,
            .data = &event_2,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try packet_2.batch_enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        });
        try testing.expectEqual(Packet.Phase.batched, packet_2.phase);

        const result = packet_1.batch_write(TestOperation, operations_allowed, .{
            .output_buffer = buffer,
            .batch_size_limit = constants.message_body_size_max,
        });

        try testing.expectEqual(TestOperation.query, result.operation);
        var decoder: MultiBatchDecoder = try .init(buffer[0..result.request_size], .{
            .element_size = TestOperation.query.event_size(),
        });

        try testing.expectEqual(@as(u32, 2), decoder.batch_count());
        try testing.expectEqualSlices(u8, std.mem.asBytes(&event_1), decoder.pop().?);
        try testing.expectEqualSlices(u8, std.mem.asBytes(&event_2), decoder.pop().?);
        try testing.expect(decoder.pop() == null);
    }

    // Zero size.
    {
        var queue: Packet.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        var packet: Packet = .init(&.{
            .user_data = null,
            .data = null,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try packet.batch_enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.time(),
        });

        const result = packet.batch_write(TestOperation, operations_allowed, .{
            .output_buffer = buffer,
            .batch_size_limit = constants.message_body_size_max,
        });

        try testing.expectEqual(TestOperation.create, result.operation);
        var decoder: MultiBatchDecoder = try .init(buffer[0..result.request_size], .{
            .element_size = TestOperation.create.event_size(),
        });

        try testing.expectEqual(@as(u32, 1), decoder.batch_count());
        try testing.expectEqualSlices(u8, &[0]u8{}, decoder.pop().?);
        try testing.expect(decoder.pop() == null);
    }
}

test "batch_write: no multibatch" {
    const operations_allowed: []const TestOperation = &.{.deprecated};

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Packet.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    const Event = TestOperation.deprecated.EventType();
    const event: Event = .{ .reserved = @splat(42) };
    var packet: Packet = .init(&.{
        .user_data = null,
        .data = &event,
        .data_size = @sizeOf(Event),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try packet.batch_enqueue(TestOperation, operations_allowed, .{
        .target = &queue,
        .batch_size_limit = constants.message_body_size_max,
        .time = time_sim.time(),
    });

    var buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8 =
        @ptrCast(try testing.allocator.alignedAlloc(
            u8,
            constants.cache_line_size,
            constants.message_body_size_max,
        ));
    defer testing.allocator.free(buffer);

    const result = packet.batch_write(TestOperation, operations_allowed, .{
        .output_buffer = buffer,
        .batch_size_limit = constants.message_body_size_max,
    });

    try testing.expectEqual(TestOperation.deprecated, result.operation);
    try testing.expect(result.request_size == @sizeOf(Event));
    try testing.expectEqualSlices(u8, std.mem.asBytes(&event), buffer[0..result.request_size]);
}
