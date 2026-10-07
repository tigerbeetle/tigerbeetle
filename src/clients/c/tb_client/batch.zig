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

pub const Batch = extern struct {
    user_data: ?*anyopaque,
    data: ?*const anyopaque,
    data_size: u32,
    user_tag: u16,
    operation: u8,
    status: Status,

    link: Queue.Link,

    multi_batch_time_monotonic: u64,
    multi_batch_next: ?*Batch,
    multi_batch_tail: ?*Batch,
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
        client_closed,
        invalid_operation,
        invalid_data_size,
    };

    /// External batch type exposed to the user.
    pub const Extern = extern struct {
        user_data: ?*anyopaque,
        data: ?*const anyopaque,
        data_size: u32,
        user_tag: u16,
        operation: u8,
        status: Status,
        @"opaque": [64]u8 = @splat(0),

        pub fn cast(self: *Extern) *Batch {
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

    pub const Queue = QueueType(Batch);

    pub fn init(batch_extern: *const Extern) Batch {
        return .{
            .user_data = batch_extern.user_data,
            .data = batch_extern.data,
            .data_size = batch_extern.data_size,
            .user_tag = batch_extern.user_tag,
            .operation = batch_extern.operation,
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

    pub fn cast(self: *Batch) *Extern {
        return @ptrCast(self);
    }

    pub fn slice(batch: *const Batch) []const u8 {
        if (batch.data_size == 0) {
            // It may be an empty array (null pointer)
            // or a buffer with no elements (valid pointer and size == 0).
            stdx.maybe(batch.data == null);
            return &[0]u8{};
        }

        const data: [*]const u8 = @ptrCast(batch.data.?);
        return data[0..batch.data_size];
    }

    /// Validates the batch's operation, data size, and expected result count.
    pub fn validate(
        batch: *const Batch,
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
            if (batch.operation < constants.vsr_operations_reserved) {
                return error.InvalidOperation;
            }

            inline for (operations_allowed) |operation| {
                if (batch.operation == @intFromEnum(operation)) {
                    break :operation operation;
                }
            }
            return error.InvalidOperation;
        };

        // Make sure the batch.data wouldn't overflow a request,
        // and that the corresponding results won't overflow a reply.
        const event_size: u32 = operation.event_size();
        assert(event_size > 0);

        const result_size: u32 = operation.result_size();
        assert(result_size > 0);

        const data: []const u8 = batch.slice();
        assert(data.len == batch.data_size);
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

    /// Enqueues a batch into the target queue,
    /// grouping it into a multibatch list when applicable.
    pub fn enqueue(
        batch: *Batch,
        comptime Operation: type,
        comptime operations_allowed: []const Operation,
        options: struct {
            target: *Batch.Queue,
            batch_size_limit: u32,
            time: stdx.Time,
        },
    ) Error!void {
        batch.assert_phase(.submitted);

        assert(options.batch_size_limit > 0);
        assert(options.batch_size_limit <= constants.message_body_size_max);
        maybe(options.target.empty());

        const batch_layout = try batch.validate(Operation, operations_allowed, .{
            .batch_size_limit = options.batch_size_limit,
        });
        errdefer comptime unreachable;
        defer assert(!options.target.empty());

        if (batch_layout.operation.is_multi_batch()) {
            var it = options.target.iterate();
            while (it.next()) |root| {
                root.assert_phase(.pending);

                if (root.operation != batch.operation) continue;

                // Check if the message has enough space for the submitted number of events:
                const request_size: u32 = size: {
                    const trailer_size = multi_batch.trailer_total_size(.{
                        .element_size = batch_layout.event_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch_layout.event_count +
                        root.multi_batch_event_count;
                    break :size (event_count * batch_layout.event_size) + trailer_size;
                };
                if (request_size > options.batch_size_limit) continue;

                // Check if the reply has enough space for the maximum expected number of results:
                const reply_size_expected: u32 = size: {
                    const trailer_size = multi_batch.trailer_total_size(.{
                        .element_size = batch_layout.result_size,
                        .batch_count = root.multi_batch_count + 1,
                    });
                    const event_count: u32 = batch_layout.result_count_expected +
                        root.multi_batch_result_count_expected;
                    break :size (event_count * batch_layout.result_size) + trailer_size;
                };
                if (reply_size_expected > constants.message_body_size_max) continue;

                batch.phase = .batched;
                if (root.multi_batch_next == null) {
                    assert(root.multi_batch_tail == null);
                    assert(root.multi_batch_count == 1);
                    root.multi_batch_next = batch;
                    root.multi_batch_tail = batch;
                } else {
                    assert(root.multi_batch_tail != null);
                    assert(root.multi_batch_count > 1);
                    root.multi_batch_tail.?.multi_batch_next = batch;
                    root.multi_batch_tail = batch;
                }
                root.multi_batch_count += 1;
                root.multi_batch_event_count += @intCast(batch_layout.event_count);
                root.multi_batch_result_count_expected += @intCast(batch_layout.result_count_expected);
                return;
            }
        }

        // Couldn't batch with existing batch so push to pending directly.
        batch.phase = .pending;
        batch.multi_batch_time_monotonic = options.time.monotonic().ns;
        batch.multi_batch_count = 1;
        batch.multi_batch_event_count = @intCast(batch_layout.event_count);
        batch.multi_batch_result_count_expected = @intCast(batch_layout.result_count_expected);
        options.target.push(batch);
    }

    /// Writes the contents of `batch.data` to the output buffer,
    /// encoding them as a multibatch if necessary.
    pub fn write(
        batch_list: *Batch,
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
        batch_list.assert_phase(.pending);

        assert(options.batch_size_limit > 0);
        assert(options.batch_size_limit <= constants.message_body_size_max);

        const batch_layout = batch_list.validate(
            Operation,
            operations_allowed,
            .{
                .batch_size_limit = options.batch_size_limit,
            },
        ) catch unreachable; // Already validated.

        if (!batch_layout.operation.is_multi_batch()) {
            assert(batch_list.multi_batch_next == null);
            const source: []const u8 = batch_list.slice();
            assert(source.len % batch_layout.event_size == 0);
            assert(source.len <= options.batch_size_limit);
            stdx.copy_disjoint(
                .inexact,
                u8,
                options.output_buffer,
                source,
            );
            return .{
                .operation = batch_layout.operation,
                .request_size = @intCast(source.len),
            };
        }
        assert(batch_layout.operation.is_multi_batch());

        var message_encoder = MultiBatchEncoder.init(options.output_buffer, .{
            .element_size = batch_layout.event_size,
        });

        var it: ?*Batch = batch_list;
        var multi_batch_event_count: u32 = 0;
        var multi_batch_result_count_expected: u32 = 0;
        while (it) |batch_next| {
            if (batch_next != batch_list) batch_next.assert_phase(.batched);
            assert(batch_next.operation == batch_list.operation);
            it = batch_next.multi_batch_next;

            const batch_next_layout = batch_next.validate(
                Operation,
                operations_allowed,
                .{
                    .batch_size_limit = options.batch_size_limit,
                },
            ) catch unreachable; // Already validated.

            const source: []const u8 = batch_next.slice();
            assert(source.len % batch_next_layout.event_size == 0);
            const target = message_encoder.writable().?;
            assert(target.len >= source.len);
            stdx.copy_disjoint(
                .exact,
                u8,
                target[0..source.len],
                source,
            );
            message_encoder.add(@intCast(source.len));
            multi_batch_event_count += batch_next_layout.event_count;
            multi_batch_result_count_expected += batch_next_layout.result_count_expected;
        }
        assert(message_encoder.batch_count == batch_list.multi_batch_count);
        assert(multi_batch_event_count == batch_list.multi_batch_event_count);
        assert(multi_batch_result_count_expected == batch_list.multi_batch_result_count_expected);

        // Check if the reply has enough space for the maximum expected number of results.
        const trailer_size = multi_batch.trailer_total_size(.{
            .element_size = batch_layout.result_size,
            .batch_count = batch_list.multi_batch_count,
        });
        const reply_size_max: u32 = (batch_layout.result_size *
            batch_list.multi_batch_result_count_expected) + trailer_size;
        assert(reply_size_max % batch_layout.result_size == 0);
        assert(reply_size_max <= constants.message_body_size_max);

        const request_size = message_encoder.finish();
        assert(request_size % batch_layout.event_size == 0);
        assert(request_size <= options.batch_size_limit);

        return .{
            .operation = batch_layout.operation,
            .request_size = request_size,
        };
    }

    /// Asserts the internal state of the batch according to its expected phase.
    /// Inline function, so `expected` can be comptime known.
    pub inline fn assert_phase(batch: *const Batch, expected: Phase) void {
        assert(batch.phase == expected);
        assert(batch.data_size == 0 or batch.data != null);
        assert(stdx.zeroed(&batch.reserved));
        maybe(batch.user_data == null);
        maybe(batch.user_tag == 0);

        switch (expected) {
            .submitted => {
                assert(batch.link.next == null);
                assert(batch.multi_batch_next == null);
                assert(batch.multi_batch_tail == null);
                assert(batch.multi_batch_count == 0);
                assert(batch.multi_batch_event_count == 0);
                assert(batch.multi_batch_result_count_expected == 0);
                assert(batch.multi_batch_time_monotonic == 0);
            },
            .pending => {
                assert(batch.multi_batch_count >= 1);
                assert(batch.multi_batch_next == null or batch.multi_batch_count > 1);
                assert((batch.multi_batch_next == null) == (batch.multi_batch_tail == null));
                maybe(batch.data_size == 0);
                maybe(batch.multi_batch_event_count == 0);
                maybe(batch.multi_batch_result_count_expected == 0);
                maybe(batch.link.next == null);
                assert(batch.multi_batch_time_monotonic != 0);
            },
            .batched => {
                assert(batch.link.next == null);
                assert(batch.multi_batch_tail == null);
                assert(batch.multi_batch_count == 0);
                assert(batch.multi_batch_event_count == 0);
                assert(batch.multi_batch_result_count_expected == 0);
                maybe(batch.multi_batch_next != null);
                assert(batch.multi_batch_time_monotonic == 0);
            },
            .sent => {
                assert(batch.link.next == null);
                assert(batch.multi_batch_count > 0);
                assert(batch.multi_batch_next == null or batch.multi_batch_count > 1);
                assert((batch.multi_batch_next == null) == (batch.multi_batch_tail == null));
                maybe(batch.multi_batch_event_count == 0);
                maybe(batch.multi_batch_result_count_expected == 0);
                assert(batch.multi_batch_time_monotonic != 0);
            },
            .complete => {
                // The batch pointer isn't available after completed,
                // it may be deallocated by the user;
                unreachable;
            },
        }
    }

    comptime {
        assert(@sizeOf(Extern) % @alignOf(Extern) == 0);
        assert(@alignOf(Extern) == 8);

        assert(@sizeOf(Batch) == @sizeOf(Extern));
        assert(@alignOf(Batch) == @alignOf(Extern));

        // Asserting the fields are identical.
        for (std.meta.fields(Extern)) |field_extern| {
            if (std.mem.eql(u8, field_extern.name, "opaque")) continue;
            const field_batch = std.meta.fields(Batch)[
                std.meta.fieldIndex(
                    Batch,
                    field_extern.name,
                ).?
            ];
            assert(field_batch.type == field_extern.type);
            assert(field_batch.alignment == field_extern.alignment);
            assert(@offsetOf(Batch, field_extern.name) ==
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

test "batch validate" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    // Event count == Result count.
    // Single event.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try batch.validate(
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try batch.validate(
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        const result = try batch.validate(
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        const result = try batch.validate(
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try testing.expectError(error.TooMuchData, batch.validate(
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.TooMuchData, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    // More than one event per batch when `is_batchable == false`.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() * 2,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid data size.
    // Zero events when `is_batchable == false`.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try testing.expectError(error.InvalidDataSize, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Reserved operation.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 0,
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Invalid operation.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 199,
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }

    // Operation not allowed.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.deprecated),
            .status = .ok,
        });

        try testing.expectError(error.InvalidOperation, batch.validate(
            TestOperation,
            operations_allowed,
            .{ .batch_size_limit = constants.message_body_size_max },
        ));
    }
}

test "batch enqueue: multibatch event_count" {
    const operations_allowed: []const TestOperation = &.{.create};

    // batch_size_limit is 10 events,
    // but with the multibatch encoding, it's 9 events at most.
    const batch_size_limit = TestOperation.create.event_size() * 10;

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Batch.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 event.
    var batch_1: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 1,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });

    try batch_1.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_result_count_expected);
    try testing.expect(batch_1.multi_batch_next == null);
    try testing.expect(batch_1.multi_batch_tail == null);
    try testing.expect(batch_1.multi_batch_time_monotonic != 0);

    // Next batch, 4 events.
    // Merged with the previous batch.
    var batch_2: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 4,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try batch_2.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.batched, batch_2.phase);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_result_count_expected);
    try testing.expect(batch_2.multi_batch_next == null);
    try testing.expect(batch_2.multi_batch_tail == null);
    try testing.expect(batch_2.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 2), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 4), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 4), batch_1.multi_batch_result_count_expected);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_next.?);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_tail.?);

    // Next batch, 5 events.
    // There's no room to merge.
    var batch_3: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 5,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try batch_3.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_3.phase);
    try testing.expectEqual(@as(u32, 1), batch_3.multi_batch_count);
    try testing.expectEqual(@as(u32, 5), batch_3.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 5), batch_3.multi_batch_result_count_expected);
    try testing.expect(batch_3.multi_batch_next == null);
    try testing.expect(batch_3.multi_batch_tail == null);
    try testing.expect(batch_3.multi_batch_time_monotonic != 0);

    // Next batch, 2 events.
    // It can be merged with the first batch.
    var batch_4: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.create.event_size() * 2,
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.create),
        .status = .ok,
    });
    try batch_4.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = batch_size_limit,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.batched, batch_4.phase);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_result_count_expected);
    try testing.expect(batch_4.multi_batch_next == null);
    try testing.expect(batch_2.multi_batch_tail == null);
    try testing.expect(batch_4.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 3), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 4 + 2), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 4 + 2), batch_1.multi_batch_result_count_expected);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_next.?);
    try testing.expectEqual(&batch_4, batch_2.multi_batch_next.?);
    try testing.expectEqual(&batch_4, batch_1.multi_batch_tail.?);
}

test "batch enqueue: multibatch result_expected_count" {
    const QueryFilter = TestOperation.query.EventType();
    const operations_allowed: []const TestOperation = &.{.query};

    // The maximum number of results:
    const result_max = TestOperation.query.result_max(constants.message_body_size_max);

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Batch.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 result.
    var batch_1: Batch = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = 1 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });

    try batch_1.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_result_count_expected);
    try testing.expect(batch_1.multi_batch_next == null);
    try testing.expect(batch_1.multi_batch_tail == null);
    try testing.expect(batch_1.multi_batch_time_monotonic != 0);

    // Next batch, 2 results.
    // Merged with the previous batch.
    var batch_2: Batch = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = 2 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try batch_2.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.batched, batch_2.phase);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), batch_2.multi_batch_result_count_expected);
    try testing.expect(batch_2.multi_batch_next == null);
    try testing.expect(batch_2.multi_batch_tail == null);
    try testing.expect(batch_2.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 2), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 1), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1 + 2), batch_1.multi_batch_result_count_expected);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_next.?);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_tail.?);

    // Next batch, max results.
    // There's no room to merge.
    var batch_3: Batch = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = result_max },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try batch_3.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_3.phase);
    try testing.expectEqual(@as(u32, 1), batch_3.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), batch_3.multi_batch_event_count);
    try testing.expectEqual(result_max, batch_3.multi_batch_result_count_expected);
    try testing.expect(batch_3.multi_batch_next == null);
    try testing.expect(batch_3.multi_batch_tail == null);
    try testing.expect(batch_3.multi_batch_time_monotonic != 0);

    // Next batch, the remaining results.
    // It can be merged with the first batch.
    var batch_4: Batch = .init(&.{
        .user_data = null,
        .data = &QueryFilter{ .limit = result_max - 3 },
        .data_size = @sizeOf(QueryFilter),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.query),
        .status = .ok,
    });
    try batch_4.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.batched, batch_4.phase);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_count);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 0), batch_4.multi_batch_result_count_expected);
    try testing.expect(batch_4.multi_batch_next == null);
    try testing.expect(batch_2.multi_batch_tail == null);
    try testing.expect(batch_4.multi_batch_time_monotonic == 0);

    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 3), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1 + 1 + 1), batch_1.multi_batch_event_count);
    try testing.expectEqual(result_max, batch_1.multi_batch_result_count_expected);
    try testing.expectEqual(&batch_2, batch_1.multi_batch_next.?);
    try testing.expectEqual(&batch_4, batch_2.multi_batch_next.?);
    try testing.expectEqual(&batch_4, batch_1.multi_batch_tail.?);
}

test "batch enqueue: no multibatch" {
    const operations_allowed: []const TestOperation = &.{.deprecated};

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Batch.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    // First batch, 1 event.
    var batch_1: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.deprecated.event_size(),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try batch_1.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_1.phase);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), batch_1.multi_batch_result_count_expected);
    try testing.expect(batch_1.multi_batch_next == null);
    try testing.expect(batch_1.multi_batch_tail == null);
    try testing.expect(batch_1.multi_batch_time_monotonic != 0);

    // Next batch, 1 event.
    // This operation does not support multibatching.
    var batch_2: Batch = .init(&.{
        .user_data = null,
        .data = undefined,
        .data_size = TestOperation.deprecated.event_size(),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try batch_2.enqueue(
        TestOperation,
        operations_allowed,
        .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        },
    );
    try testing.expectEqual(Batch.Phase.pending, batch_2.phase);
    try testing.expectEqual(@as(u32, 1), batch_2.multi_batch_count);
    try testing.expectEqual(@as(u32, 1), batch_2.multi_batch_event_count);
    try testing.expectEqual(@as(u32, 1), batch_2.multi_batch_result_count_expected);
    try testing.expect(batch_2.multi_batch_next == null);
    try testing.expect(batch_2.multi_batch_tail == null);
    try testing.expect(batch_2.multi_batch_time_monotonic != 0);
}

test "batch enqueue: validate" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    // Event count == Result count.
    // `data_size` larger than `batch_size_limit`.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() * 10,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.TooMuchData, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = TestOperation.create.event_size() * 9,
                .time = time_sim.interface(),
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
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = &filter,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 1,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.TooMuchData, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.create.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() - 1,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    // More than one event per batch when `is_batchable == false`.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = TestOperation.query.event_size() * 2,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid data size.
    // Zero events when `is_batchable == false`.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidDataSize, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Reserved operation.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 0,
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Invalid operation.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = 199,
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }

    // Operation not allowed.
    {
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = undefined,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.deprecated),
            .status = .ok,
        });

        var queue: Batch.Queue = .init(.{ .name = "testing", .verify_push = true });
        try testing.expectError(error.InvalidOperation, batch.enqueue(
            TestOperation,
            operations_allowed,
            .{
                .target = &queue,
                .batch_size_limit = constants.message_body_size_max,
                .time = time_sim.interface(),
            },
        ));
        try testing.expect(queue.empty());
    }
}

test "batch write: multibatch" {
    const operations_allowed: []const TestOperation = &.{ .create, .query };

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8 =
        @ptrCast(try testing.allocator.alignedAlloc(
            u8,
            .fromByteUnits(constants.cache_line_size),
            constants.message_body_size_max,
        ));
    defer testing.allocator.destroy(buffer);

    // Single batch.
    {
        var queue: Batch.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        const Event = TestOperation.create.EventType();
        const event: Event = .{ .reserved = @splat(42) };
        var batch: Batch = .init(&.{
            .user_data = null,
            .data = &event,
            .data_size = @sizeOf(Event),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try batch.enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        });

        const result = batch.write(TestOperation, operations_allowed, .{
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
        var queue: Batch.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        const QueryFilter = TestOperation.query.EventType();
        const event_1: QueryFilter = .{ .limit = 4 };
        var batch_1: Batch = .init(&.{
            .user_data = null,
            .data = &event_1,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try batch_1.enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        });
        try testing.expectEqual(Batch.Phase.pending, batch_1.phase);

        const event_2: QueryFilter = .{ .limit = 5 };
        var batch_2: Batch = .init(&.{
            .user_data = null,
            .data = &event_2,
            .data_size = @sizeOf(QueryFilter),
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.query),
            .status = .ok,
        });

        try batch_2.enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        });
        try testing.expectEqual(Batch.Phase.batched, batch_2.phase);

        const result = batch_1.write(TestOperation, operations_allowed, .{
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
        var queue: Batch.Queue = .init(.{
            .name = "testing",
            .verify_push = true,
        });

        var batch: Batch = .init(&.{
            .user_data = null,
            .data = null,
            .data_size = 0,
            .user_tag = 0,
            .operation = @intFromEnum(TestOperation.create),
            .status = .ok,
        });

        try batch.enqueue(TestOperation, operations_allowed, .{
            .target = &queue,
            .batch_size_limit = constants.message_body_size_max,
            .time = time_sim.interface(),
        });

        const result = batch.write(TestOperation, operations_allowed, .{
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

test "batch write: no multibatch" {
    const operations_allowed: []const TestOperation = &.{.deprecated};

    var time_sim = fixtures.init_time(.{});
    time_sim.ticks = 1;

    var queue: Batch.Queue = .init(.{
        .name = "testing",
        .verify_push = true,
    });

    const Event = TestOperation.deprecated.EventType();
    const event: Event = .{ .reserved = @splat(42) };
    var batch: Batch = .init(&.{
        .user_data = null,
        .data = &event,
        .data_size = @sizeOf(Event),
        .user_tag = 0,
        .operation = @intFromEnum(TestOperation.deprecated),
        .status = .ok,
    });

    try batch.enqueue(TestOperation, operations_allowed, .{
        .target = &queue,
        .batch_size_limit = constants.message_body_size_max,
        .time = time_sim.interface(),
    });

    var buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8 =
        @ptrCast(try testing.allocator.alignedAlloc(
            u8,
            .fromByteUnits(constants.cache_line_size),
            constants.message_body_size_max,
        ));
    defer testing.allocator.destroy(buffer);

    const result = batch.write(TestOperation, operations_allowed, .{
        .output_buffer = buffer,
        .batch_size_limit = constants.message_body_size_max,
    });

    try testing.expectEqual(TestOperation.deprecated, result.operation);
    try testing.expect(result.request_size == @sizeOf(Event));
    try testing.expectEqualSlices(u8, std.mem.asBytes(&event), buffer[0..result.request_size]);
}
