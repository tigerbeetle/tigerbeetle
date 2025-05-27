const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const format = @import("./replica_format.zig").format;

pub fn ReplicaReformatType(
    comptime StateMachine: type,
    comptime MessageBus: type,
    comptime Storage: type,
    comptime Time: type,
) type {
    const Client = vsr.ClientType(StateMachine, MessageBus, Time);
    const SuperBlock = vsr.SuperBlockType(Storage);

    return struct {
        const ReplicaReformat = @This();

        pub const Options = struct {
            format: SuperBlock.FormatOptions,
            superblock: SuperBlock.Options,
        };

        const Result = union(enum) {
            evicted: vsr.Header.Eviction.Reason,
            failed: anyerror,
            success,
        };

        allocator: std.mem.Allocator,
        options: Options,
        client: *Client,

        requests_done: u32 = 0,
        result: ?Result = null,

        pub fn init(
            allocator: std.mem.Allocator,
            client: *Client,
            options: Options,
        ) !ReplicaReformat {
            assert(options.format.view == null);
            assert(options.format.replica_count >= 3);

            return .{
                .allocator = allocator,
                .options = options,
                .client = client,
            };
        }

        pub fn deinit(reformat: *ReplicaReformat, allocator: std.mem.Allocator) void {
            _ = reformat;
            _ = allocator;
        }

        pub fn status(reformat: *const ReplicaReformat) ?Result {
            assert(reformat.requests_done <= constants.pipeline_prepare_queue_max);
            return reformat.result;
        }

        pub fn start(reformat: *ReplicaReformat) void {
            assert(reformat.requests_done == 0);
            reformat.client.register(client_register_callback, @intFromPtr(reformat));
        }

        fn client_register_callback(
            user_data: u128,
            register_result: *const vsr.RegisterResult,
        ) void {
            _ = register_result;
            const reformat: *ReplicaReformat = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(reformat.requests_done == 0);

            reformat.requests_done += 1;
            reformat.client_request();
        }

        fn client_request(reformat: *ReplicaReformat) void {
            assert(reformat.requests_done < constants.pipeline_prepare_queue_max);

            const message = reformat.client.get_message().build(.request);
            errdefer reformat.client.release_message(message.base());

            message.header.* = .{
                .client = reformat.client.id,
                .request = 0, // Set inside `raw_request`.
                .cluster = reformat.client.cluster,
                .command = .request,
                .release = reformat.client.release,
                .operation = .noop,
                .size = @intCast(@sizeOf(vsr.Header)),
            };

            const user_data = @intFromPtr(reformat);
            reformat.client.raw_request(client_request_callback, user_data, message);
        }

        fn client_request_callback(
            user_data: u128,
            operation: vsr.Operation,
            timestamp: u64,
            results: []u8,
        ) void {
            assert(operation == .noop);
            assert(timestamp > 0);

            const reformat: *ReplicaReformat = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(reformat.requests_done > 0);
            assert(reformat.requests_done < constants.pipeline_prepare_queue_max);
            assert(results.len == 0);

            reformat.requests_done += 1;
            if (reformat.requests_done == constants.pipeline_prepare_queue_max) {
                // +2 since we might have sent a DVC as part of +1 before we crashed.
                reformat.options.format.view = reformat.client.view + 2;
                format(
                    Storage,
                    reformat.allocator,
                    reformat.options.format,
                    reformat.options.superblock,
                ) catch |err| {
                    reformat.result = .{ .failed = err };
                    return;
                };
                reformat.result = .success;
            } else {
                reformat.client_request();
            }
        }
    };
}
