//! Replica recovery: Format a data file to replace one which was permanently lost.
//!
//! 1. The recovery process send `pipeline_prepare_queue_max` requests (1 register + many noops) to
//!    the cluster.
//! 2. Once those have committed, it creates the new data file. The data file is identical to
//!    `tigerbeetle format`'s output *except* that `vsr_state.view == client.view + 2` (where
//!    `client.view` is the view number of the client at the end of committing the requests).
//! 3. The recovery process exits. Now running `tigerbeetle start` as normal will work.
//!
//! The `pipeline_prepare_queue_max` committed requests ensure that if the newly recovered replica
//! nacks uncommitted ops via a DVC message, it is nacking ops which were definitely not received by
//! the previous version of the replica.
//!
//! The +2 is because:
//! - We don't want to join in the same view, since the replica might have participated in it before
//!   being lost, and we can't remember any promises we made.
//! - Likewise, we don't want to go to view + 1 -- if we were the first to collect a SVC quorum
//!   before being lost, we might have sent a DVC. Since we don't remember, we must skip past
//!   `view + 11 to ensure that we don't send a different DVC. (We have the invariant that if a
//!   replica sends a DVC for a given view, then all DVC's it sends for that view will be
//!   identical.)
const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const format = @import("./replica_format.zig").format;

const log = std.log.scoped(.reformat);

pub fn ReplicaReformatType(
    comptime StateMachine: type,
    comptime MessageBus: type,
    comptime Storage: type,
) type {
    const Client = vsr.ClientType(StateMachine.Operation, MessageBus);
    const SuperBlock = vsr.SuperBlockType(Storage);

    return struct {
        const ReplicaReformat = @This();

        const Result = union(enum) {
            failed: anyerror,
            ok,
        };

        allocator: std.mem.Allocator,
        options: SuperBlock.FormatOptions,
        client: *Client,
        storage: *Storage,

        requests_done: u32 = 0,
        result: ?Result = null,

        pub fn init(
            allocator: std.mem.Allocator,
            client: *Client,
            storage: *Storage,
            options: SuperBlock.FormatOptions,
        ) !ReplicaReformat {
            assert(options.view == null);
            assert(options.replica_count >= 3);

            return .{
                .allocator = allocator,
                .options = options,
                .client = client,
                .storage = storage,
            };
        }

        pub fn deinit(reformat: *ReplicaReformat, allocator: std.mem.Allocator) void {
            _ = reformat;
            _ = allocator;
        }
        pub fn done(reformat: *const ReplicaReformat) ?Result {
            assert(reformat.requests_done <= constants.pipeline_prepare_queue_max);
            return reformat.result;
        }

        pub fn start(reformat: *ReplicaReformat) void {
            assert(reformat.requests_done == 0);
            const user_data = @intFromPtr(reformat);
            reformat.client.register(client_register_callback, user_data);
        }

        fn client_register_callback(
            user_data: u128,
            register_result: *const vsr.RegisterResult,
        ) void {
            _ = register_result;
            const reformat: *ReplicaReformat = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(reformat.requests_done == 0);

            log.debug("{}: register", .{reformat.options.replica});

            reformat.requests_done += 1;
            reformat.client_request();
        }

        fn client_request(reformat: *ReplicaReformat) void {
            assert(reformat.requests_done < constants.pipeline_prepare_queue_max);

            log.debug("{}: request start={}", .{
                reformat.options.replica,
                reformat.requests_done,
            });

            const message = reformat.client.get_message().build(.request);
            errdefer reformat.client.release_message(message.base());

            message.header.* = .{
                .client = reformat.client.id,
                .request = 0, // Set inside `raw_request`.
                .cluster = reformat.client.cluster,
                .command = .request,
                .release = reformat.client.release,
                .operation = .noop,
                .size = @sizeOf(vsr.Header),
                .previous_request_latency = 0,
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

            log.debug("{}: request done={}", .{
                reformat.options.replica,
                reformat.requests_done,
            });

            reformat.requests_done += 1;
            if (reformat.requests_done == constants.pipeline_prepare_queue_max) {
                // +2 since we might have sent a DVC as part of +1 before we crashed.
                reformat.options.view = reformat.client.view + 2;
                format(
                    Storage,
                    reformat.allocator,
                    reformat.storage,
                    reformat.options,
                ) catch |err| {
                    reformat.result = .{ .failed = err };
                    return;
                };
                reformat.result = .ok;
            } else {
                reformat.client_request();
            }
        }
    };
}
