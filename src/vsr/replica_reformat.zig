//! Replica recovery: Format a data file to replace one which was permanently lost.
//!
//! 1. The recovery process send `pipeline_prepare_queue_max` requests (1 register + many noops) to
//!    the cluster.
//! 2. Once those have committed, it creates the new data file. The data file is identical to
//!    `tigerbeetle format`'s output *except* that `vsr_state.view == client.view + 2` (where
//!    `client.view` is the view number of the client at the end of committing the requests).
//! 3. The recovery process exits. Now running `tigerbeetle start` as normal will work.
//!
//? matklad: emplain why this would be enough.
//? resolved.
//! The `pipeline_prepare_queue_max` committed requests ensure that if the newly recovered replica
//! nacks uncommitted ops via a DVC message, it is nacking ops which were definitely not received by
//! the previous version of the replica.
//!
//? matklad: explain +2, feels important enough to not bury in git blame.
//? resolved.
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
            failed: anyerror,
            //? matklad: alternative name: ok, like we use elsewhere.
            //? resolved.
            ok,
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
        //? matklad: alternative name: done, to work with if (refromat.done()) |result|.
        //? resolved.
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

            log.debug("{}: register", .{reformat.options.format.replica});

            reformat.requests_done += 1;
            reformat.client_request();
        }

        fn client_request(reformat: *ReplicaReformat) void {
            assert(reformat.requests_done < constants.pipeline_prepare_queue_max);

            log.debug("{}: request start={}", .{
                reformat.options.format.replica,
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
                reformat.options.format.replica,
                reformat.requests_done,
            });

            reformat.requests_done += 1;
            if (reformat.requests_done == constants.pipeline_prepare_queue_max) {
                // +2 since we might have sent a DVC as part of +1 before we crashed.
                reformat.options.format.view = reformat.client.view + 2;
                //? matklad: Hm, this synchorneous call at the end of async callback chain still
                //? feels weird to me. Maybe punt it to the caller? That is, we still have async
                //? api with fn status() // fn done(), but we require the caller to call format
                //? themselves? That way, we won't have to thread options&allocator&error.
                //?
                //? Maybe we don't even need `ReplicaREformatType` struct, and can add a helper
                //? method directly to client? I don't like that that way the reformatting logic
                //? will be split between the client and main, but, also, it's not like there's a
                //? lot of logic there, so perhaps that's better? Your call!
                //?
                //? dj: I agree that it is weird, but I like having the recovery logic all together
                //? like this. Since the `recovery` process is quite simple I think the
                //? async-then-sync is tolerable.
                //? resolved.
                format(
                    Storage,
                    reformat.allocator,
                    reformat.options.format,
                    reformat.options.superblock,
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
