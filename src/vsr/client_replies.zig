const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log.scoped(.client_replies);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const IOPS = @import("../iops.zig").IOPS;
const vsr = @import("../vsr.zig");
const Message = @import("../message_pool.zig").MessagePool.Message;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Slot = @import("superblock_client_sessions.zig").ReplySlot;
const Entry = @import("superblock_client_sessions.zig").ClientSessions.Entry;

const client_replies_iops_max =
    constants.client_replies_iops_read_max + constants.client_replies_iops_write_max;

fn slot_offset(slot: Slot) usize {
    return slot.index * constants.message_size_max;
}

pub fn ClientRepliesType(comptime Storage: type) type {
    return struct {
        const ClientReplies = @This();

        const Read = struct {
            client_replies: *ClientReplies,
            completion: Storage.Read,
            callback: fn (client_replies: *ClientReplies, reply: ?*Message) void,
            message: *Message,
            /// The header of the expected reply.
            header: vsr.Header,
        };

        const Write = struct {
            client_replies: *ClientReplies,
            completion: Storage.Write,
            callback: fn (client_replies: *ClientReplies, wrote: *Message) void,
            slot: Slot,
            message: *Message,
        };

        storage: *Storage,
        message_pool: *MessagePool,
        replica: u8,

        reads: IOPS(Read, constants.client_replies_iops_read_max) = .{},
        writes: IOPS(Write, constants.client_replies_iops_write_max) = .{},

        /// Track which slots have a write currently in progress.
        writing: std.StaticBitSet(constants.clients_max) =
            std.StaticBitSet(constants.clients_max).initEmpty(),

        /// Guard against multiple concurrent writes to the same slot.
        /// Pointers are into `writes`.
        write_queue: RingBuffer(*Write, constants.client_replies_iops_write_max, .array) = .{},

        pub fn init(storage: *Storage, message_pool: *MessagePool, replica_index: u8) ClientReplies {
            return .{
                .storage = storage,
                .message_pool = message_pool,
                .replica = replica_index,
            };
        }

        pub fn deinit(client_replies: *ClientReplies) void {
            {
                var it = client_replies.reads.iterate();
                while (it.next()) |read| client_replies.message_pool.unref(read.message);
            }
            {
                var it = client_replies.writes.iterate();
                while (it.next()) |write| client_replies.message_pool.unref(write.message);
            }
            // Don't unref `write_queue`'s Writes — they are a subset of `writes`.
        }

        pub fn read_reply(
            client_replies: *ClientReplies,
            slot: Slot,
            entry: *const Entry,
            callback: fn (*ClientReplies, ?*Message) void,
        ) void {
            const client = entry.header.client;

            if (client_replies.writing.isSet(slot.index)) {
                var writes = client_replies.writes.iterate();
                var write_latest: ?*const Write = null;
                while (writes.next()) |write| {
                    if (write.message.header.client == client) {
                        if (write_latest == null or
                            write_latest.?.message.header.request < write.message.header.request)
                        {
                            write_latest = write;
                        }
                    }
                }
                assert(write_latest.?.message.header.checksum == entry.header.checksum);

                callback(client_replies, write_latest.?.message);
                return;
            }

            const read = client_replies.reads.acquire() orelse {
                log.debug("{}: read_reply: busy (client={} reply={})", .{
                    client_replies.replica,
                    entry.header.client,
                    entry.header.checksum,
                });

                callback(client_replies, null);
                return;
            };

            const message = client_replies.message_pool.get_message();
            defer client_replies.message_pool.unref(message);

            read.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .message = message.ref(),
                .callback = callback,
                .header = entry.header,
            };

            const buffer: []u8 = message.buffer[0..vsr.sector_ceil(entry.header.size)];

            log.debug("{}: read_reply: start (client={} reply={})", .{
                client_replies.replica,
                read.header.client,
                read.header.checksum,
            });

            client_replies.storage.read_sectors(
                read_reply_callback,
                &read.completion,
                buffer,
                .client_replies,
                slot_offset(slot),
            );
        }

        fn read_reply_callback(completion: *Storage.Read) void {
            const read = @fieldParentPtr(ClientReplies.Read, "completion", completion);
            const client_replies = read.client_replies;

            defer {
                client_replies.message_pool.unref(read.message);
                client_replies.reads.release(read);
            }

            if (!read.message.header.valid_checksum()) {
                log.warn("{}: read_reply: corrupt header (client={} reply={})", .{
                    client_replies.replica,
                    read.header.client,
                    read.header.checksum,
                });

                read.callback(client_replies, null);
                return;
            }

            if (read.message.header.checksum != read.header.checksum) {
                log.warn("{}: read_reply: unexpected header (client={} reply={} found={})", .{
                    client_replies.replica,
                    read.header.client,
                    read.header.checksum,
                    read.message.header.checksum,
                });

                read.callback(client_replies, null);
                return;
            }

            assert(read.message.header.command == .reply);
            assert(read.message.header.cluster == read.header.cluster);

            if (!read.message.header.valid_checksum_body(read.message.body())) {
                log.warn("{}: read_reply: corrupt body (client={} reply={})", .{
                    client_replies.replica,
                    read.header.client,
                    read.header.checksum,
                });

                read.callback(client_replies, null);
                return;
            }

            log.debug("{}: read_reply: done (client={} reply={})", .{
                client_replies.replica,
                read.header.client,
                read.header.checksum,
            });

            read.callback(client_replies, read.message);
        }

        pub fn can_write_reply(client_replies: *ClientReplies) bool {
            return client_replies.writes.available() > 0;
        }

        pub fn write_reply(
            client_replies: *ClientReplies,
            slot: Slot,
            message: *Message,
            callback: fn (client_replies: *ClientReplies, wrote: *Message) void,
        ) void {
            assert(client_replies.can_write_reply());
            assert(message.header.command == .reply);
            // There is never any need to write a body-less message, since the header is
            // stored safely in the client sessions superblock trailer.
            assert(message.header.size != @sizeOf(vsr.Header));

            const write = client_replies.writes.acquire().?;
            write.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .callback = callback,
                .message = message.ref(),
                .slot = slot,
            };

            // TODO Optimization — if there is already a write for this slot queued (but not started), replace it.
            client_replies.write_queue.push_assume_capacity(write);
            client_replies.write_reply_next();
        }

        fn write_reply_next(client_replies: *ClientReplies) void {
            while (client_replies.write_queue.head()) |write| {
                if (client_replies.writing.isSet(write.slot.index)) break;

                const message = write.message;
                _ = client_replies.write_queue.pop();
                client_replies.writing.set(write.slot.index);
                client_replies.storage.write_sectors(
                    write_reply_callback,
                    &write.completion,
                    message.buffer[0..vsr.sector_ceil(message.header.size)],
                    .client_replies,
                    slot_offset(write.slot),
                );
            }
        }

        fn write_reply_callback(completion: *Storage.Write) void {
            const write = @fieldParentPtr(ClientReplies.Write, "completion", completion);
            const client_replies = write.client_replies;
            const callback = write.callback;
            const message = write.message;

            // Release the write *before* invoking the callback, so that if the callback
            // uses can_write_reply() it doesn't appear busy when its not.
            client_replies.writing.unset(write.slot.index);
            client_replies.writes.release(write);
            callback(client_replies, message);

            client_replies.message_pool.unref(message);
            client_replies.write_reply_next();
        }
    };
}
