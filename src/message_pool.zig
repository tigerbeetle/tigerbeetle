const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const vsr = @import("vsr.zig");
const Header = vsr.Header;

comptime {
    // message_size_max must be a multiple of sector_size for Direct I/O
    assert(config.message_size_max % config.sector_size == 0);
}

/// Add an extra sector_size bytes to allow a partially received subsequent
/// message to be shifted to make space for 0 padding to vsr.sector_ceil.
const message_size_max_padded = config.message_size_max + config.sector_size;

/// A pool of reference-counted Messages, memory for which is allocated only once
/// during initialization and reused thereafter. The config.message_bus_messages_max
/// and config.message_bus_headers_max values determine the size of this pool.
pub const MessagePool = struct {
    pub const Message = struct {
        // TODO: replace this with a header() function to save memory
        header: *Header,
        /// Unless this Message is header only, this buffer is in aligned to config.sector_size
        /// and casting to that alignment in order to perform Direct I/O is safe.
        buffer: []u8,
        references: u32 = 0,
        next: ?*Message,

        /// Increment the reference count of the message and return the same pointer passed.
        pub fn ref(message: *Message) *Message {
            message.references += 1;
            return message;
        }

        pub fn body(message: *Message) []u8 {
            return message.buffer[@sizeOf(Header)..message.header.size];
        }

        fn header_only(message: Message) bool {
            const ret = message.buffer.len == @sizeOf(Header);
            assert(ret or message.buffer.len == message_size_max_padded);
            return ret;
        }
    };

    /// List of currently unused messages of message_size_max_padded
    free_list: ?*Message,
    /// List of currently usused header-sized messages
    header_only_free_list: ?*Message,

    pub fn init(allocator: *mem.Allocator) error{OutOfMemory}!MessagePool {
        var ret: MessagePool = .{
            .free_list = null,
            .header_only_free_list = null,
        };
        {
            var i: usize = 0;
            while (i < config.message_bus_messages_max) : (i += 1) {
                const buffer = try allocator.allocAdvanced(
                    u8,
                    config.sector_size,
                    message_size_max_padded,
                    .exact,
                );
                const message = try allocator.create(Message);
                message.* = .{
                    .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
                    .buffer = buffer,
                    .next = ret.free_list,
                };
                ret.free_list = message;
            }
        }
        {
            var i: usize = 0;
            while (i < config.message_bus_headers_max) : (i += 1) {
                const header = try allocator.create(Header);
                const message = try allocator.create(Message);
                message.* = .{
                    .header = header,
                    .buffer = mem.asBytes(header),
                    .next = ret.header_only_free_list,
                };
                ret.header_only_free_list = message;
            }
        }

        return ret;
    }

    /// Get an unused message with a buffer of config.message_size_max. If no such message is
    /// available, an error is returned. The returned message has exactly one reference.
    pub fn get_message(pool: *MessagePool) ?*Message {
        const ret = pool.free_list orelse return null;
        pool.free_list = ret.next;
        ret.next = null;
        assert(!ret.header_only());
        assert(ret.references == 0);
        ret.references = 1;
        return ret;
    }

    /// Get an unused message with a buffer only large enough to hold a header. If no such message
    /// is available, an error is returned. The returned message has exactly one reference.
    pub fn get_header_only_message(pool: *MessagePool) ?*Message {
        const ret = pool.header_only_free_list orelse return null;
        pool.header_only_free_list = ret.next;
        ret.next = null;
        assert(ret.header_only());
        assert(ret.references == 0);
        ret.references = 1;
        return ret;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(pool: *MessagePool, message: *Message) void {
        message.references -= 1;
        if (message.references == 0) {
            if (std.builtin.mode == .Debug) mem.set(u8, message.buffer, undefined);
            if (message.header_only()) {
                message.next = pool.header_only_free_list;
                pool.header_only_free_list = message;
            } else {
                message.next = pool.free_list;
                pool.free_list = message;
            }
        }
    }
};
