//! Pure Zig NATS Client for TigerBeetle CDC
//!
//! Implements NATS client protocol with JetStream support.
//! Uses TigerBeetle's IO interface for async networking.
//!
//! Features:
//! - Core NATS protocol (CONNECT, PUB, SUB, MSG, PING/PONG)
//! - JetStream publishing with acks
//! - KV store for progress tracking and distributed locking
//! - Request/reply pattern for JetStream API

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.nats);

const vsr = @import("../vsr.zig");
const IO = vsr.io.IO;

const protocol = @import("./nats/protocol.zig");
pub const Encoder = protocol.Encoder;
pub const Decoder = protocol.Decoder;
pub const Header = protocol.Header;
pub const Message = protocol.Message;
pub const ConnectOptions = protocol.ConnectOptions;
pub const ServerInfo = protocol.ServerInfo;

pub const tcp_port_default = protocol.tcp_port_default;

/// NATS Client using TigerBeetle's IO interface
pub const Client = struct {
    pub const Callback = *const fn (self: *Client) void;
    pub const MsgCallback = *const fn (self: *Client, msg: ?Message) void;

    allocator: std.mem.Allocator,
    io: *IO,
    fd: IO.socket_t = IO.INVALID_SOCKET,

    // Buffers
    recv_buffer: [65536]u8 = undefined,
    recv_len: usize = 0,
    send_buffer: [65536]u8 = undefined,
    send_encoder: Encoder = undefined,

    // Pending messages for batched publishing
    pending_messages: std.ArrayList(PendingMessage),

    // Completions
    recv_completion: IO.Completion = undefined,
    send_completion: IO.Completion = undefined,

    // State
    state: State = .disconnected,
    server_info: ServerInfo = .{},

    // Subscription tracking
    next_sid: u32 = 1,
    inbox_prefix: [32]u8 = undefined,
    inbox_prefix_len: usize = 0,

    // Callbacks
    connect_callback: ?Callback = null,
    publish_callback: ?Callback = null,
    request_callback: ?MsgCallback = null,
    pending_request_inbox: ?[]const u8 = null,

    // Config
    config: Config,

    pub const Config = struct {
        host: std.net.Address,
        user: ?[]const u8 = null,
        pass: ?[]const u8 = null,
        token: ?[]const u8 = null,
        name: []const u8 = "tigerbeetle-cdc",
        message_count_max: u32 = 1000,
        reply_timeout_ms: u64 = 5000,
    };

    const State = enum {
        disconnected,
        connecting,
        handshake,
        connected,
        error_state,
    };

    const PendingMessage = struct {
        subject: []const u8,
        headers: []const Header,
        payload: []const u8,
        reply_to: ?[]const u8,
    };

    pub fn init(allocator: std.mem.Allocator, config: Config, io: *IO) !Client {
        var client = Client{
            .allocator = allocator,
            .io = io,
            .config = config,
            .pending_messages = std.ArrayList(PendingMessage).init(allocator),
        };

        // Generate unique inbox prefix
        const now = std.time.nanoTimestamp();
        const inbox_str = std.fmt.bufPrint(
            &client.inbox_prefix,
            "_INBOX.{x}",
            .{@as(u64, @truncate(@as(u128, @bitCast(now))))},
        ) catch &[_]u8{};
        const len = inbox_str.len;
        client.inbox_prefix_len = len;

        client.send_encoder = Encoder.init(&client.send_buffer);

        return client;
    }

    pub fn deinit(self: *Client) void {
        if (self.fd != IO.INVALID_SOCKET) {
            std.posix.close(self.fd);
            self.fd = IO.INVALID_SOCKET;
        }
        self.pending_messages.deinit();
    }

    /// Connect to NATS server
    pub fn connect(self: *Client, callback: Callback) void {
        self.connect_callback = callback;
        self.state = .connecting;

        // Create socket and initiate connection
        const fd = std.posix.socket(
            self.config.host.any.family,
            std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK,
            std.posix.IPPROTO.TCP,
        ) catch |err| {
            log.err("socket() failed: {}", .{err});
            self.state = .error_state;
            if (self.connect_callback) |cb| cb(self);
            return;
        };

        self.fd = fd;

        // Start async connect
        self.io.connect(
            *Client,
            self,
            onConnect,
            &self.send_completion,
            self.fd,
            self.config.host,
        );
    }

    fn onConnect(self: *Client, completion: *IO.Completion, result: IO.ConnectError!void) void {
        _ = completion;
        _ = result catch |err| {
            log.err("connect() failed: {}", .{err});
            self.state = .error_state;
            if (self.connect_callback) |cb| cb(self);
            return;
        };

        self.state = .handshake;

        // Start receiving (expect INFO from server)
        self.startReceive();
    }

    fn startReceive(self: *Client) void {
        self.io.recv(
            *Client,
            self,
            onReceive,
            &self.recv_completion,
            self.fd,
            self.recv_buffer[self.recv_len..],
        );
    }

    fn onReceive(self: *Client, completion: *IO.Completion, result: IO.RecvError!usize) void {
        _ = completion;
        const bytes_read = result catch |err| {
            log.err("recv() failed: {}", .{err});
            self.state = .error_state;
            return;
        };

        if (bytes_read == 0) {
            log.info("connection closed by server", .{});
            self.state = .disconnected;
            return;
        }

        self.recv_len += bytes_read;
        self.processReceived();
    }

    fn processReceived(self: *Client) void {
        var decoder = Decoder.init(self.recv_buffer[0..self.recv_len]);

        while (true) {
            const remaining_before = decoder.remaining().len;
            if (remaining_before == 0) break;

            // Try to find a complete line
            const line_end = std.mem.indexOf(u8, decoder.remaining(), "\r\n") orelse {
                // Need more data
                break;
            };

            const line = decoder.remaining()[0..line_end];
            const cmd = Decoder.parseCommand(line);

            if (cmd) |command| {
                switch (command) {
                    .INFO => {
                        decoder.pos = 0;
                        self.server_info = decoder.parseInfo() catch {
                            log.err("failed to parse INFO", .{});
                            self.state = .error_state;
                            return;
                        };

                        log.info("connected to NATS server (jetstream={})", .{
                            self.server_info.jetstream,
                        });

                        // Send CONNECT
                        self.sendConnect();
                    },
                    .OK => {
                        decoder.advance(line_end + 2);
                        if (self.state == .handshake) {
                            self.state = .connected;
                            if (self.connect_callback) |cb| {
                                self.connect_callback = null;
                                cb(self);
                            }
                        }
                    },
                    .PING => {
                        decoder.advance(line_end + 2);
                        self.sendPong();
                    },
                    .PONG => {
                        decoder.advance(line_end + 2);
                    },
                    .MSG, .HMSG => {
                        decoder.pos = 0;
                        const msg = decoder.parseMsg() catch {
                            log.err("failed to parse MSG", .{});
                            break;
                        };

                        self.handleMessage(msg);
                    },
                    .ERR => {
                        decoder.pos = 0;
                        const err_msg = decoder.parseError() catch "unknown";
                        log.err("server error: {s}", .{err_msg});
                        decoder.advance(line_end + 2);
                    },
                    else => {
                        decoder.advance(line_end + 2);
                    },
                }
            } else {
                decoder.advance(line_end + 2);
            }

            if (decoder.remaining().len == remaining_before) {
                // No progress, need more data
                break;
            }
        }

        // Compact buffer
        const consumed = self.recv_len - decoder.remaining().len;
        if (consumed > 0) {
            std.mem.copyForwards(u8, &self.recv_buffer, self.recv_buffer[consumed..self.recv_len]);
            self.recv_len -= consumed;
        }

        // Continue receiving
        if (self.state != .error_state and self.state != .disconnected) {
            self.startReceive();
        }
    }

    fn handleMessage(self: *Client, msg: Message) void {
        // Check if this is a response to a pending request
        if (self.pending_request_inbox) |inbox| {
            if (std.mem.startsWith(u8, msg.subject, inbox)) {
                self.pending_request_inbox = null;
                if (self.request_callback) |cb| {
                    self.request_callback = null;
                    cb(self, msg);
                }
                return;
            }
        }

        // Otherwise, handle as regular message
        log.debug("received message on {s}: {d} bytes", .{ msg.subject, msg.payload.len });
    }

    fn sendConnect(self: *Client) void {
        self.send_encoder.reset();
        self.send_encoder.connect(.{
            .user = self.config.user,
            .pass = self.config.pass,
            .auth_token = self.config.token,
            .name = self.config.name,
            .headers = true,
            .no_responders = true,
        }) catch {
            log.err("failed to encode CONNECT", .{});
            self.state = .error_state;
            return;
        };

        self.doSend();
    }

    fn sendPong(self: *Client) void {
        self.send_encoder.reset();
        self.send_encoder.pong() catch return;
        self.doSend();
    }

    fn doSend(self: *Client) void {
        const data = self.send_encoder.written();
        if (data.len == 0) return;

        self.io.send(
            *Client,
            self,
            onSend,
            &self.send_completion,
            self.fd,
            data,
        );
    }

    fn onSend(self: *Client, completion: *IO.Completion, result: IO.SendError!usize) void {
        _ = completion;
        _ = result catch |err| {
            log.err("send() failed: {}", .{err});
            self.state = .error_state;
            return;
        };

        // Check if we have a publish callback waiting
        if (self.publish_callback) |cb| {
            self.publish_callback = null;
            cb(self);
        }
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /// Publish a message (fire and forget)
    pub fn publish(
        self: *Client,
        subject: []const u8,
        headers: []const Header,
        payload: []const u8,
    ) void {
        self.send_encoder.reset();

        if (headers.len > 0) {
            self.send_encoder.pub_with_headers(subject, null, headers, payload) catch {
                log.err("failed to encode HPUB", .{});
                return;
            };
        } else {
            self.send_encoder.pub_simple(subject, null, payload) catch {
                log.err("failed to encode PUB", .{});
                return;
            };
        }

        self.doSend();
    }

    /// Enqueue a message for batched publishing
    pub fn publishEnqueue(
        self: *Client,
        subject: []const u8,
        headers: []const Header,
        payload: []const u8,
    ) void {
        self.pending_messages.append(.{
            .subject = subject,
            .headers = headers,
            .payload = payload,
            .reply_to = null,
        }) catch {
            log.err("failed to enqueue message", .{});
        };
    }

    /// Send all enqueued messages
    pub fn publishSend(self: *Client, callback: Callback) void {
        self.publish_callback = callback;
        self.send_encoder.reset();

        for (self.pending_messages.items) |msg| {
            if (msg.headers.len > 0) {
                self.send_encoder.pub_with_headers(
                    msg.subject,
                    msg.reply_to,
                    msg.headers,
                    msg.payload,
                ) catch {
                    log.err("failed to encode HPUB in batch", .{});
                    continue;
                };
            } else {
                self.send_encoder.pub_simple(msg.subject, msg.reply_to, msg.payload) catch {
                    log.err("failed to encode PUB in batch", .{});
                    continue;
                };
            }
        }

        self.pending_messages.clearRetainingCapacity();
        self.doSend();
    }

    /// Request/reply pattern
    pub fn request(
        self: *Client,
        subject: []const u8,
        payload: []const u8,
        callback: MsgCallback,
    ) void {
        // Generate unique inbox for reply
        var inbox_buf: [64]u8 = undefined;
        const inbox = std.fmt.bufPrint(&inbox_buf, "{s}.{d}", .{
            self.inbox_prefix[0..self.inbox_prefix_len],
            self.next_sid,
        }) catch {
            callback(self, null);
            return;
        };
        self.next_sid += 1;

        // Subscribe to inbox
        var sid_buf: [16]u8 = undefined;
        const sid = std.fmt.bufPrint(&sid_buf, "{d}", .{self.next_sid}) catch {
            callback(self, null);
            return;
        };

        self.request_callback = callback;
        self.pending_request_inbox = inbox;

        self.send_encoder.reset();

        // SUB inbox sid
        self.send_encoder.sub(inbox, null, sid) catch {
            callback(self, null);
            return;
        };

        // UNSUB sid 1 (auto-unsubscribe after 1 message)
        self.send_encoder.unsub(sid, 1) catch {
            callback(self, null);
            return;
        };

        // PUB subject reply payload
        self.send_encoder.pub_simple(subject, inbox, payload) catch {
            callback(self, null);
            return;
        };

        self.doSend();
    }

    /// Check if connected
    pub fn isConnected(self: *const Client) bool {
        return self.state == .connected;
    }

    /// Check if JetStream is available
    pub fn hasJetStream(self: *const Client) bool {
        return self.server_info.jetstream;
    }
};
