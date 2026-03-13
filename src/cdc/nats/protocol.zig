//! NATS Protocol Implementation (Pure Zig)
//!
//! Implements NATS client protocol as specified in:
//! https://docs.nats.io/reference/reference-protocols/nats-protocol
//!
//! Text-based protocol over TCP:
//! - INFO, CONNECT, PUB, SUB, UNSUB, MSG, PING, PONG, +OK, -ERR
//!
//! JetStream API uses request/reply pattern on $JS.API.* subjects.

const std = @import("std");
const assert = std.debug.assert;

pub const tcp_port_default = 4222;

/// Maximum line length for NATS protocol messages
pub const max_line_length = 4096;

/// NATS protocol commands
pub const Command = enum {
    INFO,
    CONNECT,
    PUB,
    HPUB, // Publish with headers (NATS 2.2+)
    SUB,
    UNSUB,
    MSG,
    HMSG, // Message with headers
    PING,
    PONG,
    OK,
    ERR,
};

/// Server INFO message fields
pub const ServerInfo = struct {
    server_id: []const u8 = "",
    server_name: []const u8 = "",
    version: []const u8 = "",
    proto: u32 = 0,
    host: []const u8 = "",
    port: u16 = 4222,
    headers: bool = false,
    max_payload: u32 = 1048576,
    jetstream: bool = false,
    client_id: ?u64 = null,
    client_ip: []const u8 = "",
    nonce: []const u8 = "",
    cluster: []const u8 = "",
    connect_urls: []const []const u8 = &.{},
};

/// Client CONNECT message options
pub const ConnectOptions = struct {
    verbose: bool = false,
    pedantic: bool = false,
    tls_required: bool = false,
    auth_token: ?[]const u8 = null,
    user: ?[]const u8 = null,
    pass: ?[]const u8 = null,
    name: []const u8 = "tigerbeetle-cdc",
    lang: []const u8 = "zig",
    version: []const u8 = "0.1.0",
    protocol: u32 = 1,
    echo: bool = false,
    headers: bool = true,
    no_responders: bool = true,
};

/// Protocol encoder
pub const Encoder = struct {
    buffer: []u8,
    pos: usize = 0,

    pub fn init(buffer: []u8) Encoder {
        return .{ .buffer = buffer };
    }

    pub fn reset(self: *Encoder) void {
        self.pos = 0;
    }

    pub fn written(self: *const Encoder) []const u8 {
        return self.buffer[0..self.pos];
    }

    pub fn remaining(self: *const Encoder) usize {
        return self.buffer.len - self.pos;
    }

    fn write(self: *Encoder, data: []const u8) !void {
        if (self.pos + data.len > self.buffer.len) {
            return error.BufferOverflow;
        }
        @memcpy(self.buffer[self.pos..][0..data.len], data);
        self.pos += data.len;
    }

    fn writeInt(self: *Encoder, value: anytype) !void {
        var buf: [32]u8 = undefined;
        const str = std.fmt.bufPrint(&buf, "{d}", .{value}) catch return error.BufferOverflow;
        try self.write(str);
    }

    /// Encode CONNECT command
    pub fn connect(self: *Encoder, options: ConnectOptions) !void {
        try self.write("CONNECT {");

        // Build JSON payload
        try self.write("\"verbose\":");
        try self.write(if (options.verbose) "true" else "false");

        try self.write(",\"pedantic\":");
        try self.write(if (options.pedantic) "true" else "false");

        try self.write(",\"tls_required\":");
        try self.write(if (options.tls_required) "true" else "false");

        if (options.auth_token) |token| {
            try self.write(",\"auth_token\":\"");
            try self.write(token);
            try self.write("\"");
        }

        if (options.user) |user| {
            try self.write(",\"user\":\"");
            try self.write(user);
            try self.write("\"");
        }

        if (options.pass) |pass| {
            try self.write(",\"pass\":\"");
            try self.write(pass);
            try self.write("\"");
        }

        try self.write(",\"name\":\"");
        try self.write(options.name);
        try self.write("\"");

        try self.write(",\"lang\":\"");
        try self.write(options.lang);
        try self.write("\"");

        try self.write(",\"version\":\"");
        try self.write(options.version);
        try self.write("\"");

        try self.write(",\"protocol\":");
        try self.writeInt(options.protocol);

        try self.write(",\"echo\":");
        try self.write(if (options.echo) "true" else "false");

        try self.write(",\"headers\":");
        try self.write(if (options.headers) "true" else "false");

        try self.write(",\"no_responders\":");
        try self.write(if (options.no_responders) "true" else "false");

        try self.write("}\r\n");
    }

    /// Encode PUB command (without headers)
    pub fn pub_simple(
        self: *Encoder,
        subject: []const u8,
        reply_to: ?[]const u8,
        payload: []const u8,
    ) !void {
        try self.write("PUB ");
        try self.write(subject);
        if (reply_to) |reply| {
            try self.write(" ");
            try self.write(reply);
        }
        try self.write(" ");
        try self.writeInt(payload.len);
        try self.write("\r\n");
        try self.write(payload);
        try self.write("\r\n");
    }

    /// Encode HPUB command (with headers)
    pub fn pub_with_headers(
        self: *Encoder,
        subject: []const u8,
        reply_to: ?[]const u8,
        headers: []const Header,
        payload: []const u8,
    ) !void {
        // Build headers block
        var headers_buf: [4096]u8 = undefined;
        var headers_len: usize = 0;

        // NATS headers version line
        const version_line = "NATS/1.0\r\n";
        @memcpy(headers_buf[headers_len..][0..version_line.len], version_line);
        headers_len += version_line.len;

        for (headers) |header| {
            @memcpy(headers_buf[headers_len..][0..header.name.len], header.name);
            headers_len += header.name.len;
            headers_buf[headers_len] = ':';
            headers_len += 1;
            headers_buf[headers_len] = ' ';
            headers_len += 1;
            @memcpy(headers_buf[headers_len..][0..header.value.len], header.value);
            headers_len += header.value.len;
            headers_buf[headers_len] = '\r';
            headers_len += 1;
            headers_buf[headers_len] = '\n';
            headers_len += 1;
        }

        // Empty line to end headers
        headers_buf[headers_len] = '\r';
        headers_len += 1;
        headers_buf[headers_len] = '\n';
        headers_len += 1;

        const total_size = headers_len + payload.len;

        try self.write("HPUB ");
        try self.write(subject);
        if (reply_to) |reply| {
            try self.write(" ");
            try self.write(reply);
        }
        try self.write(" ");
        try self.writeInt(headers_len);
        try self.write(" ");
        try self.writeInt(total_size);
        try self.write("\r\n");
        try self.write(headers_buf[0..headers_len]);
        try self.write(payload);
        try self.write("\r\n");
    }

    /// Encode SUB command
    pub fn sub(self: *Encoder, subject: []const u8, queue_group: ?[]const u8, sid: []const u8) !void {
        try self.write("SUB ");
        try self.write(subject);
        if (queue_group) |qg| {
            try self.write(" ");
            try self.write(qg);
        }
        try self.write(" ");
        try self.write(sid);
        try self.write("\r\n");
    }

    /// Encode UNSUB command
    pub fn unsub(self: *Encoder, sid: []const u8, max_msgs: ?u32) !void {
        try self.write("UNSUB ");
        try self.write(sid);
        if (max_msgs) |max| {
            try self.write(" ");
            try self.writeInt(max);
        }
        try self.write("\r\n");
    }

    /// Encode PING command
    pub fn ping(self: *Encoder) !void {
        try self.write("PING\r\n");
    }

    /// Encode PONG command
    pub fn pong(self: *Encoder) !void {
        try self.write("PONG\r\n");
    }
};

/// Header key-value pair
pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// Protocol decoder
pub const Decoder = struct {
    buffer: []const u8,
    pos: usize = 0,

    pub const Error = error{
        InvalidProtocol,
        UnexpectedEof,
        BufferOverflow,
        InvalidJson,
    };

    pub fn init(buffer: []const u8) Decoder {
        return .{ .buffer = buffer };
    }

    pub fn remaining(self: *const Decoder) []const u8 {
        return self.buffer[self.pos..];
    }

    pub fn advance(self: *Decoder, n: usize) void {
        self.pos = @min(self.pos + n, self.buffer.len);
    }

    /// Read until CRLF, returns line without CRLF
    fn readLine(self: *Decoder) Error![]const u8 {
        const start = self.pos;
        while (self.pos < self.buffer.len) {
            if (self.pos + 1 < self.buffer.len and
                self.buffer[self.pos] == '\r' and
                self.buffer[self.pos + 1] == '\n')
            {
                const line = self.buffer[start..self.pos];
                self.pos += 2;
                return line;
            }
            self.pos += 1;
        }
        return error.UnexpectedEof;
    }

    /// Read exactly n bytes
    fn readBytes(self: *Decoder, n: usize) Error![]const u8 {
        if (self.pos + n > self.buffer.len) {
            return error.UnexpectedEof;
        }
        const data = self.buffer[self.pos..][0..n];
        self.pos += n;
        return data;
    }

    /// Parse command type from line
    pub fn parseCommand(line: []const u8) ?Command {
        if (std.mem.startsWith(u8, line, "INFO ")) return .INFO;
        if (std.mem.startsWith(u8, line, "MSG ")) return .MSG;
        if (std.mem.startsWith(u8, line, "HMSG ")) return .HMSG;
        if (std.mem.startsWith(u8, line, "PING")) return .PING;
        if (std.mem.startsWith(u8, line, "PONG")) return .PONG;
        if (std.mem.startsWith(u8, line, "+OK")) return .OK;
        if (std.mem.startsWith(u8, line, "-ERR")) return .ERR;
        return null;
    }

    /// Parse INFO message
    pub fn parseInfo(self: *Decoder) Error!ServerInfo {
        const line = try self.readLine();
        if (!std.mem.startsWith(u8, line, "INFO ")) {
            return error.InvalidProtocol;
        }
        const json = line[5..];
        return parseServerInfoJson(json);
    }

    /// Parse MSG or HMSG
    pub fn parseMsg(self: *Decoder) Error!Message {
        const line = try self.readLine();

        var msg: Message = .{};
        var parts = std.mem.splitScalar(u8, line, ' ');

        const cmd = parts.next() orelse return error.InvalidProtocol;
        const has_headers = std.mem.eql(u8, cmd, "HMSG");

        msg.subject = parts.next() orelse return error.InvalidProtocol;
        msg.sid = parts.next() orelse return error.InvalidProtocol;

        // Check if next is reply-to or size
        const next = parts.next() orelse return error.InvalidProtocol;
        const maybe_next = parts.next();

        if (has_headers) {
            // HMSG subject sid [reply] hdr_len total_len
            if (maybe_next) |after_next| {
                if (parts.next()) |last| {
                    // Has reply-to
                    msg.reply_to = next;
                    msg.headers_len = std.fmt.parseInt(usize, after_next, 10) catch
                        return error.InvalidProtocol;
                    msg.total_len = std.fmt.parseInt(usize, last, 10) catch
                        return error.InvalidProtocol;
                } else {
                    // No reply-to
                    msg.headers_len = std.fmt.parseInt(usize, next, 10) catch
                        return error.InvalidProtocol;
                    msg.total_len = std.fmt.parseInt(usize, after_next, 10) catch
                        return error.InvalidProtocol;
                }
            } else {
                return error.InvalidProtocol;
            }
        } else {
            // MSG subject sid [reply] len
            if (maybe_next) |size_str| {
                msg.reply_to = next;
                msg.total_len = std.fmt.parseInt(usize, size_str, 10) catch
                    return error.InvalidProtocol;
            } else {
                msg.total_len = std.fmt.parseInt(usize, next, 10) catch
                    return error.InvalidProtocol;
            }
        }

        // Read payload
        const payload = try self.readBytes(msg.total_len);

        if (has_headers and msg.headers_len > 0) {
            msg.headers_data = payload[0..msg.headers_len];
            msg.payload = payload[msg.headers_len..];
        } else {
            msg.payload = payload;
        }

        // Consume trailing CRLF
        _ = try self.readBytes(2);

        return msg;
    }

    /// Check for +OK response
    pub fn expectOk(self: *Decoder) Error!void {
        const line = try self.readLine();
        if (!std.mem.startsWith(u8, line, "+OK")) {
            return error.InvalidProtocol;
        }
    }

    /// Parse -ERR message
    pub fn parseError(self: *Decoder) Error![]const u8 {
        const line = try self.readLine();
        if (!std.mem.startsWith(u8, line, "-ERR")) {
            return error.InvalidProtocol;
        }
        // Return error message (strip -ERR and quotes)
        var err_msg = line[4..];
        if (err_msg.len > 0 and err_msg[0] == ' ') err_msg = err_msg[1..];
        if (err_msg.len >= 2 and err_msg[0] == '\'' and err_msg[err_msg.len - 1] == '\'') {
            err_msg = err_msg[1 .. err_msg.len - 1];
        }
        return err_msg;
    }

    fn parseServerInfoJson(json: []const u8) ServerInfo {
        // Simple JSON parser for ServerInfo
        var info = ServerInfo{};

        // Extract jetstream boolean
        if (std.mem.indexOf(u8, json, "\"jetstream\":true")) |_| {
            info.jetstream = true;
        }

        // Extract headers boolean
        if (std.mem.indexOf(u8, json, "\"headers\":true")) |_| {
            info.headers = true;
        }

        // Extract max_payload
        if (std.mem.indexOf(u8, json, "\"max_payload\":")) |start| {
            const num_start = start + 14;
            var num_end = num_start;
            while (num_end < json.len and json[num_end] >= '0' and json[num_end] <= '9') {
                num_end += 1;
            }
            if (num_end > num_start) {
                info.max_payload = std.fmt.parseInt(u32, json[num_start..num_end], 10) catch
                    1048576;
            }
        }

        // Extract proto
        if (std.mem.indexOf(u8, json, "\"proto\":")) |start| {
            const num_start = start + 8;
            var num_end = num_start;
            while (num_end < json.len and json[num_end] >= '0' and json[num_end] <= '9') {
                num_end += 1;
            }
            if (num_end > num_start) {
                info.proto = std.fmt.parseInt(u32, json[num_start..num_end], 10) catch 0;
            }
        }

        return info;
    }
};

/// Parsed message
pub const Message = struct {
    subject: []const u8 = "",
    sid: []const u8 = "",
    reply_to: ?[]const u8 = null,
    headers_len: usize = 0,
    total_len: usize = 0,
    headers_data: []const u8 = "",
    payload: []const u8 = "",

    /// Parse headers into iterator
    pub fn headersIterator(self: *const Message) HeaderIterator {
        return HeaderIterator.init(self.headers_data);
    }
};

/// Iterator over message headers
pub const HeaderIterator = struct {
    data: []const u8,
    pos: usize = 0,
    started: bool = false,

    pub fn init(data: []const u8) HeaderIterator {
        return .{ .data = data };
    }

    pub fn next(self: *HeaderIterator) ?Header {
        // Skip version line on first call
        if (!self.started) {
            self.started = true;
            // Skip "NATS/1.0\r\n"
            if (std.mem.indexOf(u8, self.data, "\r\n")) |idx| {
                self.pos = idx + 2;
            }
        }

        while (self.pos < self.data.len) {
            const start = self.pos;

            // Find end of line
            const line_end = if (std.mem.indexOfPos(u8, self.data, self.pos, "\r\n")) |idx|
                idx
            else
                self.data.len;

            const line = self.data[start..line_end];
            self.pos = @min(line_end + 2, self.data.len);

            // Empty line marks end of headers
            if (line.len == 0) {
                return null;
            }

            // Parse "Name: Value"
            if (std.mem.indexOf(u8, line, ": ")) |sep| {
                return Header{
                    .name = line[0..sep],
                    .value = line[sep + 2 ..],
                };
            } else if (std.mem.indexOf(u8, line, ":")) |sep| {
                return Header{
                    .name = line[0..sep],
                    .value = if (sep + 1 < line.len) line[sep + 1 ..] else "",
                };
            }
        }

        return null;
    }
};

// ============================================================================
// JetStream Protocol Helpers
// ============================================================================

/// JetStream API subjects
pub const js = struct {
    pub const api_prefix = "$JS.API";

    pub fn streamCreate(stream: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}.STREAM.CREATE.{s}", .{ api_prefix, stream }) catch "";
    }

    pub fn streamInfo(stream: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}.STREAM.INFO.{s}", .{ api_prefix, stream }) catch "";
    }

    pub fn publish(stream: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}.STREAM.{s}", .{ api_prefix, stream }) catch "";
    }

    pub fn consumerCreate(stream: []const u8, consumer: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(
            buf,
            "{s}.CONSUMER.CREATE.{s}.{s}",
            .{ api_prefix, stream, consumer },
        ) catch "";
    }

    pub fn kvGet(bucket: []const u8, key: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "$KV.{s}.{s}", .{ bucket, key }) catch "";
    }

    pub fn kvPut(bucket: []const u8, key: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "$KV.{s}.{s}", .{ bucket, key }) catch "";
    }
};

// ============================================================================
// Tests
// ============================================================================

test "encoder: CONNECT" {
    var buf: [1024]u8 = undefined;
    var enc = Encoder.init(&buf);

    try enc.connect(.{});

    const result = enc.written();
    try std.testing.expect(std.mem.startsWith(u8, result, "CONNECT {"));
    try std.testing.expect(std.mem.endsWith(u8, result, "}\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "\"headers\":true") != null);
}

test "encoder: PUB simple" {
    var buf: [1024]u8 = undefined;
    var enc = Encoder.init(&buf);

    try enc.pub_simple("test.subject", null, "hello");

    const result = enc.written();
    try std.testing.expectEqualStrings("PUB test.subject 5\r\nhello\r\n", result);
}

test "encoder: PUB with reply" {
    var buf: [1024]u8 = undefined;
    var enc = Encoder.init(&buf);

    try enc.pub_simple("test.subject", "_INBOX.123", "hello");

    const result = enc.written();
    try std.testing.expectEqualStrings("PUB test.subject _INBOX.123 5\r\nhello\r\n", result);
}

test "decoder: parse MSG" {
    const data = "MSG test.subject 1 5\r\nhello\r\n";
    var dec = Decoder.init(data);

    const msg = try dec.parseMsg();
    try std.testing.expectEqualStrings("test.subject", msg.subject);
    try std.testing.expectEqualStrings("1", msg.sid);
    try std.testing.expect(msg.reply_to == null);
    try std.testing.expectEqualStrings("hello", msg.payload);
}

test "decoder: parse MSG with reply" {
    const data = "MSG test.subject 1 _INBOX.123 5\r\nhello\r\n";
    var dec = Decoder.init(data);

    const msg = try dec.parseMsg();
    try std.testing.expectEqualStrings("test.subject", msg.subject);
    try std.testing.expectEqualStrings("1", msg.sid);
    try std.testing.expectEqualStrings("_INBOX.123", msg.reply_to.?);
    try std.testing.expectEqualStrings("hello", msg.payload);
}
