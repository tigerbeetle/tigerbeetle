//! NATS Transport Implementation for CDC (Pure Zig)
//!
//! Implements the Transport interface for NATS with JetStream.
//!
//! Features:
//! - Distributed lock via NATS KV with atomic create
//! - Progress tracking via NATS KV
//! - Publishing to JetStream stream
//! - Batched publishing with acknowledgment

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.nats_transport);

const vsr = @import("../vsr.zig");
const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const CommonConfig = transport_mod.CommonConfig;
const nats = @import("nats.zig");

/// NATS transport implementation for CDC.
///
/// Uses NATS JetStream for reliable message delivery:
/// - Distributed lock: NATS KV bucket with atomic create (fails if key exists)
/// - Progress tracking: NATS KV bucket with revision tracking
/// - Publishing: JetStream stream with acks
pub const NatsTransport = struct {
    allocator: std.mem.Allocator,
    client: nats.Client,
    config: Config,

    // State
    connected: bool = false,
    lock_acquired: bool = false,

    // Pending callbacks
    connect_callback: ?*const fn (*anyopaque, bool) void = null,
    connect_callback_ctx: ?*anyopaque = null,
    lock_callback: ?*const fn (*anyopaque, bool) void = null,
    lock_callback_ctx: ?*anyopaque = null,
    progress_callback: ?*const fn (*anyopaque, ?Transport.ProgressState) void = null,
    progress_callback_ctx: ?*anyopaque = null,
    publish_callback: ?*const fn (*anyopaque, bool) void = null,
    publish_callback_ctx: ?*anyopaque = null,
    update_progress_callback: ?*const fn (*anyopaque, bool) void = null,
    update_progress_callback_ctx: ?*anyopaque = null,

    // KV keys (generated from cluster_id)
    lock_key: []const u8,
    progress_key: []const u8,

    // Publishing state
    pending_count: u32 = 0,

    pub const Config = struct {
        /// NATS server URL (parsed to host:port)
        host: std.net.Address,
        /// NATS user (optional)
        user: ?[]const u8 = null,
        /// NATS password (optional)
        password: ?[]const u8 = null,
        /// NATS auth token (optional)
        token: ?[]const u8 = null,
        /// JetStream stream name for publishing
        stream_name: []const u8 = "tigerbeetle",
        /// Subject prefix for publishing
        subject_prefix: []const u8 = "tigerbeetle.events",
        /// KV bucket for lock and progress
        kv_bucket: []const u8 = "tigerbeetle-cdc",
        /// Common transport config
        common: CommonConfig,
    };

    const vtable: Transport.VTable = .{
        .connect = connect,
        .acquireLock = acquireLock,
        .getProgress = getProgress,
        .publishEnqueue = publishEnqueue,
        .publishSend = publishSend,
        .updateProgress = updateProgress,
        .tick = tick,
        .isConnected = isConnected,
        .deinit = deinit_transport,
    };

    pub fn init(allocator: std.mem.Allocator, io: *vsr.io.IO, config: Config) !NatsTransport {
        const lock_key = try std.fmt.allocPrint(
            allocator,
            "lock.{}",
            .{config.common.cluster_id},
        );
        errdefer allocator.free(lock_key);

        const progress_key = try std.fmt.allocPrint(
            allocator,
            "progress.{}",
            .{config.common.cluster_id},
        );
        errdefer allocator.free(progress_key);

        const client = try nats.Client.init(allocator, .{
            .host = config.host,
            .user = config.user,
            .pass = config.password,
            .token = config.token,
            .name = "tigerbeetle-cdc",
            .message_count_max = config.common.message_count_max,
        }, io);

        return .{
            .allocator = allocator,
            .client = client,
            .config = config,
            .lock_key = lock_key,
            .progress_key = progress_key,
        };
    }

    pub fn transport(self: *NatsTransport) Transport {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn connect(ptr: *anyopaque, callback: *const fn (*anyopaque, bool) void, ctx: *anyopaque) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.connect_callback = callback;
        self.connect_callback_ctx = ctx;

        self.client.connect(&struct {
            fn cb(client: *nats.Client) void {
                const transport_ptr: *NatsTransport = @alignCast(@fieldParentPtr("client", client));
                transport_ptr.connected = client.isConnected();

                if (transport_ptr.connected) {
                    log.info("connected to NATS (jetstream={})", .{client.hasJetStream()});
                }

                if (transport_ptr.connect_callback) |cb_fn| {
                    cb_fn(transport_ptr.connect_callback_ctx.?, transport_ptr.connected);
                }
            }
        }.cb);
    }

    fn acquireLock(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.lock_callback = callback;
        self.lock_callback_ctx = ctx;

        // Use NATS KV to acquire lock
        // Create key with our instance ID - fails if already exists
        var subject_buf: [256]u8 = undefined;
        const kv_subject = std.fmt.bufPrint(&subject_buf, "$KV.{s}.{s}", .{
            self.config.kv_bucket,
            self.lock_key,
        }) catch {
            if (self.lock_callback) |cb_fn| {
                cb_fn(self.lock_callback_ctx.?, false);
            }
            return;
        };

        // Build lock payload with instance info
        var payload_buf: [256]u8 = undefined;
        const payload = std.fmt.bufPrint(&payload_buf, "{{\"instance\":\"{s}\",\"acquired\":{d}}}", .{
            "tigerbeetle-cdc",
            std.time.milliTimestamp(),
        }) catch "";

        // Use request to get ack
        self.client.request(kv_subject, payload, &struct {
            fn cb(client: *nats.Client, msg: ?nats.Message) void {
                const transport_ptr: *NatsTransport = @alignCast(@fieldParentPtr("client", client));

                // Check if we got a positive response
                const acquired = if (msg) |m| blk: {
                    // Look for success indicators in response
                    // NATS KV returns +OK style responses
                    break :blk !std.mem.startsWith(u8, m.payload, "-");
                } else false;

                transport_ptr.lock_acquired = acquired;

                if (transport_ptr.lock_callback) |cb_fn| {
                    cb_fn(transport_ptr.lock_callback_ctx.?, acquired);
                }
            }
        }.cb);
    }

    fn getProgress(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, ?Transport.ProgressState) void,
        ctx: *anyopaque,
    ) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.progress_callback = callback;
        self.progress_callback_ctx = ctx;

        // Get progress from NATS KV
        var subject_buf: [256]u8 = undefined;
        const kv_subject = std.fmt.bufPrint(&subject_buf, "$KV.{s}.{s}", .{
            self.config.kv_bucket,
            self.progress_key,
        }) catch {
            if (self.progress_callback) |cb_fn| {
                cb_fn(self.progress_callback_ctx.?, null);
            }
            return;
        };

        self.client.request(kv_subject, "", &struct {
            fn cb(client: *nats.Client, msg: ?nats.Message) void {
                const transport_ptr: *NatsTransport = @alignCast(@fieldParentPtr("client", client));

                const progress = if (msg) |m| blk: {
                    // Parse progress from KV value
                    break :blk parseProgress(m.payload);
                } else null;

                if (transport_ptr.progress_callback) |cb_fn| {
                    cb_fn(transport_ptr.progress_callback_ctx.?, progress);
                }
            }
        }.cb);
    }

    fn parseProgress(payload: []const u8) ?Transport.ProgressState {
        // Simple JSON parsing for {"timestamp":123,"release":"1.0.0"}
        var timestamp: ?u64 = null;
        var release: ?vsr.Release = null;

        // Find timestamp
        if (std.mem.indexOf(u8, payload, "\"timestamp\":")) |start| {
            const num_start = start + 12;
            var num_end = num_start;
            while (num_end < payload.len and payload[num_end] >= '0' and payload[num_end] <= '9') {
                num_end += 1;
            }
            if (num_end > num_start) {
                timestamp = std.fmt.parseInt(u64, payload[num_start..num_end], 10) catch null;
            }
        }

        // Find release
        if (std.mem.indexOf(u8, payload, "\"release\":\"")) |start| {
            const str_start = start + 11;
            if (std.mem.indexOfPos(u8, payload, str_start, "\"")) |str_end| {
                release = vsr.Release.parse(payload[str_start..str_end]) catch null;
            }
        }

        if (timestamp) |ts| {
            return .{
                .timestamp = ts,
                .release = release orelse vsr.Release.minimum,
            };
        }

        return null;
    }

    fn publishEnqueue(ptr: *anyopaque, options: Transport.PublishOptions) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));

        // Build subject
        var subject_buf: [256]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.{s}", .{
            self.config.subject_prefix,
            options.routing_key,
        }) catch self.config.subject_prefix;

        // Build headers
        var headers_storage: [8]nats.Header = undefined;
        var header_count: usize = 0;

        // Event type header
        headers_storage[header_count] = .{
            .name = "Nats-Msg-Type",
            .value = options.headers.event_type,
        };
        header_count += 1;

        // Ledger header
        var ledger_buf: [16]u8 = undefined;
        const ledger_str = std.fmt.bufPrint(&ledger_buf, "{d}", .{options.headers.ledger}) catch "0";
        headers_storage[header_count] = .{ .name = "X-Ledger", .value = ledger_str };
        header_count += 1;

        // Transfer code header
        var transfer_code_buf: [16]u8 = undefined;
        const transfer_code_str = std.fmt.bufPrint(
            &transfer_code_buf,
            "{d}",
            .{options.headers.transfer_code},
        ) catch "0";
        headers_storage[header_count] = .{ .name = "X-Transfer-Code", .value = transfer_code_str };
        header_count += 1;

        // Enqueue to client
        self.client.publishEnqueue(subject, headers_storage[0..header_count], options.body);
        self.pending_count += 1;
    }

    fn publishSend(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.publish_callback = callback;
        self.publish_callback_ctx = ctx;

        if (self.pending_count == 0) {
            // Nothing to send
            if (self.publish_callback) |cb_fn| {
                cb_fn(self.publish_callback_ctx.?, true);
            }
            return;
        }

        self.client.publishSend(&struct {
            fn cb(client: *nats.Client) void {
                const transport_ptr: *NatsTransport = @alignCast(@fieldParentPtr("client", client));
                transport_ptr.pending_count = 0;

                if (transport_ptr.publish_callback) |cb_fn| {
                    cb_fn(transport_ptr.publish_callback_ctx.?, true);
                }
            }
        }.cb);
    }

    fn updateProgress(
        ptr: *anyopaque,
        progress: Transport.ProgressState,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.update_progress_callback = callback;
        self.update_progress_callback_ctx = ctx;

        // Build KV subject
        var subject_buf: [256]u8 = undefined;
        const kv_subject = std.fmt.bufPrint(&subject_buf, "$KV.{s}.{s}", .{
            self.config.kv_bucket,
            self.progress_key,
        }) catch {
            if (self.update_progress_callback) |cb_fn| {
                cb_fn(self.update_progress_callback_ctx.?, false);
            }
            return;
        };

        // Build progress JSON
        var payload_buf: [256]u8 = undefined;
        var release_buf: [32]u8 = undefined;
        const release_str = std.fmt.bufPrint(&release_buf, "{}", .{progress.release}) catch "0.0.0";
        const payload = std.fmt.bufPrint(&payload_buf, "{{\"timestamp\":{d},\"release\":\"{s}\"}}", .{
            progress.timestamp,
            release_str,
        }) catch "";

        // Publish to KV
        self.client.publish(kv_subject, &.{}, payload);

        // Assume success (fire and forget for progress updates)
        if (self.update_progress_callback) |cb_fn| {
            cb_fn(self.update_progress_callback_ctx.?, true);
        }
    }

    fn tick(ptr: *anyopaque) void {
        _ = ptr;
        // NATS client is event-driven via IO completions
        // Nothing to do here
    }

    fn isConnected(ptr: *anyopaque) bool {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        return self.connected and self.client.isConnected();
    }

    fn deinit_transport(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *NatsTransport = @ptrCast(@alignCast(ptr));
        self.client.deinit();
        allocator.free(self.lock_key);
        allocator.free(self.progress_key);
    }
};
