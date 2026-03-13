const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");

/// Abstract transport interface for CDC message publishing.
///
/// This interface allows the CDC runner to be transport-agnostic,
/// supporting multiple backends:
/// - AMQP (RabbitMQ)
/// - NATS (JetStream)
/// - Kafka (future)
/// - Pulsar (future)
///
/// Each transport must implement:
/// 1. Connection management
/// 2. Distributed locking (ensure only one CDC per cluster)
/// 3. Progress tracking (for recovery after restart)
/// 4. Batched publishing with acknowledgment
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Connect to the message broker.
        /// Callback is invoked when connection is established.
        connect: *const fn (
            self: *anyopaque,
            callback: *const fn (ctx: *anyopaque, success: bool) void,
            callback_ctx: *anyopaque,
        ) void,

        /// Attempt to acquire the distributed lock.
        /// Only one CDC instance per cluster should be active.
        /// Callback receives true if lock acquired, false otherwise.
        acquireLock: *const fn (
            self: *anyopaque,
            callback: *const fn (ctx: *anyopaque, acquired: bool) void,
            callback_ctx: *anyopaque,
        ) void,

        /// Retrieve the last published timestamp for recovery.
        /// Callback receives null if no progress exists (start from beginning).
        getProgress: *const fn (
            self: *anyopaque,
            callback: *const fn (ctx: *anyopaque, progress: ?ProgressState) void,
            callback_ctx: *anyopaque,
        ) void,

        /// Enqueue a message for batched publishing.
        /// Messages are held until publishSend is called.
        publishEnqueue: *const fn (
            self: *anyopaque,
            options: PublishOptions,
        ) void,

        /// Send all enqueued messages to the broker.
        /// Callback is invoked when broker acknowledges the batch.
        publishSend: *const fn (
            self: *anyopaque,
            callback: *const fn (ctx: *anyopaque, success: bool) void,
            callback_ctx: *anyopaque,
        ) void,

        /// Update progress after successful publish.
        /// Stores the timestamp for recovery on restart.
        updateProgress: *const fn (
            self: *anyopaque,
            progress: ProgressState,
            callback: *const fn (ctx: *anyopaque, success: bool) void,
            callback_ctx: *anyopaque,
        ) void,

        /// Drive I/O operations (called from event loop).
        tick: *const fn (self: *anyopaque) void,

        /// Check if connected and ready.
        isConnected: *const fn (self: *anyopaque) bool,

        /// Clean up resources.
        deinit: *const fn (self: *anyopaque, allocator: std.mem.Allocator) void,
    };

    /// Progress state for recovery.
    pub const ProgressState = struct {
        /// Last successfully published timestamp.
        timestamp: u64,
        /// TigerBeetle release version (for compatibility checks).
        release: vsr.Release,
    };

    /// Options for publishing a message.
    pub const PublishOptions = struct {
        /// Routing key / subject / topic.
        routing_key: []const u8,
        /// Message headers (transport-specific encoding).
        headers: Headers,
        /// Message body (JSON).
        body: []const u8,
        /// Event timestamp (nanoseconds).
        timestamp: u64,
    };

    /// Message headers for routing/filtering.
    pub const Headers = struct {
        event_type: []const u8,
        ledger: u32,
        transfer_code: u16,
        debit_account_code: u16,
        credit_account_code: u16,
    };

    // Delegate methods to vtable

    pub fn connect(
        self: Transport,
        callback: *const fn (ctx: *anyopaque, success: bool) void,
        callback_ctx: *anyopaque,
    ) void {
        self.vtable.connect(self.ptr, callback, callback_ctx);
    }

    pub fn acquireLock(
        self: Transport,
        callback: *const fn (ctx: *anyopaque, acquired: bool) void,
        callback_ctx: *anyopaque,
    ) void {
        self.vtable.acquireLock(self.ptr, callback, callback_ctx);
    }

    pub fn getProgress(
        self: Transport,
        callback: *const fn (ctx: *anyopaque, progress: ?ProgressState) void,
        callback_ctx: *anyopaque,
    ) void {
        self.vtable.getProgress(self.ptr, callback, callback_ctx);
    }

    pub fn publishEnqueue(self: Transport, options: PublishOptions) void {
        self.vtable.publishEnqueue(self.ptr, options);
    }

    pub fn publishSend(
        self: Transport,
        callback: *const fn (ctx: *anyopaque, success: bool) void,
        callback_ctx: *anyopaque,
    ) void {
        self.vtable.publishSend(self.ptr, callback, callback_ctx);
    }

    pub fn updateProgress(
        self: Transport,
        progress: ProgressState,
        callback: *const fn (ctx: *anyopaque, success: bool) void,
        callback_ctx: *anyopaque,
    ) void {
        self.vtable.updateProgress(self.ptr, progress, callback, callback_ctx);
    }

    pub fn tick(self: Transport) void {
        self.vtable.tick(self.ptr);
    }

    pub fn isConnected(self: Transport) bool {
        return self.vtable.isConnected(self.ptr);
    }

    pub fn deinit(self: Transport, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

/// Transport type enumeration for CLI.
pub const TransportType = enum {
    amqp,
    nats,
    // Future:
    // kafka,
    // pulsar,
};

/// Common configuration shared across transports.
pub const CommonConfig = struct {
    /// TigerBeetle cluster ID (used for lock/progress key namespacing).
    cluster_id: u128,
    /// Maximum messages per batch.
    message_count_max: u32,
    /// Maximum message body size.
    message_body_size_max: u32,
    /// Reply/ack timeout in ticks.
    reply_timeout_ticks: u64,
};
