//! The Workload drives an end-to-end test: from client requests, through consensus and the state
//! machine, down to the storage engine, and back.
//!
//! The Workload constructs messages to create and query accounts and transfers, and validates the
//! replies.
//!
//! Goals:
//!
//! * Run in a fixed amount of memory. (For long-running tests or performance testing).
//! * Query and verify transfers arbitrarily far back. (To exercise the storage engine).
//!
//! Transfer Encoding:
//!
//! * `Transfer.id` is a deterministic, reversible permutation of an ascending index.
//! * With the transfer's index as a seed, the Workload knows the eventual outcome of the transfer.
//! * `Transfer.user_data` is a checksum of the remainder of the transfer's data
//!   (excluding `timestamp` and `user_data` itself). This helps `on_lookup_transfers` to
//!   validate its results.
//!
const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const maybe = stdx.maybe;
const Ratio = stdx.PRNG.Ratio;
const ratio = stdx.PRNG.ratio;

const constants = @import("../constants.zig");
const tb = @import("../tigerbeetle.zig");
const vsr = @import("../vsr.zig");
const accounting_auditor = @import("auditor.zig");
const Auditor = accounting_auditor.AccountingAuditor;
const IteratorForCreateType = accounting_auditor.IteratorForCreateType;
const IdPermutation = @import("../testing/id.zig").IdPermutation;
const TimestampRange = @import("../lsm/timestamp_range.zig").TimestampRange;
const fuzz = @import("../testing/fuzz.zig");

const PriorityQueue = std.PriorityQueue;

const TransferOutcome = enum {
    /// The transfer is guaranteed to commit.
    /// For example, a single-phase transfer between valid accounts without balance limits.
    success,
    /// The transfer is invalid. For example, the `ledger` field is missing.
    failure,
    /// Due to races with timeouts or other transfers, the outcome of the transfer is uncertain.
    /// For example, post/void-pending transfers race with their timeout.
    unknown,
};

/// A Transfer generated from the plan is guaranteed to have a matching `outcome`, but it may use a
/// different Method. (For example, `method=pending` may fall back to `method=single_phase` if the
/// Auditor's pending transfer queue is full).
const TransferPlan = struct {
    /// When false, send invalid payments that are guaranteed to be rejected with an error.
    valid: bool,

    /// When `limit` is set, at least one of the following is true:
    ///
    /// * the debit account has debits_must_not_exceed_credits
    /// * the credit account has credits_must_not_exceed_debits
    ///
    limit: bool,

    method: Method,

    const Method = enum {
        single_phase,
        pending,
        post_pending,
        void_pending,
    };

    fn outcome(self: TransferPlan) TransferOutcome {
        if (!self.valid) return .failure;
        if (self.limit) return .unknown;
        return switch (self.method) {
            .single_phase, .pending => .success,
            .post_pending, .void_pending => .unknown,
        };
    }
};

const TransferTemplate = struct {
    ledger: u32,
    result: accounting_auditor.CreateTransferResultSet,
};

const TransferBatchQueue = PriorityQueue(TransferBatch, void, struct {
    /// Ascending order.
    fn compare(_: void, a: TransferBatch, b: TransferBatch) std.math.Order {
        assert(a.min != b.min);
        assert(a.max != b.max);
        return std.math.order(a.min, b.min);
    }
}.compare);

const TransferBatch = struct {
    /// Index of the first transfer in the batch.
    min: usize,
    /// Index of the last transfer in the batch.
    max: usize,
};

/// Indexes: [valid:bool][limit:bool][method]
const transfer_templates = table: {
    @setEvalBranchQuota(4_000);

    const SNGL = @intFromEnum(TransferPlan.Method.single_phase);
    const PEND = @intFromEnum(TransferPlan.Method.pending);
    const POST = @intFromEnum(TransferPlan.Method.post_pending);
    const VOID = @intFromEnum(TransferPlan.Method.void_pending);
    const Result = accounting_auditor.CreateTransferResultSet;
    const result = Result.init;

    const InitValues = std.enums.EnumFieldStruct(
        tb.CreateTransferResult.Ordered,
        bool,
        false,
    );
    const two_phase_ok: InitValues = .{
        .ok = true,
        .pending_transfer_already_posted = true,
        .pending_transfer_already_voided = true,
        .pending_transfer_expired = true,
    };

    const limits = result(.{
        .exceeds_credits = true,
        .exceeds_debits = true,
    });

    const either = struct {
        fn either(a: Result, b: Result) Result {
            var c = a;
            c.setUnion(b);
            return c;
        }
    }.either;

    const template = struct {
        fn template(ledger: u32, transfer_result: Result) TransferTemplate {
            return .{
                .ledger = ledger,
                .result = transfer_result,
            };
        }
    }.template;

    // [valid:bool][limit:bool][method]
    var templates: [2][2][std.meta.fields(TransferPlan.Method).len]TransferTemplate = undefined;

    // template(ledger, result)
    templates[0][0][SNGL] = template(0, result(.{ .ledger_must_not_be_zero = true }));
    templates[0][0][PEND] = template(0, result(.{ .ledger_must_not_be_zero = true }));
    templates[0][0][POST] = template(9, result(.{ .pending_transfer_has_different_ledger = true }));
    templates[0][0][VOID] = template(9, result(.{ .pending_transfer_has_different_ledger = true }));

    templates[0][1][SNGL] = template(0, result(.{ .ledger_must_not_be_zero = true }));
    templates[0][1][PEND] = template(0, result(.{ .ledger_must_not_be_zero = true }));
    templates[0][1][POST] = template(9, result(.{ .pending_transfer_has_different_ledger = true }));
    templates[0][1][VOID] = template(9, result(.{ .pending_transfer_has_different_ledger = true }));

    templates[1][0][SNGL] = template(1, result(.{ .ok = true }));
    templates[1][0][PEND] = template(1, result(.{ .ok = true }));
    templates[1][0][POST] = template(1, result(two_phase_ok));
    templates[1][0][VOID] = template(1, result(two_phase_ok));

    templates[1][1][SNGL] = template(1, either(limits, result(.{ .ok = true })));
    templates[1][1][PEND] = template(1, either(limits, result(.{ .ok = true })));
    templates[1][1][POST] = template(1, either(limits, result(two_phase_ok)));
    templates[1][1][VOID] = template(1, either(limits, result(two_phase_ok)));

    break :table templates;
};

pub fn WorkloadType(comptime AccountingStateMachine: type) type {
    const Operation = AccountingStateMachine.Operation;

    const Action = enum(u8) {
        create_accounts = @intFromEnum(Operation.create_accounts),
        create_transfers = @intFromEnum(Operation.create_transfers),
        lookup_accounts = @intFromEnum(Operation.lookup_accounts),
        lookup_transfers = @intFromEnum(Operation.lookup_transfers),
        get_account_transfers = @intFromEnum(Operation.get_account_transfers),
        get_account_balances = @intFromEnum(Operation.get_account_balances),
        query_accounts = @intFromEnum(Operation.query_accounts),
        query_transfers = @intFromEnum(Operation.query_transfers),
        get_change_events = @intFromEnum(Operation.get_change_events),

        deprecated_create_accounts_unbatched = @intFromEnum(
            Operation.deprecated_create_accounts_unbatched,
        ),
        deprecated_create_transfers_unbatched = @intFromEnum(
            Operation.deprecated_create_transfers_unbatched,
        ),
        deprecated_lookup_accounts_unbatched = @intFromEnum(
            Operation.deprecated_lookup_accounts_unbatched,
        ),
        deprecated_lookup_transfers_unbatched = @intFromEnum(
            Operation.deprecated_lookup_transfers_unbatched,
        ),
        deprecated_get_account_transfers_unbatched = @intFromEnum(
            Operation.deprecated_get_account_transfers_unbatched,
        ),
        deprecated_get_account_balances_unbatched = @intFromEnum(
            Operation.deprecated_get_account_balances_unbatched,
        ),
        deprecated_query_accounts_unbatched = @intFromEnum(
            Operation.deprecated_query_accounts_unbatched,
        ),
        deprecated_query_transfers_unbatched = @intFromEnum(
            Operation.deprecated_query_transfers_unbatched,
        ),
    };

    const Lookup = enum {
        /// Query a transfer that has either been committed or rejected.
        delivered,
        /// Query a transfer whose `create_transfers` is in-flight.
        sending,
    };

    return struct {
        const Workload = @This();

        pub const Options = OptionsType(AccountingStateMachine, Action, Lookup);

        prng: *stdx.PRNG,
        auditor: Auditor,
        options: Options,

        transfer_plan_seed: u64,

        /// Whether a `create_accounts` message has ever been sent.
        accounts_sent: bool = false,

        /// The index of the next transfer to send.
        transfers_sent: usize = 0,

        /// All transfers below this index have been delivered.
        /// Any transfers above this index that have been delivered are stored in
        /// `transfers_delivered_recently`.
        transfers_delivered_past: usize = 0,

        /// Track index ranges of `create_transfers` batches that have committed but are greater
        /// than or equal to `transfers_delivered_past` (which is still in-flight).
        transfers_delivered_recently: TransferBatchQueue,

        /// Track the number of pending transfers that have been sent but not committed.
        transfers_pending_in_flight: usize = 0,

        /// Transfers that succeeded and must result in `exists` when retried.
        transfers_retry_exists: std.ArrayListUnmanaged(tb.Transfer),

        /// IDs of transfers that failed with transient codes
        /// and must result in `id_already_failed` when retried.
        transfers_retry_failed: std.AutoArrayHashMapUnmanaged(u128, void),

        pub fn init(
            allocator: std.mem.Allocator,
            prng: *stdx.PRNG,
            options: Options,
        ) !Workload {
            assert(options.accounts_batch_size_span + options.accounts_batch_size_min <=
                AccountingStateMachine.batch_max.create_accounts);
            assert(options.accounts_batch_size_span >= 1);
            assert(options.transfers_batch_size_span + options.transfers_batch_size_min <=
                AccountingStateMachine.batch_max.create_transfers);
            assert(options.transfers_batch_size_span >= 1);

            var auditor = try Auditor.init(allocator, prng, options.auditor_options);
            errdefer auditor.deinit(allocator);

            var transfers_delivered_recently = TransferBatchQueue.init(allocator, {});
            errdefer transfers_delivered_recently.deinit();
            try transfers_delivered_recently.ensureTotalCapacity(
                options.auditor_options.client_count * constants.client_request_queue_max,
            );

            for (auditor.accounts, 0..) |*account, i| {
                const query_intersection =
                    auditor.query_intersections[prng.index(auditor.query_intersections)];

                account.* = std.mem.zeroInit(tb.Account, .{
                    .id = auditor.account_index_to_id(i),
                    .user_data_64 = query_intersection.user_data_64,
                    .user_data_32 = query_intersection.user_data_32,
                    .code = query_intersection.code,
                    .ledger = 1,
                });

                if (prng.chance(options.account_limit_probability)) {
                    const b = prng.boolean();
                    account.flags.debits_must_not_exceed_credits = b;
                    account.flags.credits_must_not_exceed_debits = !b;
                }

                account.flags.history = prng.chance(options.account_history_probability);
            }

            var transfers_retry_failed: std.AutoArrayHashMapUnmanaged(u128, void) = .{};
            try transfers_retry_failed.ensureTotalCapacity(
                allocator,
                options.transfers_retry_failed_max,
            );
            errdefer transfers_retry_failed.deinit(allocator);

            var transfers_retry_exists: std.ArrayListUnmanaged(tb.Transfer) = try .initCapacity(
                allocator,
                options.transfers_retry_exists_max,
            );
            errdefer transfers_retry_exists.deinit(allocator);

            return .{
                .prng = prng,
                .auditor = auditor,
                .options = options,
                .transfer_plan_seed = prng.int(u64),
                .transfers_delivered_recently = transfers_delivered_recently,
                .transfers_retry_failed = transfers_retry_failed,
                .transfers_retry_exists = transfers_retry_exists,
            };
        }

        pub fn deinit(self: *Workload, allocator: std.mem.Allocator) void {
            self.auditor.deinit(allocator);
            self.transfers_delivered_recently.deinit();
            self.transfers_retry_failed.deinit(allocator);
            self.transfers_retry_exists.deinit(allocator);
        }

        pub fn done(self: *const Workload) bool {
            if (self.transfers_delivered_recently.len != 0) return false;
            return self.auditor.done();
        }

        /// A client may build multiple requests to queue up while another is in-flight.
        pub fn build_request(
            self: *Workload,
            client_index: usize,
            body_buffer: []align(@alignOf(vsr.Header)) u8,
        ) struct {
            operation: Operation,
            size: usize,
        } {
            assert(client_index < self.auditor.options.client_count);
            assert(body_buffer.len == constants.message_body_size_max);

            const action = action: {
                if (!self.accounts_sent and self.prng.boolean()) {
                    // Early in the test make sure some accounts get created.
                    self.accounts_sent = true;
                    break :action .create_accounts;
                }

                break :action self.prng.enum_weighted(Action, self.options.operations);
            };

            const operation: Operation = @enumFromInt(@intFromEnum(action));
            const event_size: u32 = operation.event_size();
            const event_max: u32 = operation.event_max(self.options.batch_size_limit);
            assert(event_max > 0);
            assert(body_buffer.len >= event_size * event_max);

            const result_size: u32 = operation.result_size();
            const result_max = operation.result_max(self.options.batch_size_limit);
            assert(result_max > 0);
            assert(constants.message_body_size_max >=
                result_size * result_max);

            if (!operation.is_multi_batch()) {
                const size = self.build_request_batch(
                    client_index,
                    action,
                    body_buffer,
                    event_max,
                );
                assert(size <= body_buffer.len);
                return .{
                    .operation = operation,
                    .size = size,
                };
            }
            assert(operation.is_multi_batch());

            var body_encoder = vsr.multi_batch.MultiBatchEncoder.init(
                body_buffer[0..self.options.batch_size_limit],
                .{
                    .element_size = event_size,
                },
            );
            var event_count: u32 = 0;
            var result_count: u32 = 0;
            for (0..self.options.multi_batch_per_request_limit) |_| {
                const writable = body_encoder.writable() orelse break;
                if (writable.len == 0) break;

                const event_count_remain: u32 =
                    if (operation.is_batchable())
                        event_max - event_count
                    else
                        1;
                const batch_size = self.build_request_batch(
                    client_index,
                    action,
                    writable,
                    event_count_remain,
                );
                assert(batch_size <= writable.len);

                // Checking if the expected result will fit in the multi-batch reply.
                const reply_trailer_size: u32 = vsr.multi_batch.trailer_total_size(.{
                    .element_size = result_size,
                    .batch_count = body_encoder.batch_count + 1,
                });
                const result_count_expected: u32 =
                    operation.result_count_expected(writable[0..batch_size]);
                const reply_message_size: u32 =
                    ((result_count + result_count_expected) * result_size) + reply_trailer_size;
                if (reply_message_size > constants.message_body_size_max) {
                    // For operations that produce 1:1 result per event
                    // (e.g., `create_*` and `lookup_*`), this was already validated
                    // when checking if `event_count` fits within the multi-batch request.
                    assert(!operation.is_batchable());
                    break;
                }
                assert(result_count + result_count_expected <= result_max);

                body_encoder.add(@intCast(batch_size));
                event_count += @intCast(@divExact(batch_size, event_size));
                assert(event_count <= event_max);

                result_count += result_count_expected;
                assert(result_count <= result_max);

                // Maybe single-batch request.
                if (body_encoder.batch_count == 1 and self.prng.boolean()) break;
            }
            maybe(event_count == 0);
            assert(result_count == 0 or event_count > 0);
            assert(body_encoder.batch_count > 0);
            assert(body_encoder.batch_count <= self.options.multi_batch_per_request_limit);

            const bytes_written = body_encoder.finish();
            assert(bytes_written <= self.options.batch_size_limit);

            return .{
                .operation = operation,
                .size = bytes_written,
            };
        }

        fn build_request_batch(
            self: *Workload,
            client_index: usize,
            action: Action,
            body: []u8,
            batch_limit: u32,
        ) usize {
            switch (action) {
                inline else => |action_comptime| {
                    const operation_comptime = comptime std.enums.nameCast(
                        Operation,
                        action_comptime,
                    );
                    const Event = operation_comptime.EventType();
                    const event_size: u32 = operation_comptime.event_size();
                    const batchable: []Event = self.batch(
                        Event,
                        action_comptime,
                        body,
                        batch_limit,
                    );
                    assert(batchable.len <= batch_limit);

                    const count = switch (action_comptime) {
                        .create_accounts,
                        .deprecated_create_accounts_unbatched,
                        => self.build_create_accounts(
                            client_index,
                            batchable,
                        ),
                        .create_transfers,
                        .deprecated_create_transfers_unbatched,
                        => self.build_create_transfers(
                            client_index,
                            batchable,
                        ),
                        .lookup_accounts,
                        .deprecated_lookup_accounts_unbatched,
                        => self.build_lookup_accounts(batchable),
                        .lookup_transfers,
                        .deprecated_lookup_transfers_unbatched,
                        => self.build_lookup_transfers(batchable),
                        .get_account_transfers,
                        .get_account_balances,
                        .deprecated_get_account_transfers_unbatched,
                        .deprecated_get_account_balances_unbatched,
                        => self.build_get_account_filter(
                            client_index,
                            action_comptime,
                            batchable,
                        ),
                        .query_accounts,
                        .query_transfers,
                        .deprecated_query_accounts_unbatched,
                        .deprecated_query_transfers_unbatched,
                        => self.build_query_filter(
                            client_index,
                            action_comptime,
                            batchable,
                        ),
                        .get_change_events => self.build_get_change_events_filter(
                            client_index,
                            batchable,
                        ),
                    };
                    assert(count <= batchable.len);
                    assert(count <= batch_limit);

                    const batch_size: usize = count * event_size;
                    assert(batch_size <= body.len);
                    maybe(batch_size == 0);
                    return batch_size;
                },
            }
        }

        /// `on_reply` is called for replies in commit order.
        pub fn on_reply(
            self: *Workload,
            client_index: usize,
            operation: Operation,
            timestamp: u64,
            request_body: []const u8,
            reply_body: []const u8,
        ) void {
            assert(timestamp != 0);
            assert(request_body.len <= constants.message_body_size_max);
            assert(reply_body.len <= constants.message_body_size_max);

            if (!operation.is_multi_batch()) {
                return self.on_reply_batch(
                    client_index,
                    operation,
                    timestamp,
                    request_body,
                    reply_body,
                );
            }
            assert(operation.is_multi_batch());

            const event_size: u32 = operation.event_size();
            const result_size: u32 = operation.result_size();
            var body_decoder = vsr.multi_batch.MultiBatchDecoder.init(request_body, .{
                .element_size = event_size,
            }) catch unreachable;
            assert(body_decoder.batch_count() > 0);
            var reply_decoder = vsr.multi_batch.MultiBatchDecoder.init(reply_body, .{
                .element_size = result_size,
            }) catch unreachable;
            assert(reply_decoder.batch_count() > 0);
            assert(body_decoder.batch_count() == reply_decoder.batch_count());

            const prepare_nanoseconds = struct {
                fn prepare_nanoseconds(
                    operation_inner: Operation,
                    input_len: usize,
                    batch_size_limit: u32,
                ) u64 {
                    return switch (operation_inner) {
                        .pulse => Operation.create_transfers.event_max(
                            batch_size_limit,
                        ),
                        .create_accounts => @divExact(input_len, @sizeOf(tb.Account)),
                        .create_transfers => @divExact(input_len, @sizeOf(tb.Transfer)),
                        .lookup_accounts => 0,
                        .lookup_transfers => 0,
                        .get_account_transfers => 0,
                        .get_account_balances => 0,
                        .query_accounts => 0,
                        .query_transfers => 0,
                        .get_change_events => 0,
                        else => unreachable,
                    };
                }
            }.prepare_nanoseconds;
            var batch_timestamp: u64 = timestamp - prepare_nanoseconds(
                operation,
                body_decoder.payload.len,
                self.options.batch_size_limit,
            );
            while (body_decoder.pop()) |batch_body| {
                const batch_reply = reply_decoder.pop().?;
                batch_timestamp += prepare_nanoseconds(
                    operation,
                    batch_body.len,
                    self.options.batch_size_limit,
                );
                self.on_reply_batch(
                    client_index,
                    operation,
                    batch_timestamp,
                    batch_body,
                    batch_reply,
                );
            }
            assert(reply_decoder.pop() == null);
        }

        pub fn on_reply_batch(
            self: *Workload,
            client_index: usize,
            operation: Operation,
            timestamp: u64,
            request_body: []const u8,
            reply_body: []const u8,
        ) void {
            switch (operation) {
                .create_accounts,
                .deprecated_create_accounts_unbatched,
                => self.auditor.on_create_accounts(
                    client_index,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.Account, request_body),
                    stdx.bytes_as_slice(.exact, tb.CreateAccountsResult, reply_body),
                ),
                .create_transfers,
                .deprecated_create_transfers_unbatched,
                => self.on_create_transfers(
                    client_index,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.Transfer, request_body),
                    stdx.bytes_as_slice(.exact, tb.CreateTransfersResult, reply_body),
                ),
                .lookup_accounts,
                .deprecated_lookup_accounts_unbatched,
                => self.auditor.on_lookup_accounts(
                    client_index,
                    timestamp,
                    stdx.bytes_as_slice(.exact, u128, request_body),
                    stdx.bytes_as_slice(.exact, tb.Account, reply_body),
                ),
                .lookup_transfers,
                .deprecated_lookup_transfers_unbatched,
                => self.on_lookup_transfers(
                    client_index,
                    timestamp,
                    stdx.bytes_as_slice(.exact, u128, request_body),
                    stdx.bytes_as_slice(.exact, tb.Transfer, reply_body),
                ),
                inline .get_account_transfers,
                .deprecated_get_account_transfers_unbatched,
                => |operation_comptime| self.on_get_account_transfers(
                    operation_comptime,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.AccountFilter, request_body),
                    stdx.bytes_as_slice(.exact, tb.Transfer, reply_body),
                ),
                inline .get_account_balances,
                .deprecated_get_account_balances_unbatched,
                => |operation_comptime| self.on_get_account_balances(
                    operation_comptime,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.AccountFilter, request_body),
                    stdx.bytes_as_slice(.exact, tb.AccountBalance, reply_body),
                ),
                inline .query_accounts,
                .deprecated_query_accounts_unbatched,
                => |operation_comptime| self.on_query(
                    operation_comptime,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.QueryFilter, request_body),
                    stdx.bytes_as_slice(.exact, tb.Account, reply_body),
                ),
                inline .query_transfers,
                .deprecated_query_transfers_unbatched,
                => |operation_comptime| self.on_query(
                    operation_comptime,
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.QueryFilter, request_body),
                    stdx.bytes_as_slice(.exact, tb.Transfer, reply_body),
                ),
                .get_change_events => self.on_get_change_events(
                    timestamp,
                    stdx.bytes_as_slice(.exact, tb.ChangeEventsFilter, request_body),
                    stdx.bytes_as_slice(.exact, tb.ChangeEvent, reply_body),
                ),
                //Not handled by the client.
                .pulse => unreachable,
            }
        }

        /// `on_pulse` is called for pulse operations in commit order.
        pub fn on_pulse(
            self: *Workload,
            operation: Operation,
            timestamp: u64,
        ) void {
            assert(timestamp != 0);
            assert(operation == .pulse);

            self.auditor.expire_pending_transfers(timestamp);
        }

        fn build_create_accounts(
            self: *Workload,
            client_index: usize,
            accounts: []tb.Account,
        ) usize {
            const results = self.auditor.expect_create_accounts(client_index);
            for (accounts, 0..) |*account, i| {
                const account_index = self.prng.index(self.auditor.accounts);
                account.* = self.auditor.accounts[account_index];
                account.debits_pending = 0;
                account.debits_posted = 0;
                account.credits_pending = 0;
                account.credits_posted = 0;
                account.timestamp = 0;
                results[i] = accounting_auditor.CreateAccountResultSet{};

                if (self.prng.chance(self.options.create_account_invalid_probability)) {
                    account.ledger = 0;
                    // The result depends on whether the id already exists:
                    results[i].insert(.exists_with_different_ledger);
                    results[i].insert(.ledger_must_not_be_zero);
                } else {
                    if (!self.auditor.accounts_state[account_index].created) {
                        results[i].insert(.ok);
                    }
                    // Even if the account doesn't exist yet, we may race another request.
                    results[i].insert(.exists);
                }
                assert(results[i].count() > 0);
            }
            return accounts.len;
        }

        fn build_create_transfers(
            self: *Workload,
            client_index: usize,
            transfers: []tb.Transfer,
        ) usize {
            const results = self.auditor.expect_create_transfers(client_index);
            assert(results.len >= transfers.len);
            var transfers_count: usize = transfers.len;
            var i: usize = 0;
            while (i < transfers_count) {
                const transfer_index = self.transfers_sent;
                const transfer_plan = self.transfer_index_to_plan(transfer_index);
                const transfer_id = self.transfer_index_to_id(transfer_index);
                results[i] = self.build_transfer(
                    transfer_id,
                    transfer_plan,
                    &transfers[i],
                ) orelse {
                    // This transfer index can't be built; stop with what we have so far.
                    // Hopefully it will be unblocked before the next `create_transfers`.
                    transfers_count = i;
                    break;
                };

                if (i != 0 and results[i].count() == 1 and results[i - 1].count() == 1) {
                    // To support random `lookup_transfers`, linked transfers can't be planned.
                    // Instead, link transfers opportunistically, when consecutive transfers can be
                    // linked without altering any of their outcomes.

                    if (results[i].contains(.ok) and results[i - 1].contains(.ok) and
                        self.prng.chance(self.options.linked_valid_probability))
                    {
                        transfers[i - 1].flags.linked = true;
                    }

                    if (!results[i].contains(.ok) and !results[i - 1].contains(.ok) and
                        self.prng.chance(self.options.linked_invalid_probability))
                    {
                        // Convert the previous transfer to a single-phase no-limit transfer, but
                        // link it to the current transfer — it will still fail.
                        const result_set_opt = self.build_transfer(transfers[i - 1].id, .{
                            .valid = true,
                            .limit = false,
                            .method = .single_phase,
                        }, &transfers[i - 1]);
                        if (result_set_opt) |result_set| {
                            assert(result_set.count() == 1);
                            assert(result_set.contains(.ok));

                            transfers[i - 1].flags.linked = true;
                            results[i - 1] = accounting_auditor.CreateTransferResultSet.init(.{
                                .linked_event_failed = true,
                            });
                        }
                    }
                }
                assert(results[i].count() > 0);

                if (transfers[i].flags.pending) self.transfers_pending_in_flight += 1;
                i += 1;
                self.transfers_sent += 1;
            }
            assert(transfers_count == i);
            assert(transfers_count <= transfers.len);

            self.build_retry_transfers(transfers[0..transfers_count], results);

            // Checksum transfers only after the whole batch is ready.
            // The opportunistic linking backtracks to modify transfers.
            for (transfers[0..transfers_count]) |*transfer| {
                transfer.user_data_128 = vsr.checksum(std.mem.asBytes(transfer));
            }

            return transfers_count;
        }

        fn build_retry_transfers(
            self: *Workload,
            transfers: []tb.Transfer,
            results: []accounting_auditor.CreateTransferResultSet,
        ) void {
            assert(results.len >= transfers.len);

            // Neither the first nor the last id can regress to preserve the
            // `transfers_delivered_recently` and `transfers_delivered_past` logic.
            // So we must insert retries in the middle of the batch.
            if (transfers.len <= 1) return;
            for (1..transfers.len - 1) |i| {
                if (self.transfers_retry_exists.items.len == 0 and
                    self.transfers_retry_failed.count() == 0) break;

                // To support random `lookup_transfers`, we replace the transfer with a retry,
                // without altering the outcome for this specific `transfer_index`.
                const transfer_index = self.transfer_id_to_index(transfers[i].id);
                const transfer_plan = self.transfer_index_to_plan(transfer_index);
                const can_retry = !transfer_plan.valid and
                    !transfers[i].flags.linked and
                    !transfers[i - 1].flags.linked;
                if (can_retry and
                    self.prng.chance(self.options.create_transfer_retry_probability))
                {
                    switch (self.prng.chances(.{
                        .exists = @intFromBool(self.transfers_retry_exists.items.len > 0),
                        .failed = @intFromBool(self.transfers_retry_failed.count() > 0),
                    })) {
                        .exists => {
                            // Retry a successfully completed transfer, result == `exists`.
                            const index = self.prng.index(self.transfers_retry_exists.items);
                            transfers[i] = self.transfers_retry_exists.swapRemove(index);
                            results[i] = .initOne(.exists);
                        },
                        .failed => {
                            // Retry a failed transfer ID, result == `id_already_failed`.
                            const index = self.prng.index(self.transfers_retry_failed.keys());
                            const id_failed = self.transfers_retry_failed.keys()[index];
                            self.transfers_retry_failed.swapRemoveAt(index);
                            transfers[i] = std.mem.zeroInit(tb.Transfer, .{ .id = id_failed });
                            results[i] = .initOne(.id_already_failed);
                        },
                    }
                }
            }
        }

        fn build_lookup_accounts(self: *Workload, lookup_ids: []u128) usize {
            for (lookup_ids) |*id| {
                if (self.prng.chance(self.options.lookup_account_invalid_probability)) {
                    // Pick an account with valid index (rather than "random.int(u128)") because the
                    // Auditor must decode the id to check for a matching account.
                    id.* = self.auditor.account_index_to_id(self.prng.int(usize));
                } else {
                    const account_index = self.prng.index(self.auditor.accounts);
                    id.* = self.auditor.accounts[account_index].id;
                }
            }
            return lookup_ids.len;
        }

        fn build_lookup_transfers(self: *const Workload, lookup_ids: []u128) usize {
            const delivered = self.transfers_delivered_past;
            const lookup_window = self.prng.enum_weighted(Lookup, self.options.lookup_transfer);
            const lookup_window_start = switch (lookup_window) {
                .delivered => self.prng.int_inclusive(usize, delivered),
                .sending => self.prng.range_inclusive(
                    usize,
                    delivered,
                    self.transfers_sent,
                ),
            };

            // +1 to make the span-max inclusive.
            const lookup_window_size = @min(
                fuzz.random_int_exponential(
                    self.prng,
                    usize,
                    self.options.lookup_transfer_span_mean,
                ),
                self.transfers_sent - lookup_window_start,
            );
            if (lookup_window_size == 0) return 0;

            for (lookup_ids) |*lookup_id| {
                lookup_id.* = self.transfer_index_to_id(
                    lookup_window_start + self.prng.int_inclusive(usize, lookup_window_size - 1),
                );
            }
            return lookup_ids.len;
        }

        fn build_get_account_filter(
            self: *const Workload,
            client_index: usize,
            comptime action: Action,
            body: []tb.AccountFilter,
        ) usize {
            _ = client_index;
            comptime assert(action == .get_account_transfers or
                action == .get_account_balances or
                action == .deprecated_get_account_transfers_unbatched or
                action == .deprecated_get_account_balances_unbatched);
            assert(body.len == 1);
            const account_filter = &body[0];
            account_filter.* = tb.AccountFilter{
                .account_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .code = 0,
                .limit = 0,
                .flags = .{
                    .credits = false,
                    .debits = false,
                    .reversed = false,
                },
                .timestamp_min = 0,
                .timestamp_max = 0,
            };

            account_filter.account_id = if (self.auditor.pick_account(.{
                .created = null,
                .debits_must_not_exceed_credits = null,
                .credits_must_not_exceed_debits = null,
            })) |account| account.id else
                // Pick an account with valid index (rather than "random.int(u128)") because the
                // Auditor must decode the id to check for a matching account.
                self.auditor.account_index_to_id(self.prng.int(usize));

            // It may be an invalid account.
            const account_state: ?*const Auditor.AccountState = self.auditor.get_account_state(
                account_filter.account_id,
            );

            account_filter.flags.reversed = self.prng.boolean();

            // The timestamp range is restrictive to the number of transfers inserted at the
            // moment the filter was generated. Only when this filter is in place we can assert
            // the expected result count.
            if (account_state != null and
                self.prng.chance(self.options.account_filter_timestamp_range_probability))
            {
                account_filter.flags.credits = true;
                account_filter.flags.debits = true;
                account_filter.limit = account_state.?.transfers_count(account_filter.flags);
                account_filter.timestamp_min = account_state.?.transfer_timestamp_min;
                account_filter.timestamp_max = account_state.?.transfer_timestamp_max;

                // Exclude the first or the last result depending on the sort order,
                // if there are more than one single transfer.
                account_filter.timestamp_min += @intFromBool(!account_filter.flags.reversed);
                account_filter.timestamp_max -|= @intFromBool(account_filter.flags.reversed);
            } else {
                switch (self.prng.enum_uniform(enum { none, debits, credits, all })) {
                    .none => {}, // Testing invalid flags.
                    .debits => account_filter.flags.debits = true,
                    .credits => account_filter.flags.credits = true,
                    .all => {
                        account_filter.flags.debits = true;
                        account_filter.flags.credits = true;
                    },
                }

                const operation = comptime std.enums.nameCast(Operation, action);
                const batch_result_max = operation.result_max(self.options.batch_size_limit);
                account_filter.limit = switch (self.prng.enum_uniform(enum {
                    zero,
                    one,
                    random,
                    batch_max,
                    int_max,
                })) {
                    .zero => 0,
                    .one => 1,
                    .random => self.prng.int_inclusive(u32, batch_result_max),
                    .batch_max => batch_result_max,
                    .int_max => std.math.maxInt(u32),
                };
            }

            return 1;
        }

        fn build_query_filter(
            self: *const Workload,
            client_index: usize,
            comptime action: Action,
            body: []tb.QueryFilter,
        ) usize {
            _ = client_index;
            comptime assert(action == .query_accounts or
                action == .query_transfers or
                action == .deprecated_query_accounts_unbatched or
                action == .deprecated_query_transfers_unbatched);
            assert(body.len == 1);
            const query_filter = &body[0];

            const operation = comptime std.enums.nameCast(Operation, action);
            const batch_result_max = operation.result_max(self.options.batch_size_limit);
            const limit: u32 = switch (self.prng.enum_uniform(enum {
                zero,
                one,
                random,
                batch_max,
                int_max,
            })) {
                .zero => 0,
                .one => 1,
                .random => self.prng.int_inclusive(u32, batch_result_max),
                .batch_max => batch_result_max,
                .int_max => std.math.maxInt(u32),
            };

            if (self.prng.chance(self.options.query_filter_not_found_probability)) {
                query_filter.* = .{
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .code = 0,
                    .ledger = 999, // Non-existent ledger
                    .limit = limit,
                    .flags = .{
                        .reversed = false,
                    },
                    .timestamp_min = 0,
                    .timestamp_max = 0,
                };
            } else {
                const query_intersection_index = self.prng.index(self.auditor.query_intersections);
                const query_intersection =
                    self.auditor.query_intersections[query_intersection_index];

                query_filter.* = .{
                    .user_data_128 = 0,
                    .user_data_64 = query_intersection.user_data_64,
                    .user_data_32 = query_intersection.user_data_32,
                    .code = query_intersection.code,
                    .ledger = 0,
                    .limit = limit,
                    .flags = .{
                        .reversed = self.prng.boolean(),
                    },
                    .timestamp_min = 0,
                    .timestamp_max = 0,
                };

                // Maybe filter by timestamp:
                const state = switch (action) {
                    .query_accounts,
                    .deprecated_query_accounts_unbatched,
                    => &query_intersection.accounts,
                    .query_transfers,
                    .deprecated_query_transfers_unbatched,
                    => &query_intersection.transfers,
                    else => unreachable,
                };

                if (state.count > 1 and state.count <= batch_result_max and
                    self.prng.chance(self.options.query_filter_timestamp_range_probability))
                {
                    // Excluding the first or last object:
                    if (query_filter.flags.reversed) {
                        query_filter.timestamp_min = state.timestamp_min;
                        query_filter.timestamp_max = state.timestamp_max - 1;
                    } else {
                        query_filter.timestamp_min = state.timestamp_min + 1;
                        query_filter.timestamp_max = state.timestamp_max;
                    }
                    // Later we can assert that results.len == count - 1:
                    query_filter.limit = state.count;
                }
            }

            return 1;
        }

        fn build_get_change_events_filter(
            self: *Workload,
            client_index: usize,
            body: []tb.ChangeEventsFilter,
        ) usize {
            _ = client_index;
            assert(body.len == 1);
            const filter = &body[0];

            const snapshot = self.auditor.changes_tracker.acquire_snapshot() orelse {
                // We can only track a limited set of events,
                // so we issue a query with an invalid filter when the results can't be asserted.
                filter.* = switch (self.prng.enum_uniform(enum {
                    zeroed,
                    invalid_timestamps,
                })) {
                    .zeroed => .{
                        .limit = 0,
                        .timestamp_min = 0,
                        .timestamp_max = 0,
                    },
                    .invalid_timestamps => filter: {
                        const timestamp: u64 = self.prng.range_inclusive(
                            u64,
                            TimestampRange.timestamp_min,
                            TimestampRange.timestamp_max,
                        );
                        break :filter .{
                            .limit = self.prng.int(u32),
                            .timestamp_min = timestamp + 1,
                            .timestamp_max = timestamp,
                        };
                    },
                };
                return 1;
            };
            assert(snapshot.count_total() > 0);

            const limit: u32 = switch (self.prng.enum_uniform(enum {
                exact,
                batch_max,
                int_max,
            })) {
                .exact => snapshot.count_total(),
                .batch_max => Operation.get_change_events.result_max(
                    self.options.batch_size_limit,
                ),
                .int_max => std.math.maxInt(u32),
            };
            filter.* = .{
                .limit = limit,
                .timestamp_min = snapshot.timestamp_min,
                .timestamp_max = snapshot.timestamp_max,
            };
            return 1;
        }

        /// The transfer built is guaranteed to match the TransferPlan's outcome.
        /// The transfer built is _not_ guaranteed to match the TransferPlan's method.
        ///
        /// Returns `null` if the transfer plan cannot be fulfilled (because there aren't enough
        /// accounts created).
        fn build_transfer(
            self: *Workload,
            transfer_id: u128,
            transfer_plan: TransferPlan,
            transfer: *tb.Transfer,
        ) ?accounting_auditor.CreateTransferResultSet {
            // If the specified method is unavailable, swap it.
            // Changing the method may narrow the TransferOutcome (unknown→success, unknown→failure)
            // but never broaden it (success→unknown, success→failure).
            const method = method: {
                const default = transfer_plan.method;
                if (default == .pending and
                    self.auditor.pending_expiries.count() + self.transfers_pending_in_flight ==
                        self.auditor.options.transfers_pending_max)
                {
                    break :method .single_phase;
                }

                if (default == .post_pending or default == .void_pending) {
                    if (self.auditor.pending_transfers.count() == 0) {
                        break :method .single_phase;
                    }
                }
                break :method default;
            };

            const index_valid = @intFromBool(transfer_plan.valid);
            const index_limit = @intFromBool(transfer_plan.limit);
            const index_method = @intFromEnum(method);
            const transfer_template = &transfer_templates[index_valid][index_limit][index_method];

            const limit_debits = transfer_plan.limit and self.prng.boolean();
            const limit_credits = transfer_plan.limit and (self.prng.boolean() or !limit_debits);
            assert(transfer_plan.limit == (limit_debits or limit_credits));

            const debit_account = self.auditor.pick_account(.{
                .created = true,
                .debits_must_not_exceed_credits = limit_debits,
                .credits_must_not_exceed_debits = null,
            }) orelse return null;
            assert(!limit_debits or debit_account.flags.debits_must_not_exceed_credits);

            const credit_account = self.auditor.pick_account(.{
                .created = true,
                .debits_must_not_exceed_credits = null,
                .credits_must_not_exceed_debits = limit_credits,
                .exclude = debit_account.id,
            }) orelse return null;
            assert(!limit_credits or credit_account.flags.credits_must_not_exceed_debits);

            const query_intersection_index = self.prng.index(
                self.auditor.query_intersections,
            );
            const query_intersection = self.auditor.query_intersections[query_intersection_index];

            transfer.* = .{
                .id = transfer_id,
                .debit_account_id = debit_account.id,
                .credit_account_id = credit_account.id,
                // "user_data_128" will be set to a checksum of the Transfer.
                .user_data_128 = 0,
                .user_data_64 = query_intersection.user_data_64,
                .user_data_32 = query_intersection.user_data_32,
                .code = query_intersection.code,
                .pending_id = 0,
                .timeout = 0,
                .ledger = transfer_template.ledger,
                .flags = .{},
                .timestamp = 0,
                .amount = self.prng.int_inclusive(u128, std.math.maxInt(u8)),
            };

            switch (method) {
                .single_phase => {},
                .pending => {
                    transfer.flags = .{ .pending = true };
                    // Bound the timeout to ensure we never hit `overflows_timeout`.
                    transfer.timeout = 1 + @as(u32, @min(
                        std.math.maxInt(u32) / 2,
                        fuzz.random_int_exponential(
                            self.prng,
                            u32,
                            self.options.pending_timeout_mean,
                        ),
                    ));
                },
                .post_pending, .void_pending => {
                    // Don't depend on `HashMap.keyIterator()` being deterministic.
                    // Pick a random "target" key, then post/void the id it is nearest to.
                    const target = self.prng.int(u128);
                    var previous: ?u128 = null;
                    var iterator = self.auditor.pending_transfers.keyIterator();
                    while (iterator.next()) |id| {
                        if (previous == null or
                            @max(target, id.*) - @min(target, id.*) <
                                @max(target, previous.?) - @min(target, previous.?))
                        {
                            previous = id.*;
                        }
                    }

                    // If there were no pending ids, the method would have been changed.
                    const pending_id = previous.?;
                    const pending_transfer = self.auditor.pending_transfers.getPtr(previous.?).?;
                    const dr = pending_transfer.debit_account_index;
                    const cr = pending_transfer.credit_account_index;
                    const pending_query_intersection = self.auditor
                        .query_intersections[pending_transfer.query_intersection_index];
                    // Don't use the default '0' parameters because the StateMachine overwrites 0s
                    // with the pending transfer's values, invalidating the post/void transfer
                    // checksum.
                    transfer.debit_account_id = self.auditor.account_index_to_id(dr);
                    transfer.credit_account_id = self.auditor.account_index_to_id(cr);
                    transfer.user_data_64 = pending_query_intersection.user_data_64;
                    transfer.user_data_32 = pending_query_intersection.user_data_32;
                    transfer.code = pending_query_intersection.code;
                    if (method == .post_pending) {
                        transfer.amount =
                            self.prng.range_inclusive(u128, 0, pending_transfer.amount);
                    } else {
                        transfer.amount = pending_transfer.amount;
                    }
                    transfer.pending_id = pending_id;
                    transfer.flags = .{
                        .post_pending_transfer = method == .post_pending,
                        .void_pending_transfer = method == .void_pending,
                    };
                },
            }
            assert(transfer_template.result.count() > 0);
            return transfer_template.result;
        }

        fn batch(
            self: *const Workload,
            comptime T: type,
            comptime action: Action,
            body: []u8,
            event_count_remain: u32,
        ) []T {
            const batch_min = switch (action) {
                .create_accounts,
                .lookup_accounts,
                .deprecated_create_accounts_unbatched,
                .deprecated_lookup_accounts_unbatched,
                => self.options.accounts_batch_size_min,
                .create_transfers,
                .lookup_transfers,
                .deprecated_create_transfers_unbatched,
                .deprecated_lookup_transfers_unbatched,
                => self.options.transfers_batch_size_min,
                .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                .deprecated_get_account_transfers_unbatched,
                .deprecated_get_account_balances_unbatched,
                .deprecated_query_accounts_unbatched,
                .deprecated_query_transfers_unbatched,
                .get_change_events,
                => 1,
            };
            const batch_span = switch (action) {
                .create_accounts,
                .lookup_accounts,
                .deprecated_create_accounts_unbatched,
                .deprecated_lookup_accounts_unbatched,
                => self.options.accounts_batch_size_span,
                .create_transfers,
                .lookup_transfers,
                .deprecated_create_transfers_unbatched,
                .deprecated_lookup_transfers_unbatched,
                => self.options.transfers_batch_size_span,
                .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                .deprecated_get_account_transfers_unbatched,
                .deprecated_get_account_balances_unbatched,
                .deprecated_query_accounts_unbatched,
                .deprecated_query_transfers_unbatched,
                .get_change_events,
                => 0,
            };

            const slice = stdx.bytes_as_slice(.inexact, T, body);
            const batch_size = @min(
                batch_min + self.prng.int_inclusive(usize, batch_span),
                event_count_remain,
            );

            return slice[0..batch_size];
        }

        fn transfer_id_to_index(self: *const Workload, id: u128) usize {
            // -1 because id=0 is not valid, so index=0→id=1.
            return @as(usize, @intCast(self.options.transfer_id_permutation.decode(id))) - 1;
        }

        fn transfer_index_to_id(self: *const Workload, index: usize) u128 {
            // +1 so that index=0 is encoded as a valid id.
            return self.options.transfer_id_permutation.encode(index + 1);
        }

        /// To support `lookup_transfers`, the `TransferPlan` is deterministic based on:
        /// * `Workload.transfer_plan_seed`, and
        /// * the transfer `index`.
        fn transfer_index_to_plan(self: *const Workload, index: usize) TransferPlan {
            var prng = stdx.PRNG.from_seed(self.transfer_plan_seed ^ @as(u64, index));
            const method: TransferPlan.Method = blk: {
                if (prng.chance(self.options.create_transfer_pending_probability)) {
                    break :blk .pending;
                }
                if (prng.chance(self.options.create_transfer_post_probability)) {
                    break :blk .post_pending;
                }
                if (prng.chance(self.options.create_transfer_void_probability)) {
                    break :blk .void_pending;
                }
                break :blk .single_phase;
            };
            return .{
                .valid = !prng.chance(self.options.create_transfer_invalid_probability),
                .limit = prng.chance(self.options.create_transfer_limit_probability),
                .method = method,
            };
        }

        fn on_create_transfers(
            self: *Workload,
            client_index: usize,
            timestamp: u64,
            transfers: []const tb.Transfer,
            results_sparse: []const tb.CreateTransfersResult,
        ) void {
            self.auditor.on_create_transfers(client_index, timestamp, transfers, results_sparse);
            if (transfers.len == 0) return;

            const transfer_index_min = self.transfer_id_to_index(transfers[0].id);
            const transfer_index_max = self.transfer_id_to_index(transfers[transfers.len - 1].id);
            assert(transfer_index_min <= transfer_index_max);

            self.transfers_delivered_recently.add(.{
                .min = transfer_index_min,
                .max = transfer_index_max,
            }) catch unreachable;

            while (self.transfers_delivered_recently.peek()) |delivered| {
                if (self.transfers_delivered_past == delivered.min) {
                    self.transfers_delivered_past = delivered.max + 1;
                    _ = self.transfers_delivered_recently.remove();
                } else {
                    assert(self.transfers_delivered_past < delivered.min);
                    break;
                }
            }

            const CreateTransfersResultIterator = IteratorForCreateType(tb.CreateTransfersResult);
            var results_iterator: CreateTransfersResultIterator = .init(results_sparse);
            for (transfers, 0..) |*transfer, i| {
                const result: tb.CreateTransferResult = results_iterator.take(i) orelse .ok;
                if (transfer.flags.pending and result != .exists) {
                    self.transfers_pending_in_flight -= 1;
                }

                // Add some successfully completed transfers to be retried in the next request.
                if (result == .ok and !transfer.flags.linked and
                    self.transfers_retry_exists.items.len <
                        self.options.transfers_retry_exists_max and
                    self.prng.chance(self.options.create_transfer_retry_probability))
                {
                    var transfer_exists = transfer.*;
                    assert(transfer_exists.timestamp == 0);
                    assert(transfer_exists.user_data_128 != 0);

                    transfer_exists.user_data_128 = 0; // This will be replaced by the checksum.
                    self.transfers_retry_exists.appendAssumeCapacity(transfer_exists);
                }

                // Enqueue the `id`s of transient errors to be retried in the next request.
                if (result != .ok and result.transient() and
                    self.transfers_retry_failed.count() <
                        self.options.transfers_retry_failed_max)
                {
                    self.transfers_retry_failed.putAssumeCapacityNoClobber(
                        transfer.id,
                        {},
                    );
                }
            }
        }

        fn on_lookup_transfers(
            self: *Workload,
            client_index: usize,
            timestamp: u64,
            ids: []const u128,
            results: []const tb.Transfer,
        ) void {
            self.auditor.on_lookup_transfers(client_index, timestamp, ids, results);

            var transfers = accounting_auditor.IteratorForLookupType(tb.Transfer).init(results);
            for (ids) |transfer_id| {
                const transfer_index = self.transfer_id_to_index(transfer_id);
                const transfer_outcome = self.transfer_index_to_plan(transfer_index).outcome();
                const result = transfers.take(transfer_id);

                if (result) |transfer| validate_transfer_checksum(transfer);

                if (transfer_index >= self.transfers_sent) {
                    // This transfer hasn't been created yet.
                    assert(result == null);
                    continue;
                }

                switch (transfer_outcome) {
                    .success => {
                        if (transfer_index < self.transfers_delivered_past) {
                            // The transfer was delivered; it must exist.
                            assert(result != null);
                        } else {
                            var it = self.transfers_delivered_recently.iterator();
                            while (it.next()) |delivered| {
                                if (transfer_index >= delivered.min and
                                    transfer_index <= delivered.max)
                                {
                                    // The transfer was delivered recently; it must exist.
                                    assert(result != null);
                                    break;
                                }
                            } else {
                                // The `create_transfers` has not committed (it may be in-flight).
                                assert(result == null);
                            }
                        }
                    },
                    // An invalid transfer is never persisted.
                    .failure => assert(result == null),
                    // Due to races and timeouts, these transfer types may not succeed.
                    .unknown => {},
                }
            }
        }

        fn on_get_account_transfers(
            self: *Workload,
            comptime operation: Operation,
            timestamp: u64,
            body: []const tb.AccountFilter,
            results: []const tb.Transfer,
        ) void {
            _ = timestamp;
            comptime assert(operation == .get_account_transfers or
                operation == .deprecated_get_account_transfers_unbatched);
            assert(body.len == 1);

            const batch_result_max = operation.result_max(self.options.batch_size_limit);
            const account_filter = &body[0];
            assert(results.len <= account_filter.limit);
            assert(results.len <= batch_result_max);

            const account_state = self.auditor.get_account_state(
                account_filter.account_id,
            ) orelse {
                // Invalid account id.
                assert(results.len == 0);
                return;
            };

            const filter_valid = account_state.created and
                (account_filter.flags.credits or account_filter.flags.debits) and
                account_filter.limit > 0 and
                account_filter.timestamp_min <= account_filter.timestamp_max;
            if (!filter_valid) {
                // Invalid filter.
                assert(results.len == 0);
                return;
            }

            self.validate_account_filter_result_count(
                operation,
                account_state,
                account_filter,
                results.len,
            );

            var timestamp_previous: u64 = if (account_filter.flags.reversed)
                account_state.transfer_timestamp_max +| 1
            else
                account_state.transfer_timestamp_min -| 1;

            for (results) |*transfer| {
                if (account_filter.flags.reversed) {
                    assert(transfer.timestamp < timestamp_previous);
                } else {
                    assert(transfer.timestamp > timestamp_previous);
                }
                timestamp_previous = transfer.timestamp;

                assert(account_filter.timestamp_min == 0 or
                    transfer.timestamp >= account_filter.timestamp_min);
                assert(account_filter.timestamp_max == 0 or
                    transfer.timestamp <= account_filter.timestamp_max);

                validate_transfer_checksum(transfer);

                const transfer_index = self.transfer_id_to_index(transfer.id);
                assert(transfer_index < self.transfers_sent);

                const transfer_plan = self.transfer_index_to_plan(transfer_index);
                assert(transfer_plan.valid);
                assert(transfer_plan.outcome() != .failure);
                if (transfer.flags.pending) assert(transfer_plan.method == .pending);
                if (transfer.flags.post_pending_transfer) {
                    assert(transfer_plan.method == .post_pending);
                }
                if (transfer.flags.void_pending_transfer) {
                    assert(transfer_plan.method == .void_pending);
                }
                if (transfer_plan.method == .single_phase) assert(!transfer.flags.pending and
                    !transfer.flags.post_pending_transfer and
                    !transfer.flags.void_pending_transfer);

                assert(transfer.debit_account_id == account_filter.account_id or
                    transfer.credit_account_id == account_filter.account_id);
                assert(account_filter.flags.credits or account_filter.flags.debits);
                assert(account_filter.flags.credits or
                    transfer.debit_account_id == account_filter.account_id);
                assert(account_filter.flags.debits or
                    transfer.credit_account_id == account_filter.account_id);

                if (transfer_plan.limit) {
                    // The plan does not guarantee the "limit" flag for posting
                    // or voiding pending transfers.
                    const post_or_void_pending_transfer = transfer.flags.post_pending_transfer or
                        transfer.flags.void_pending_transfer;
                    assert(post_or_void_pending_transfer == (transfer.pending_id != 0));

                    const dr_account = self.auditor.get_account(transfer.debit_account_id).?;
                    const cr_account = self.auditor.get_account(transfer.credit_account_id).?;
                    assert(
                        post_or_void_pending_transfer or
                            dr_account.flags.debits_must_not_exceed_credits or
                            cr_account.flags.credits_must_not_exceed_debits,
                    );
                }
            }
        }

        fn on_get_account_balances(
            self: *Workload,
            comptime operation: Operation,
            timestamp: u64,
            body: []const tb.AccountFilter,
            results: []const tb.AccountBalance,
        ) void {
            _ = timestamp;
            comptime assert(operation == .get_account_balances or
                operation == .deprecated_get_account_balances_unbatched);
            assert(body.len == 1);

            const batch_result_max = operation.result_max(self.options.batch_size_limit);
            const account_filter = &body[0];
            assert(results.len <= account_filter.limit);
            assert(results.len <= batch_result_max);

            const account_state = self.auditor.get_account_state(
                account_filter.account_id,
            ) orelse {
                // Invalid account id.
                assert(results.len == 0);
                return;
            };

            const filter_valid = account_state.created and
                self.auditor.get_account(account_filter.account_id).?.flags.history and
                (account_filter.flags.credits or account_filter.flags.debits) and
                account_filter.limit > 0 and
                account_filter.timestamp_min <= account_filter.timestamp_max;
            if (!filter_valid) {
                // Invalid filter.
                assert(results.len == 0);
                return;
            }

            self.validate_account_filter_result_count(
                operation,
                account_state,
                account_filter,
                results.len,
            );

            var timestamp_last: u64 = if (account_filter.flags.reversed)
                account_state.transfer_timestamp_max +| 1
            else
                account_state.transfer_timestamp_min -| 1;

            for (results) |*balance| {
                assert(if (account_filter.flags.reversed)
                    balance.timestamp < timestamp_last
                else
                    balance.timestamp > timestamp_last);
                timestamp_last = balance.timestamp;

                assert(account_filter.timestamp_min == 0 or
                    balance.timestamp >= account_filter.timestamp_min);
                assert(account_filter.timestamp_max == 0 or
                    balance.timestamp <= account_filter.timestamp_max);
            }
        }

        fn validate_account_filter_result_count(
            self: *const Workload,
            comptime operation: Operation,
            account_state: *const Auditor.AccountState,
            account_filter: *const tb.AccountFilter,
            result_count: usize,
        ) void {
            comptime assert(operation == .get_account_transfers or
                operation == .get_account_balances or
                operation == .deprecated_get_account_transfers_unbatched or
                operation == .deprecated_get_account_balances_unbatched);
            maybe(account_filter.limit == 0);

            const batch_result_max = operation.result_max(self.options.batch_size_limit);
            const transfer_count = account_state.transfers_count(account_filter.flags);
            if (account_filter.timestamp_min == 0 and account_filter.timestamp_max == 0) {
                assert(account_filter.limit <= batch_result_max or
                    account_filter.limit == std.math.maxInt(u32));
                assert(result_count ==
                    @min(account_filter.limit, batch_result_max, transfer_count));
            } else {
                // If timestamp range is set, then the limit is exactly the number of transfer
                // at the time the filter was generated, but new transfers could have been
                // inserted since then.
                assert(account_filter.limit <= transfer_count);
                assert(account_filter.timestamp_max >= account_filter.timestamp_min);
                if (account_filter.flags.reversed) {
                    // This filter is only set if there is at least one transfer, so the first
                    // transfer timestamp never changes.
                    assert(account_filter.timestamp_min == account_state.transfer_timestamp_min);
                    // The filter `timestamp_max` was decremented to skip one result.
                    assert(account_filter.timestamp_max < account_state.transfer_timestamp_max);
                } else {
                    // The filter `timestamp_min` was incremented to skip one result.
                    assert(account_filter.timestamp_min > account_state.transfer_timestamp_min);
                    // New transfers can update `transfer_timestamp_max`.
                    assert(account_filter.timestamp_max <= account_state.transfer_timestamp_max);
                }

                // Either `transfer_count` is greater than the batch size (so removing a result
                // doesn't make a difference) or there is exactly one less result that was
                // excluded by the timestamp filter.
                assert((result_count == batch_result_max and transfer_count > batch_result_max) or
                    result_count == account_filter.limit - 1);
            }
        }

        fn on_query(
            self: *Workload,
            comptime operation: Operation,
            timestamp: u64,
            body: []const tb.QueryFilter,
            results: []const operation.ResultType(),
        ) void {
            _ = timestamp;
            comptime assert(operation == .query_accounts or
                operation == .query_transfers or
                operation == .deprecated_query_accounts_unbatched or
                operation == .deprecated_query_transfers_unbatched);
            assert(body.len == 1);

            const batch_result_max: u32 = operation.result_max(self.options.batch_size_limit);
            const filter = &body[0];

            if (filter.ledger != 0) {
                // No results expected.
                assert(results.len == 0);
                return;
            }

            assert(filter.user_data_64 != 0);
            assert(filter.user_data_32 != 0);
            assert(filter.code != 0);
            assert(filter.user_data_128 == 0);
            assert(filter.ledger == 0);
            maybe(filter.limit == 0);
            maybe(filter.timestamp_min == 0);
            maybe(filter.timestamp_max == 0);

            const query_intersection_index = filter.code - 1;
            const query_intersection = self.auditor.query_intersections[query_intersection_index];
            const state = switch (operation) {
                .query_accounts,
                .deprecated_query_accounts_unbatched,
                => &query_intersection.accounts,
                .query_transfers,
                .deprecated_query_transfers_unbatched,
                => &query_intersection.transfers,
                else => unreachable,
            };

            assert(results.len <= filter.limit);
            assert(results.len <= batch_result_max);

            if (filter.timestamp_min > 0 or filter.timestamp_max > 0) {
                assert(filter.limit <= state.count);
                assert(filter.timestamp_min > 0);
                assert(filter.timestamp_max > 0);
                assert(filter.timestamp_min <= filter.timestamp_max);

                // Filtering by timestamp always exclude one single result.
                assert(results.len == filter.limit - 1);
            } else {
                assert(results.len == @min(
                    filter.limit,
                    batch_result_max,
                    state.count,
                ));
            }

            var timestamp_previous: u64 = if (filter.flags.reversed)
                std.math.maxInt(u64)
            else
                0;

            for (results) |*result| {
                if (filter.flags.reversed) {
                    assert(result.timestamp < timestamp_previous);
                } else {
                    assert(result.timestamp > timestamp_previous);
                }
                timestamp_previous = result.timestamp;

                if (filter.timestamp_min > 0) {
                    assert(result.timestamp >= filter.timestamp_min);
                }
                if (filter.timestamp_max > 0) {
                    assert(result.timestamp <= filter.timestamp_max);
                }

                assert(result.user_data_64 == filter.user_data_64);
                assert(result.user_data_32 == filter.user_data_32);
                assert(result.code == filter.code);

                if (operation == .query_transfers or
                    operation == .deprecated_query_transfers_unbatched)
                {
                    validate_transfer_checksum(result);
                }
            }
        }

        fn on_get_change_events(
            self: *Workload,
            timestamp: u64,
            body: []const tb.ChangeEventsFilter,
            results: []const tb.ChangeEvent,
        ) void {
            assert(body.len == 1);
            self.auditor.on_get_change_events(timestamp, body[0], results);

            for (results) |*result| {
                assert(stdx.zeroed(&result.reserved));
                switch (result.type) {
                    .single_phase => {
                        assert(result.timestamp == result.transfer_timestamp);
                        assert(!result.transfer_flags.pending);
                        assert(!result.transfer_flags.post_pending_transfer);
                        assert(!result.transfer_flags.void_pending_transfer);
                        assert(result.transfer_pending_id == 0);
                        assert(result.transfer_amount <= result.debit_account_debits_posted);
                        assert(result.transfer_amount <= result.credit_account_credits_posted);
                    },
                    .two_phase_pending => {
                        assert(result.timestamp == result.transfer_timestamp);
                        assert(result.transfer_flags.pending);
                        assert(!result.transfer_flags.post_pending_transfer);
                        assert(!result.transfer_flags.void_pending_transfer);
                        assert(result.transfer_pending_id == 0);
                        assert(result.transfer_amount <= result.debit_account_debits_pending);
                        assert(result.transfer_amount <= result.credit_account_credits_pending);
                    },
                    .two_phase_posted => {
                        assert(result.timestamp == result.transfer_timestamp);
                        assert(result.transfer_flags.post_pending_transfer);
                        assert(!result.transfer_flags.pending);
                        assert(!result.transfer_flags.void_pending_transfer);
                        assert(result.transfer_pending_id != 0);
                        assert(result.transfer_amount <= result.debit_account_debits_posted);
                        assert(result.transfer_amount <= result.credit_account_credits_posted);
                    },
                    .two_phase_voided => {
                        assert(result.timestamp == result.transfer_timestamp);
                        assert(result.transfer_flags.void_pending_transfer);
                        assert(!result.transfer_flags.pending);
                        assert(!result.transfer_flags.post_pending_transfer);
                        assert(result.transfer_pending_id != 0);
                    },
                    .two_phase_expired => {
                        assert(result.transfer_timeout > 0);
                        const timeout_ns: u64 =
                            @as(u64, result.transfer_timeout) * std.time.ns_per_s;
                        assert(result.timestamp >= result.transfer_timestamp + timeout_ns);
                        assert(result.transfer_flags.pending);
                        assert(!result.transfer_flags.post_pending_transfer);
                        assert(!result.transfer_flags.void_pending_transfer);
                        assert(result.transfer_pending_id == 0);
                    },
                }
                assert(result.transfer_flags.closing_debit == result.debit_account_flags.closed);
                assert(result.transfer_flags.closing_credit == result.credit_account_flags.closed);
                validate_get_event_checksum(result);
            }
        }

        /// Verify the transfer's integrity.
        fn validate_transfer_checksum(transfer: *const tb.Transfer) void {
            const checksum_actual = transfer.user_data_128;
            var check = transfer.*;
            check.user_data_128 = 0;
            check.timestamp = 0;
            const checksum_expect = vsr.checksum(std.mem.asBytes(&check));
            assert(checksum_expect == checksum_actual);
        }

        fn validate_get_event_checksum(event: *const tb.ChangeEvent) void {
            const transfer: tb.Transfer = .{
                .id = event.transfer_id,
                .debit_account_id = event.debit_account_id,
                .credit_account_id = event.credit_account_id,
                .amount = event.transfer_amount,
                .pending_id = event.transfer_pending_id,
                .user_data_128 = event.transfer_user_data_128,
                .user_data_64 = event.transfer_user_data_64,
                .user_data_32 = event.transfer_user_data_32,
                .timeout = event.transfer_timeout,
                .ledger = event.ledger,
                .code = event.transfer_code,
                .flags = event.transfer_flags,
                .timestamp = event.timestamp,
            };
            validate_transfer_checksum(&transfer);
        }
    };
}

fn OptionsType(
    comptime AccountingStateMachine: type,
    comptime Action: type,
    comptime Lookup: type,
) type {
    return struct {
        batch_size_limit: u32,
        multi_batch_per_request_limit: u32,

        auditor_options: Auditor.Options,
        transfer_id_permutation: IdPermutation,

        operations: stdx.PRNG.EnumWeightsType(Action),

        create_account_invalid_probability: Ratio,
        create_transfer_invalid_probability: Ratio,
        create_transfer_limit_probability: Ratio,
        create_transfer_pending_probability: Ratio,
        create_transfer_post_probability: Ratio,
        create_transfer_void_probability: Ratio,
        create_transfer_retry_probability: Ratio,
        lookup_account_invalid_probability: Ratio,

        account_filter_invalid_account_probability: Ratio,
        account_filter_timestamp_range_probability: Ratio,

        query_filter_not_found_probability: Ratio,
        query_filter_timestamp_range_probability: Ratio,
        lookup_transfer: stdx.PRNG.EnumWeightsType(Lookup),

        // Size of timespan for querying, measured in transfers
        lookup_transfer_span_mean: usize,

        account_limit_probability: Ratio,
        account_history_probability: Ratio,

        /// This probability is only checked for consecutive guaranteed-successful transfers.
        linked_valid_probability: Ratio,
        /// This probability is only checked for consecutive invalid transfers.
        linked_invalid_probability: Ratio,

        pending_timeout_mean: u32,

        accounts_batch_size_min: usize,
        accounts_batch_size_span: usize, // inclusive
        transfers_batch_size_min: usize,
        transfers_batch_size_span: usize, // inclusive

        /// Maximum number of failed transfer IDs to keep in the retry list.
        transfers_retry_failed_max: usize,

        /// Maximum number of successfully completed transfers to keep in the retry list.
        transfers_retry_exists_max: usize,

        const Options = @This();
        const Operation = AccountingStateMachine.Operation;

        pub fn generate(prng: *stdx.PRNG, options: struct {
            batch_size_limit: u32,
            multi_batch_per_request_limit: u32,
            client_count: usize,
            in_flight_max: usize,
        }) Options {
            assert(
                options.batch_size_limit <= constants.message_body_size_max,
            );

            const batch_create_accounts_limit = @min(
                Operation.create_accounts.event_max(options.batch_size_limit),
                Operation.deprecated_create_accounts_unbatched.event_max(options.batch_size_limit),
            );
            assert(batch_create_accounts_limit > 0);
            assert(batch_create_accounts_limit <=
                AccountingStateMachine.batch_max.create_accounts);

            const batch_create_transfers_limit = @min(
                Operation.create_transfers.event_max(options.batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(
                    options.batch_size_limit,
                ),
            );
            assert(batch_create_transfers_limit > 0);
            assert(batch_create_transfers_limit <=
                AccountingStateMachine.batch_max.create_transfers);
            return .{
                .batch_size_limit = options.batch_size_limit,
                .multi_batch_per_request_limit = options.multi_batch_per_request_limit,
                .auditor_options = .{
                    .accounts_max = prng.range_inclusive(usize, 2, 128),
                    .account_id_permutation = IdPermutation.generate(prng),
                    .client_count = options.client_count,
                    .transfers_pending_max = 256,
                    .changes_events_max = Operation
                        .get_change_events.event_max(options.batch_size_limit),
                    .in_flight_max = options.in_flight_max,
                    .pulse_expiries_max = @max(
                        Operation.create_transfers.event_max(options.batch_size_limit),
                        Operation.deprecated_create_transfers_unbatched.event_max(
                            options.batch_size_limit,
                        ),
                    ),
                },
                .transfer_id_permutation = IdPermutation.generate(prng),
                .operations = .{
                    .create_accounts = prng.range_inclusive(u64, 1, 10),
                    .create_transfers = prng.range_inclusive(u64, 1, 100),
                    .lookup_accounts = prng.range_inclusive(u64, 1, 20),
                    .lookup_transfers = prng.range_inclusive(u64, 1, 20),
                    .get_account_transfers = prng.range_inclusive(u64, 1, 20),
                    .get_account_balances = prng.range_inclusive(u64, 1, 20),
                    .query_accounts = prng.range_inclusive(u64, 1, 20),
                    .query_transfers = prng.range_inclusive(u64, 1, 20),
                    .get_change_events = prng.range_inclusive(u64, 1, 20),

                    .deprecated_create_accounts_unbatched = prng.range_inclusive(u64, 1, 10),
                    .deprecated_create_transfers_unbatched = prng.range_inclusive(u64, 1, 100),
                    .deprecated_lookup_accounts_unbatched = prng.range_inclusive(u64, 1, 20),
                    .deprecated_lookup_transfers_unbatched = prng.range_inclusive(u64, 1, 20),
                    .deprecated_get_account_transfers_unbatched = prng.range_inclusive(u64, 1, 20),
                    .deprecated_get_account_balances_unbatched = prng.range_inclusive(u64, 1, 20),
                    .deprecated_query_accounts_unbatched = prng.range_inclusive(u64, 1, 20),
                    .deprecated_query_transfers_unbatched = prng.range_inclusive(u64, 1, 20),
                },
                .create_account_invalid_probability = ratio(1, 100),
                .create_transfer_invalid_probability = ratio(1, 100),
                .create_transfer_limit_probability = ratio(prng.int_inclusive(u8, 100), 100),
                .create_transfer_pending_probability = ratio(prng.range_inclusive(u8, 1, 100), 100),
                .create_transfer_post_probability = ratio(prng.range_inclusive(u8, 1, 50), 100),
                .create_transfer_void_probability = ratio(prng.range_inclusive(u8, 1, 50), 100),
                .create_transfer_retry_probability = ratio(prng.range_inclusive(u8, 1, 10), 100),
                .lookup_account_invalid_probability = ratio(1, 100),

                .account_filter_invalid_account_probability = ratio(
                    prng.range_inclusive(u8, 1, 20),
                    100,
                ),
                .account_filter_timestamp_range_probability = ratio(
                    prng.range_inclusive(u8, 1, 80),
                    100,
                ),

                .query_filter_not_found_probability = ratio(prng.range_inclusive(u8, 1, 20), 100),
                .query_filter_timestamp_range_probability = ratio(
                    prng.range_inclusive(u8, 1, 80),
                    100,
                ),

                .lookup_transfer = .{
                    .delivered = prng.range_inclusive(u64, 1, 10),
                    .sending = prng.range_inclusive(u64, 1, 10),
                },
                .lookup_transfer_span_mean = prng.range_inclusive(usize, 10, 1000),
                .account_limit_probability = ratio(prng.int_inclusive(u8, 80), 100),
                .account_history_probability = ratio(prng.int_inclusive(u8, 80), 100),
                .linked_valid_probability = ratio(prng.int_inclusive(u8, 100), 100),
                // 100% chance: this only applies to consecutive invalid transfers, which are rare.
                .linked_invalid_probability = ratio(100, 100),
                // One second.
                .pending_timeout_mean = 1,
                .accounts_batch_size_min = 0,
                .accounts_batch_size_span = prng.range_inclusive(
                    usize,
                    1,
                    batch_create_accounts_limit,
                ),
                .transfers_batch_size_min = 0,
                .transfers_batch_size_span = prng.range_inclusive(
                    usize,
                    1,
                    batch_create_transfers_limit,
                ),
                .transfers_retry_failed_max = 128,
                .transfers_retry_exists_max = 128,
            };
        }
    };
}
