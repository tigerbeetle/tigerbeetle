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
const log = std.log.scoped(.test_workload);

const config = @import("../../config.zig");
const tb = @import("../../tigerbeetle.zig");
const vsr = @import("../../vsr.zig");
const accounting_auditor = @import("./auditor.zig");
const Auditor = accounting_auditor.AccountingAuditor;

const accounts_batch_size_max = @divFloor(config.message_size_max - @sizeOf(vsr.Header), @sizeOf(tb.Account));
const transfers_batch_size_max = @divFloor(config.message_size_max - @sizeOf(vsr.Header), @sizeOf(tb.Transfer));

// TODO Test linked create_accounts.
// TODO Weight the first few messages toward `create_accounts` so that early `create_transfers`
//      don't just fail.
// TODO Test lookup of "imminent" transfers (sent but not yet delivered).

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

/// Indexes: [valid:bool][limit:bool][method]
const transfer_templates = table: {
    const SNGL = @enumToInt(TransferPlan.Method.single_phase);
    const PEND = @enumToInt(TransferPlan.Method.pending);
    const POST = @enumToInt(TransferPlan.Method.post_pending);
    const VOID = @enumToInt(TransferPlan.Method.void_pending);
    const Result = accounting_auditor.CreateTransferResultSet;
    const result = Result.init;

    const two_phase_ok = .{
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
        fn template(ledger: u32, result: Result) TransferTemplate {
            return .{
                .ledger = ledger,
                .result = result,
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

///// OnePhaseValid creates "imminent" transfers: the workload knows they will succeed
///// eventually, but until they do they must be tracked so that `lookup_transfers` knows
///// not to expect them yet.
//const ImminentTransferRange = struct {
//    index: usize,
//    count: usize,
//};

pub fn AccountingWorkloadType(comptime AccountingStateMachine: type) type {
    const Operation = AccountingStateMachine.Operation;

    const Action = enum(u8) {
        create_accounts = @enumToInt(Operation.create_accounts),
        create_transfers = @enumToInt(Operation.create_transfers),
        lookup_accounts = @enumToInt(Operation.lookup_accounts),
        lookup_transfers = @enumToInt(Operation.lookup_transfers),
    };

    return struct {
        const Self = @This();

        pub const Options = struct {
            auditor_options: Auditor.Options,
            transfer_id_permutation: accounting_auditor.IdPermutation,

            operations: std.enums.EnumFieldStruct(Action, usize, null),

            create_account_invalid_probability: u8, // ≤ 100
            create_transfer_invalid_probability: u8, // ≤ 100
            create_transfer_limit_probability: u8, // ≤ 100
            create_transfer_pending_probability: u8, // ≤ 100
            create_transfer_post_probability: u8, // ≤ 100
            create_transfer_void_probability: u8, // ≤ 100
            lookup_account_invalid_probability: u8, // ≤ 100

            lookup_transfer: std.enums.EnumFieldStruct(enum {
                /// Query a transfer that has either been committed or rejected.
                delivered,
                /// Query a transfer whose `create_transfers` is in-flight.
                sending,
            }, usize, null),

            // size of timespan for querying, measured in transfers
            lookup_transfer_span_min: usize,
            lookup_transfer_span_mean: usize,

            account_limit_probability: u8, // ≤ 100

            /// This probability is only checked for consecutive guaranteed-successful transfers.
            linked_valid_probability: u8,
            /// This probability is only checked for consecutive invalid transfers.
            linked_invalid_probability: u8,

            pending_timeout_min: u64,
            pending_timeout_mean: u64,

            accounts_batch_size_min: usize,
            accounts_batch_size_span: usize, // inclusive
            transfers_batch_size_min: usize,
            transfers_batch_size_span: usize, // inclusive
        };

        random: std.rand.Random,
        auditor: Auditor,
        options: Options,

        /// The index of the next transfer to send.
        transfers_sent: usize = 0,

        /// Per client, the index of the highest transfer for which the client has received a reply
        /// to `create_accounts`.
        transfers_delivered: []usize,

        // TODO store index instead
        //transfers_imminent: std.AutoHashSetUnmanaged(u128),

        transfer_plan_seed: u64,

        pub fn init(allocator: std.mem.Allocator, random: std.rand.Random, options: Options) !Self {
            assert(options.create_account_invalid_probability <= 100);
            assert(options.create_transfer_invalid_probability <= 100);
            assert(options.create_transfer_limit_probability <= 100);
            assert(options.create_transfer_pending_probability <= 100);
            assert(options.create_transfer_post_probability <= 100);
            assert(options.create_transfer_void_probability <= 100);
            assert(options.lookup_account_invalid_probability <= 100);

            assert(options.lookup_transfer_span_min > 0);
            assert(options.lookup_transfer_span_min <= options.lookup_transfer_span_mean);
            assert(options.account_limit_probability <= 100);
            assert(options.linked_valid_probability <= 100);
            assert(options.linked_invalid_probability <= 100);
            assert(options.pending_timeout_min > 0);
            assert(options.pending_timeout_min <= options.pending_timeout_mean);

            assert(options.accounts_batch_size_span + options.accounts_batch_size_min <= accounts_batch_size_max);
            assert(options.accounts_batch_size_span >= 1);
            assert(options.transfers_batch_size_span + options.transfers_batch_size_min <= transfers_batch_size_max);
            assert(options.transfers_batch_size_span >= 1);

            var auditor = try Auditor.init(allocator, random, options.auditor_options);
            errdefer auditor.deinit(allocator);

            var transfers_delivered = try allocator.alloc(usize, options.auditor_options.client_count);
            errdefer allocator.free(transfers_delivered);
            std.mem.set(usize, transfers_delivered, 0);

            //var transfers_imminent = std.AutoHashSetUnmanaged(u128).init();
            //errdefer transfers_imminent

            for (auditor.accounts) |*account, i| {
                account.* = std.mem.zeroInit(tb.Account, .{
                    .id = auditor.account_index_to_id(i),
                    .ledger = 1,
                    .code = 123,
                });

                if (chance(random, options.account_limit_probability)) {
                    const b = random.boolean();
                    account.flags.debits_must_not_exceed_credits = b;
                    account.flags.credits_must_not_exceed_debits = !b;
                }
            }

            return Self{
                .random = random,
                .auditor = auditor,
                .options = options,
                .transfers_delivered = transfers_delivered,
                .transfer_plan_seed = random.int(u64),
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.auditor.deinit(allocator);
            allocator.free(self.transfers_delivered);
            //self.transfers_imminent.deinit(allocator);
        }

        /// A client may build multiple requests to queue up while another is in-flight.
        pub fn build_request(self: *Self, client_index: usize, body: []align(@alignOf(vsr.Header)) u8) struct {
            operation: Operation,
            size: usize,
        } {
            assert(client_index < self.auditor.options.client_count);
            assert(body.len == config.message_size_max - @sizeOf(vsr.Header));

            const action = switch (sample_distribution(self.random, self.options.operations)) {
                .create_accounts => Action.create_accounts,
                .create_transfers => Action.create_transfers,
                .lookup_accounts => Action.lookup_accounts,
                .lookup_transfers => Action.lookup_transfers,
            };

            const size = switch (action) {
                .create_accounts => @sizeOf(tb.Account) *
                    self.build_create_accounts(client_index, self.batch(tb.Account, action, body)),
                .create_transfers => @sizeOf(tb.Transfer) *
                    self.build_create_transfers(client_index, self.batch(tb.Transfer, action, body)),
                .lookup_accounts => @sizeOf(u128) *
                    self.build_lookup_accounts(self.batch(u128, action, body)),
                .lookup_transfers => @sizeOf(u128) *
                    self.build_lookup_transfers(self.batch(u128, action, body)),
            };
            assert(size <= body.len);

            return .{
                .operation = @intToEnum(Operation, @enumToInt(action)),
                .size = size,
            };
        }

        /// `on_reply` is called for replies in commit order.
        pub fn on_reply(
            self: *Self,
            client_index: usize,
            operation: vsr.Operation,
            timestamp: u64,
            request_body: []align(@alignOf(vsr.Header)) const u8,
            reply_body: []align(@alignOf(vsr.Header)) const u8,
        ) void {
            assert(timestamp != 0);
            assert(request_body.len <= config.message_size_max - @sizeOf(vsr.Header));
            assert(reply_body.len <= config.message_size_max - @sizeOf(vsr.Header));

            switch (operation.cast(AccountingStateMachine)) {
                .create_accounts => self.auditor.on_create_accounts(
                    client_index,
                    timestamp,
                    std.mem.bytesAsSlice(tb.Account, request_body),
                    std.mem.bytesAsSlice(tb.CreateAccountsResult, reply_body),
                ),
                .create_transfers => self.on_create_transfers(
                    client_index,
                    timestamp,
                    std.mem.bytesAsSlice(tb.Transfer, request_body),
                    std.mem.bytesAsSlice(tb.CreateTransfersResult, reply_body),
                ),
                .lookup_accounts => self.auditor.on_lookup_accounts(
                    client_index,
                    timestamp,
                    std.mem.bytesAsSlice(u128, request_body),
                    std.mem.bytesAsSlice(tb.Account, reply_body),
                ),
                .lookup_transfers => self.on_lookup_transfers(
                    client_index,
                    timestamp,
                    std.mem.bytesAsSlice(u128, request_body),
                    std.mem.bytesAsSlice(tb.Transfer, reply_body),
                ),
                else => unreachable,
            }
        }

        fn build_create_accounts(self: *Self, client_index: usize, accounts: []tb.Account) usize {
            const results = self.auditor.expect_create_accounts(client_index);
            for (accounts) |*account, i| {
                const account_index = self.random.uintLessThanBiased(usize, self.auditor.accounts.len);
                account.* = self.auditor.accounts[account_index];
                results[i] = accounting_auditor.CreateAccountResultSet{};

                if (chance(self.random, self.options.create_account_invalid_probability)) {
                    account.ledger = 0;
                    results[i].insert(.ledger_must_not_be_zero);
                } else {
                    if (!self.auditor.accounts_created[account_index]) {
                        results[i].insert(.ok);
                    }
                    // Even if the account doesn't exist yet, we may race another request.
                    results[i].insert(.exists);
                    results[i].insert(.exists_with_different_debits_pending);
                    results[i].insert(.exists_with_different_debits_posted);
                    results[i].insert(.exists_with_different_credits_pending);
                    results[i].insert(.exists_with_different_credits_posted);
                }
                assert(results[i].count() > 0);
            }
            return accounts.len;
        }

        fn build_create_transfers(self: *Self, client_index: usize, transfers: []tb.Transfer) usize {
            const results = self.auditor.expect_create_transfers(client_index);
            var transfers_count: usize = transfers.len;
            var i: usize = 0;
            while (i < transfers_count) {
                const transfer_index = self.transfers_sent;
                const transfer_plan = self.transfer_index_to_plan(transfer_index);
                const transfer_id = self.transfer_index_to_id(transfer_index);
                results[i] = self.build_transfer(transfer_id, transfer_plan, &transfers[i]) orelse {
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
                        chance(self.random, self.options.linked_valid_probability))
                    {
                        transfers[i - 1].flags.linked = true;
                    }

                    if (!results[i].contains(.ok) and !results[i - 1].contains(.ok) and
                        chance(self.random, self.options.linked_invalid_probability))
                    {
                        // Convert the previous transfer to a single-phase no-limit transfer, but
                        // link it to the current transfer — it will still fail.
                        _ = self.build_transfer(transfers[i - 1].id, .{
                            .valid = true,
                            .limit = false,
                            .method = .single_phase,
                        }, &transfers[i - 1]);
                        transfers[i - 1].flags.linked = true;
                        results[i - 1] = accounting_auditor.CreateTransferResultSet.init(.{
                            .linked_event_failed = true,
                        });
                    }
                }
                assert(results[i].count() > 0);

                transfers[i].user_data = vsr.checksum(std.mem.asBytes(&transfers[i]));
                i += 1;
                self.transfers_sent += 1;
            }
            assert(transfers_count == i);
            assert(transfers_count <= transfers.len);
            return transfers_count;
        }

        fn build_lookup_accounts(self: *Self, lookup_ids: []u128) usize {
            for (lookup_ids) |*id| {
                if (chance(self.random, self.options.lookup_account_invalid_probability)) {
                    // Pick an account with valid index (rather than "random.int(u128)") because the
                    // Auditor must decode the id to check for a matching account.
                    id.* = self.auditor.account_index_to_id(self.random.int(usize));
                } else {
                    id.* = self.auditor.accounts[self.random.uintLessThanBiased(usize, self.auditor.accounts.len)].id;
                }
            }
            return lookup_ids.len;
        }

        fn build_lookup_transfers(self: *const Self, lookup_ids: []u128) usize {
            const delivering_min = self.transfers_delivering_min();
            const lookup_window = sample_distribution(self.random, self.options.lookup_transfer);
            const lookup_window_start = switch (lookup_window) {
                // +1 to avoid an error when delivering_min=0.
                .delivered => self.random.uintLessThanBiased(usize, delivering_min + 1),
                // +1 to avoid an error when delivering_min=transfers_sent.
                .sending => self.random.intRangeLessThanBiased(
                    usize,
                    delivering_min,
                    self.transfers_sent + 1,
                ),
            };

            // +1 to make the span-max inclusive.
            const lookup_window_size = std.math.min(
                sample_exponential(
                    self.random,
                    self.options.lookup_transfer_span_min,
                    self.options.lookup_transfer_span_mean + 1,
                ),
                self.transfers_sent - lookup_window_start,
            );
            if (lookup_window_size == 0) return 0;

            for (lookup_ids) |*lookup_id| {
                lookup_id.* = self.transfer_index_to_id(
                    lookup_window_start + self.random.uintLessThanBiased(usize, lookup_window_size),
                );
            }
            return lookup_ids.len;
        }

        /// The transfer built is guaranteed to match the TransferPlan's outcome.
        /// The transfer built is _not_ guaranteed to match the TransferPlan's method.
        ///
        /// Returns `null` if the transfer plan cannot be fulfilled (because there aren't enough
        /// accounts created).
        fn build_transfer(
            self: *const Self,
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
                    self.auditor.pending_expiries.count() ==
                    self.auditor.options.transfers_pending_max)
                {
                    break :method .single_phase;
                }

                if (default == .post_pending or default == .void_pending) {
                    if (self.auditor.pending_amounts.count() == 0) {
                        break :method .single_phase;
                    }
                }
                break :method default;
            };

            const transfer_template = &transfer_templates
                [@boolToInt(transfer_plan.valid)]
                [@boolToInt(transfer_plan.limit)]
                [@enumToInt(method)];

            const limit_debits = transfer_plan.limit and self.random.boolean();
            const limit_credits = transfer_plan.limit and (self.random.boolean() or !limit_debits);

            const debit_account = self.auditor.pick_account(.{
                .created = true,
                .debits_must_not_exceed_credits = limit_debits,
                .credits_must_not_exceed_debits = null,
            }) orelse return null;

            const credit_account = self.auditor.pick_account(.{
                .created = true,
                .debits_must_not_exceed_credits = null,
                .credits_must_not_exceed_debits = limit_credits,
                .exclude = debit_account.id,
            }) orelse return null;

            transfer.* = .{
                .id = transfer_id,
                .debit_account_id = debit_account.id,
                .credit_account_id = credit_account.id,
                // "user_data" will be set to a checksum of the Transfer.
                .user_data = 0,
                .reserved = 0,
                .pending_id = 0,
                .timeout = 0,
                .ledger = transfer_template.ledger,
                .code = 123,
                .flags = .{},
                // +1 to avoid `.amount_must_not_be_zero`.
                .amount = 1 + self.random.int(u8),
            };

            switch (method) {
                .single_phase => {},
                .pending => {
                    transfer.flags = .{ .pending = true };
                    transfer.timeout = sample_exponential(
                        self.random,
                        self.options.pending_timeout_min,
                        self.options.pending_timeout_mean,
                    );
                },
                .post_pending, .void_pending => {
                    // Don't depend on `HashMap.keyIterator()` being deterministic.
                    // Pick a random "target" key, then post/void the id it is nearest to.
                    const target = self.random.int(u128);
                    var previous: ?u128 = null;
                    var iterator = self.auditor.pending_amounts.keyIterator();
                    while (iterator.next()) |id| {
                        if (previous == null or
                            std.math.max(target, id.*) - std.math.min(target, id.*) <
                            std.math.max(target, previous.?) - std.math.min(target, previous.?))
                        {
                            previous = id.*;
                        }
                    }

                    // If there were no pending ids, the method would have been changed.
                    const pending_id = previous.?;
                    transfer.debit_account_id = 0;
                    transfer.credit_account_id = 0;
                    transfer.amount = 0;
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

        fn batch(self: *const Self, comptime T: type, action: Action, body: []align(@alignOf(vsr.Header)) u8) []T {
            const batch_min = switch (action) {
                .create_accounts, .lookup_accounts => self.options.accounts_batch_size_min,
                .create_transfers, .lookup_transfers => self.options.transfers_batch_size_min,
            };
            const batch_span = switch (action) {
                .create_accounts, .lookup_accounts => self.options.accounts_batch_size_span,
                .create_transfers, .lookup_transfers => self.options.transfers_batch_size_span,
            };

            // +1 because the span is inclusive.
            const batch_size = batch_min + self.random.uintLessThanBiased(usize, batch_span + 1);
            return std.mem.bytesAsSlice(T, body)[0..batch_size];
        }

        fn transfer_id_to_index(self: *const Self, id: u128) usize {
            // -1 because id=0 is not valid, so index=0→id=1.
            return @intCast(usize, self.options.transfer_id_permutation.decode(id)) - 1;
        }

        fn transfer_index_to_id(self: *const Self, index: usize) u128 {
            // +1 so that index=0 is encoded as a valid id.
            return self.options.transfer_id_permutation.encode(index + 1);
        }

        /// To support `lookup_transfers`, the `TransferPlan` is deterministic based on:
        /// * `Workload.transfer_plan_seed`, and
        /// * the transfer `index`.
        fn transfer_index_to_plan(self: *const Self, index: usize) TransferPlan {
            var prng = std.rand.DefaultPrng.init(self.transfer_plan_seed ^ @as(u64, index));
            const random = prng.random();
            const method: TransferPlan.Method = blk: {
                if (chance(random, self.options.create_transfer_pending_probability)) {
                    break :blk .pending;
                }
                if (chance(random, self.options.create_transfer_post_probability)) {
                    break :blk .post_pending;
                }
                if (chance(random, self.options.create_transfer_void_probability)) {
                    break :blk .void_pending;
                }
                break :blk .single_phase;
            };
            return .{
                .valid = !chance(random, self.options.create_transfer_invalid_probability),
                .limit = chance(random, self.options.create_transfer_limit_probability),
                .method = method,
            };
        }

        /// Returns the index of the lowest transfer that is in-flight, across all clients.
        /// (There may be transfers greater than this index that have been delivered by other clients.)
        fn transfers_delivering_min(self: *const Self) usize {
            var delivering_min: ?usize = null;
            for (self.transfers_delivered) |delivered_by_client| {
                if (delivering_min == null or delivering_min.? > delivered_by_client + 1) {
                    delivering_min = delivered_by_client + 1;
                }
            }
            return std.math.min(self.transfers_sent, delivering_min orelse 0);
        }

        fn on_create_transfers(
            self: *Self,
            client_index: usize,
            timestamp: u64,
            transfers: []const tb.Transfer,
            results_sparse: []const tb.CreateTransfersResult,
        ) void {
            self.auditor.on_create_transfers(client_index, timestamp, transfers, results_sparse);

            if (transfers.len > 0) {
                self.transfers_delivered[client_index] =
                    self.transfer_id_to_index(transfers[transfers.len - 1].id);
            }
        }

        fn on_lookup_transfers(
            self: *Self,
            client_index: usize,
            timestamp: u64,
            ids: []const u128,
            results: []const tb.Transfer,
        ) void {
            self.auditor.on_lookup_transfers(client_index, timestamp, ids, results);

            const transfers_delivering = self.transfers_delivering_min();
            var transfers = accounting_auditor.IteratorForLookup(tb.Transfer).init(results);
            for (ids) |transfer_id| {
                const transfer_index = self.transfer_id_to_index(transfer_id);
                const transfer_outcome = self.transfer_index_to_plan(transfer_index).outcome();
                const result = transfers.take(transfer_id);

                if (result) |transfer| {
                    // The transfer exists; verify its integrity.
                    const checksum_actual = transfer.user_data;
                    var check = transfer.*;
                    check.user_data = 0;
                    check.timestamp = 0;
                    const checksum_expect = vsr.checksum(std.mem.asBytes(&check));
                    assert(checksum_expect == checksum_actual);
                }

                if (transfer_index >= self.transfers_sent) {
                    // This transfer hasn't been created yet.
                    assert(result == null);
                    continue;
                }

                switch (transfer_outcome) {
                    .success => {
                        if (transfer_index < transfers_delivering) {
                            // The transfer was delivered; it must exist.
                            assert(result != null);
                        } else {
                            // TODO Track the ranges of `create_transfers` that are in-flight, so we know
                            // whether or not `transfer_index` must exist at this point.
                        }
                    },
                    // An invalid transfer is never persisted.
                    .failure => assert(result == null),
                    // Due to races and timeouts, these transfer types are not guaranteed to succeed.
                    .unknown => {},
                }
            }
        }
    };
}

fn sample_exponential(random: std.rand.Random, min: anytype, mean: @TypeOf(min)) @TypeOf(min) {
    return min + @floatToInt(@TypeOf(min), @intToFloat(f64, mean - min) * random.floatExp(f64));
}

/// Sample from a discrete distribution.
/// Use integers instead of floating-point numbers to avoid nondeterminism on different hardware.
fn sample_distribution(
    random: std.rand.Random,
    distribution: anytype,
) std.meta.FieldEnum(@TypeOf(distribution)) {
    const SampleSpace = std.meta.FieldEnum(@TypeOf(distribution));
    const Indexer = std.enums.EnumIndexer(SampleSpace);

    var sum = sum: {
        var sum: usize = 0;
        comptime var i: usize = 0;
        inline while (i < Indexer.count) : (i += 1) {
            const key = comptime @tagName(Indexer.keyForIndex(i));
            sum += @field(distribution, key);
        }
        break :sum sum;
    };

    var pick = random.uintLessThanBiased(usize, sum);
    comptime var i: usize = 0;
    inline while (i < Indexer.count) : (i += 1) {
        const event = comptime Indexer.keyForIndex(i);
        const weight = @field(distribution, @tagName(event));
        if (pick < weight) return event;
        pick -= weight;
    }

    @panic("sample_discrete: empty sample space");
}

/// Returns true, `p` percent of the time, else false.
fn chance(random: std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThanBiased(u8, 100) < p;
}
