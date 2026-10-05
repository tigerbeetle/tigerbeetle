const std = @import("std");
const assert = std.debug.assert;
const tb = @import("../tigerbeetle.zig");

const stdx = @import("stdx");

const timestamp_max = std.math.maxInt(u64) - 1;
const transfer_id_max = std.math.maxInt(u128);
const amount_max = std.math.maxInt(u128);
pub const account_max_id = std.math.maxInt(u128);

pub fn StateMachineReferenceType(comptime account_count_max: usize, comptime transfer_count_max: usize) type {
    return struct {
        const Self = @This();

        const PendingState = enum {
            none,
            pending,
            posted,
            voided,
            expired,
        };

        const TransferEntry = struct {
            transfer: tb.Transfer,
            pending_state: PendingState,
        };
        const ChangeEventKind = enum {
            single_phase,
            two_phase_posted,
            two_phase_voided,
            two_phase_pending,
            two_phase_expired,
        };

        const ChangeEvent = struct {
            timestamp: u64,
            kind: ChangeEventKind,
            transfer: tb.Transfer,
            debit_account: tb.Account,
            credit_account: tb.Account,
        };

        const Snapshot = struct {
            accounts: [account_count_max]tb.Account,
            accounts_count: usize,
            transfers: [transfer_count_max]TransferEntry,
            transfers_count: usize,
            retired_ids: [transfer_count_max]u128,
            retired_ids_count: usize,
            journal_count: usize, // Journal is append only.
        };

        // TODO: revisit data structures
        accounts: [account_count_max]tb.Account = undefined,
        accounts_count: usize = 0,
        transfers: [transfer_count_max]TransferEntry = undefined,
        transfers_count: usize = 0,
        retired_ids: [transfer_count_max]u128 = undefined,
        retired_ids_count: usize = 0,
        journal: [transfer_count_max * 2]ChangeEvent = undefined, // TODO: revise limit
        journal_count: usize = 0,

        now: u64 = 0,

        pub const TransferFilter = struct {
            account_id: ?u128 = null,
            debits: bool = true,
            credits: bool = true,
            user_data_128: u128 = 0,
            user_data_64: u64 = 0,
            user_data_32: u32 = 0,
            ledger: u32 = 0,
            code: u16 = 0,
            timestamp_min: u64 = 0,
            timestamp_max: u64 = 0,
            reversed: bool = false,
        };

        fn valid_range(minimum: u64, maximum: u64) bool {
            return minimum < timestamp_max and maximum < timestamp_max and
                (maximum == 0 or minimum <= maximum);
        }

        fn deadline(t: tb.Transfer) u128 {
            return @as(u128, t.timestamp) + @as(u128, t.timeout) * 1_000_000_000;
        }
        /// Process at most limit expirations, ordered by deadline then creation time.
        /// A pulse's last expiration event has the pulse timestamp.
        pub fn pulse(self: *Self, limit: usize) usize {
            return self.expire(limit);
        }

        fn expire(self: *Self, limit: usize) usize {
            var due: [transfer_count_max]usize = undefined;
            var count: usize = 0;
            for (self.transfers[0..self.transfers_count], 0..) |r, i| {
                if (r.pending_state != .pending) continue;
                if (r.transfer.timeout == 0) continue;
                if (deadline(r.transfer) > self.now) continue;

                due[count] = i;
                count += 1;
            }
            std.mem.sort(usize, due[0..count], self, struct {
                fn lessThan(model: *const Self, a: usize, b: usize) bool {
                    const transfer_a = model.transfers[a].transfer;
                    const transfer_b = model.transfers[b].transfer;
                    return if (deadline(transfer_a) == deadline(transfer_b))
                        transfer_a.timestamp < transfer_b.timestamp
                    else
                        deadline(transfer_a) < deadline(transfer_b);
                }
            }.lessThan);
            count = @min(count, limit);
            if (count == 0) return 0;
            assert(count <= self.journal.len - self.journal_count);
            assert(self.now >= count and self.now - count >= self.watermark());
            for (due[0..count], 0..) |index, i| {
                const t = self.transfers[index].transfer;
                const debit = &self.accounts[self.account_index(t.debit_account_id).?];
                const credit = &self.accounts[self.account_index(t.credit_account_id).?];
                release_pending(debit, credit, t);
                if (t.flags.closing_debit) debit.flags.closed = false;
                if (t.flags.closing_credit) credit.flags.closed = false;
                self.transfers[index].pending_state = .expired;
                self.record(t, self.now - count + i + 1, true);
            }
            return count;
        }

        // Accounts start with zero balances. Transfers and pending resolutions change
        // total debits and credits equally, and linked rollback restores both sides.
        // Tests that seed balances directly must preserve this equality too.
        fn invariants(self: *Self) void {
            var debits_total: u256 = 0;
            var credits_total: u256 = 0;

            for (self.accounts_used()) |account| {
                debits_total += @as(u256, account.debits_posted) + account.debits_pending;
                credits_total += @as(u256, account.credits_posted) + account.credits_pending;
            }
            assert(debits_total == credits_total);
        }

        pub fn query_transfers(
            self: *const Self,
            filter: TransferFilter,
            output: []tb.Transfer,
        ) []const tb.Transfer {
            if (!valid_range(filter.timestamp_min, filter.timestamp_max)) return output[0..0];
            var count: usize = 0;
            // Transfers are appended in timestamp order: imports must advance beyond
            // the journal watermark, and linked rollback restores both visible prefixes.
            for (0..self.transfers_count) |offset| {
                if (count == output.len) break;
                const index = if (filter.reversed) self.transfers_count - 1 - offset else offset;
                const t = self.transfers[index].transfer;
                if (filter.account_id) |id| {
                    const debit_matches = filter.debits and t.debit_account_id == id;
                    const credit_matches = filter.credits and t.credit_account_id == id;
                    if (!debit_matches and !credit_matches) continue;
                }
                if (t.timestamp < filter.timestamp_min) continue;
                if (filter.timestamp_max != 0 and t.timestamp > filter.timestamp_max) continue;
                var match = true;
                inline for (.{
                    "user_data_128", "user_data_64", "user_data_32", "ledger", "code",
                }) |field| {
                    if (@field(filter, field) != 0 and @field(filter, field) != @field(t, field))
                        match = false;
                }
                if (!match) continue;
                output[count] = t;
                count += 1;
            }
            return output[0..count];
        }

        fn snapshot(self: *const Self) Snapshot {
            var result: Snapshot = undefined;
            inline for (std.meta.fields(Snapshot)) |field| {
                @field(result, field.name) = @field(self, field.name);
            }
            return result;
        }

        fn restore(self: *Self, saved: *const Snapshot) void {
            inline for (std.meta.fields(Snapshot)) |field| {
                @field(self, field.name) = @field(saved, field.name);
            }
        }

        pub fn create_accounts(self: *Self, accounts: []const tb.Account, results: []tb.CreateAccountResult) !void {
            try self.execute(tb.Account, accounts, tb.CreateAccountResult, results);
        }

        pub fn create_transfers(self: *Self, transfers: []const tb.Transfer, results: []tb.CreateTransferResult) !void {
            try self.execute(tb.Transfer, transfers, tb.CreateTransferResult, results);
        }

        fn advance(self: *Self, elapsed: u64) void {
            assert(self.now + elapsed < timestamp_max);
            self.now += elapsed;
        }

        fn execute(
            self: *Self,
            comptime Event: type,
            events: []const Event,
            comptime Result: type,
            results: []Result,
        ) !void {
            defer self.invariants();
            assert(events.len == results.len);
            if (events.len == 0) return;
            if (self.now + events.len >= timestamp_max) return error.InvalidTime;
            // TODO: Double check this logic and pull out to here.
            // One submitted batch has one admission cutoff, even though event
            // validation timestamps advance. Each API call is a separate batch.
            const import_cutoff = self.now + @as(u64, @intCast(events.len)) - 1;

            const batch_imported = events[0].flags.imported;

            var event_index: usize = 0;
            while (event_index < events.len) {
                const chain_opened = event_index;
                var chain_closed = event_index + 1; //exclusive
                while (chain_closed < events.len and events[chain_closed - 1].flags.linked) : (chain_closed += 1) {}

                var chain_failed: ?usize = null;
                const events_chained = events[chain_opened..chain_closed];
                var results_chained = results[chain_opened..chain_closed];

                const checkpoint = self.snapshot();
                // How do we best test this, and how do we snapshot?
                for (events_chained, results_chained, 0..) |event, *result, chain_index| {
                    // Validate each event after advancing one tick from the batch start.
                    const timestamp = self.now + (chain_index + event_index) + 1;

                    const result_status: @FieldType(Result, "status") = result: {
                        if (chain_index + event_index == events.len - 1 and
                            event.flags.linked) break :result .linked_event_chain_open;
                        if (chain_failed) |_| break :result .linked_event_failed;
                        if (batch_imported and !event.flags.imported) break :result .imported_event_expected;
                        if (!batch_imported and event.flags.imported) break :result .imported_event_not_expected;

                        switch (Event) {
                            tb.Account => break :result self.create_account(event, timestamp, import_cutoff),
                            tb.Transfer => break :result self.create_transfer(event, timestamp, import_cutoff),
                            else => comptime unreachable,
                        }
                    };

                    result.status = result_status;
                    result.timestamp = switch (result_status) {
                        .created => if (event.flags.imported) event.timestamp else timestamp,
                        .exists => if (Event == tb.Account)
                            self.accounts[self.account_index(event.id).?].timestamp
                        else
                            self.transfers[self.transfer_index(event.id).?].transfer.timestamp,
                        else => timestamp,
                    };

                    if (result_status != .created and chain_failed == null) chain_failed = chain_index;
                }
                if (chain_failed) |chain_failure| {
                    self.restore(&checkpoint);
                    // Later events already have their failure status and validation
                    // timestamp. Earlier events retain their original result timestamp.
                    for (results_chained[0..chain_failure]) |*result| {
                        result.status = .linked_event_failed;
                    }

                    if (transient_failure(Result, results_chained[chain_failure])) {
                        self.retired_ids[self.retired_ids_count] = events_chained[chain_failure].id;
                        self.retired_ids_count += 1;
                    }
                }
                event_index = chain_closed;
            }
            // Advance the time.
            self.advance(events.len);
        }

        fn account_index(self: *const Self, id: u128) ?usize {
            for (self.accounts[0..self.accounts_count], 0..) |account, index| {
                if (account.id == id) return index;
            }
            return null;
        }

        fn transfer_index(self: *const Self, id: u128) ?usize {
            for (self.transfers[0..self.transfers_count], 0..) |entry, index| {
                if (entry.transfer.id == id) return index;
            }
            return null;
        }

        fn accounts_used(self: *Self) []tb.Account {
            return self.accounts[0..self.accounts_count];
        }
        fn transfers_used(self: *Self) []TransferEntry {
            return self.transfers[0..self.transfers_count];
        }

        fn time_valid(comptime Status: type, imported: bool, timestamp: u64, now: u64) ?Status {
            if (!imported)
                return if (timestamp == 0) null else .timestamp_must_be_zero;

            if (timestamp == 0 or timestamp >= timestamp_max)
                return .imported_event_timestamp_out_of_range;

            return if (timestamp > now)
                .imported_event_timestamp_must_not_advance
            else
                null;
        }

        fn create_account(self: *Self, event: tb.Account, timestamp: u64, import_cutoff: u64) tb.CreateAccountStatus {
            var account = event;

            // TODO: do we need the import cutoff or can we hoist it
            if (time_valid(
                tb.CreateAccountStatus,
                account.flags.imported,
                account.timestamp,
                import_cutoff,
            )) |status| {
                return status;
            }
            if (account.reserved != 0) return .reserved_field;
            if (account.flags.padding != 0) return .reserved_flag;
            if (account.id == 0) return .id_must_not_be_zero;
            if (account.id == account_max_id) return .id_must_not_be_int_max;
            // TODO: refactor a bit cleaner
            if (self.account_index(account.id)) |index| {
                const account_old = self.accounts[index];

                if (!std.meta.eql(account.flags, account_old.flags)) return .exists_with_different_flags;
                inline for (.{
                    "user_data_128", "user_data_64", "user_data_32", "ledger", "code",
                }) |field| {
                    if (@field(account, field) != @field(account_old, field))
                        return @field(tb.CreateAccountStatus, "exists_with_different_" ++ field);
                }
                return .exists;
            }
            if (account.flags.debits_must_not_exceed_credits and
                account.flags.credits_must_not_exceed_debits) return .flags_are_mutually_exclusive;
            inline for (.{ "debits_pending", "debits_posted", "credits_pending", "credits_posted" }) |field| {
                if (@field(account, field) != 0) return @field(tb.CreateAccountStatus, field ++ "_must_be_zero");
            }

            if (account.debits_pending != 0) return .debits_pending_must_be_zero;
            if (account.debits_posted != 0) return .debits_posted_must_be_zero;
            if (account.credits_pending != 0) return .credits_pending_must_be_zero;
            if (account.credits_posted != 0) return .credits_posted_must_be_zero;
            if (account.ledger == 0) return .ledger_must_not_be_zero;
            if (account.code == 0) return .code_must_not_be_zero;

            if (account.flags.imported) {
                for (self.accounts_used()) |account_existing| {
                    if (account.timestamp <= account_existing.timestamp) return .imported_event_timestamp_must_not_regress;
                }
                for (self.transfers_used()) |transfer_existing| {
                    if (account.timestamp == transfer_existing.transfer.timestamp) return .imported_event_timestamp_must_not_regress;
                }
            } else account.timestamp = timestamp;
            self.accounts[self.accounts_count] = account;
            self.accounts_count += 1;
            return .created;
        }

        fn resolution_amount(transfer: tb.Transfer, pending: tb.Transfer) u128 {
            assert(transfer.flags.post_pending_transfer or transfer.flags.void_pending_transfer);
            return if ((transfer.flags.post_pending_transfer and transfer.amount == amount_max) or
                (transfer.flags.void_pending_transfer and transfer.amount == 0))
                pending.amount
            else
                transfer.amount;
        }

        fn release_pending(debit: *tb.Account, credit: *tb.Account, pending: tb.Transfer) void {
            assert(pending.flags.pending);
            debit.debits_pending -= pending.amount;
            credit.credits_pending -= pending.amount;
        }

        fn compare_transfer(self: *const Self, input: tb.Transfer, old: tb.Transfer) tb.CreateTransferStatus {
            var t = input;
            if (!std.meta.eql(t.flags, old.flags)) return .exists_with_different_flags;
            if (t.pending_id != old.pending_id) return .exists_with_different_pending_id;
            if (t.timeout != old.timeout) return .exists_with_different_timeout;
            const resolve = t.flags.post_pending_transfer or t.flags.void_pending_transfer;
            if (resolve) {
                // TODO: review this part of the code. [pending]
                const pending_index = self.transfer_index(t.pending_id).?;
                const transfer_pending = self.transfers[pending_index].transfer;
                inline for (.{
                    "debit_account_id", "credit_account_id", "ledger",       "code",
                    "user_data_128",    "user_data_64",      "user_data_32",
                }) |field| {
                    if (@field(t, field) == 0) @field(t, field) = @field(transfer_pending, field);
                }
                t.amount = resolution_amount(t, transfer_pending);
            }
            if (t.debit_account_id != old.debit_account_id)
                return .exists_with_different_debit_account_id;
            if (t.credit_account_id != old.credit_account_id)
                return .exists_with_different_credit_account_id;
            const flexible_amount = t.flags.balancing_debit or t.flags.balancing_credit;
            if (if (flexible_amount) t.amount < old.amount else t.amount != old.amount)
                return .exists_with_different_amount;
            inline for (.{
                "user_data_128", "user_data_64", "user_data_32", "ledger", "code",
            }) |field| {
                if (@field(t, field) != @field(old, field))
                    return @field(tb.CreateTransferStatus, "exists_with_different_" ++ field);
            }
            return .exists;
        }

        fn create_transfer(self: *Self, event: tb.Transfer, now: u64, import_cutoff: u64) tb.CreateTransferStatus {
            var transfer = event;
            const flags = transfer.flags;
            const void_or_post = flags.post_pending_transfer or flags.void_pending_transfer;
            if (time_valid(
                tb.CreateTransferStatus,
                flags.imported,
                transfer.timestamp,
                import_cutoff,
            )) |status| return status;
            if (flags.padding != 0) return .reserved_flag;
            if (transfer.id == 0) return .id_must_not_be_zero;
            if (transfer.id == transfer_id_max) return .id_must_not_be_int_max;
            if (self.transfer_index(transfer.id)) |index| {
                return self.compare_transfer(transfer, self.transfers[index].transfer);
            }
            for (self.retired_ids[0..self.retired_ids_count]) |id_retired| {
                if (transfer.id == id_retired) return .id_already_failed;
            }

            if (flags.pending and void_or_post) return .flags_are_mutually_exclusive;
            if (flags.post_pending_transfer and flags.void_pending_transfer) return .flags_are_mutually_exclusive;
            if (void_or_post and (flags.balancing_credit or flags.balancing_debit or flags.closing_debit or flags.closing_credit)) return .flags_are_mutually_exclusive;
            if (void_or_post) {
                if (transfer.pending_id == 0) return .pending_id_must_not_be_zero;
                if (transfer.pending_id == transfer_id_max) return .pending_id_must_not_be_int_max;
                if (transfer.pending_id == transfer.id) return .pending_id_must_be_different;
            } else {
                if (transfer.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
                if (transfer.debit_account_id == transfer_id_max) return .debit_account_id_must_not_be_int_max;
                if (transfer.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
                if (transfer.credit_account_id == transfer_id_max) return .credit_account_id_must_not_be_int_max;
                if (transfer.debit_account_id == transfer.credit_account_id) return .accounts_must_be_different;
                if (transfer.pending_id != 0) return .pending_id_must_be_zero;
            }
            // void_or_post => always not pending
            if (!flags.pending and transfer.timeout != 0) return .timeout_reserved_for_pending_transfer;
            if (!flags.pending and (flags.closing_debit or flags.closing_credit)) return .closing_transfer_must_be_pending;

            var pending_index: ?usize = null;
            if (void_or_post) {
                pending_index = self.transfer_index(transfer.pending_id) orelse return .pending_transfer_not_found;
                const pending_transfer = self.transfers[pending_index.?].transfer;

                if (!pending_transfer.flags.pending) return .pending_transfer_not_pending;
                inline for (.{ "debit_account_id", "credit_account_id", "ledger", "code" }) |field| {
                    if (@field(transfer, field) != 0 and @field(transfer, field) != @field(pending_transfer, field))
                        return @field(tb.CreateTransferStatus, "pending_transfer_has_different_" ++ field);
                    @field(transfer, field) = @field(pending_transfer, field);
                }
                transfer.amount = resolution_amount(transfer, pending_transfer);
                if (transfer.amount > pending_transfer.amount) return .exceeds_pending_transfer_amount;
                if (flags.void_pending_transfer and transfer.amount != pending_transfer.amount) return .pending_transfer_has_different_amount;

                switch (self.transfers[pending_index.?].pending_state) {
                    .posted => return .pending_transfer_already_posted,
                    .voided => return .pending_transfer_already_voided,
                    .expired => return .pending_transfer_expired,
                    .pending => {},
                    .none => unreachable,
                }
                // Rejection leaves pending balances intact; pulse performs expiration.
                if (pending_transfer.timeout != 0 and deadline(pending_transfer) <= now)
                    return .pending_transfer_expired;
                inline for (.{ "user_data_128", "user_data_64", "user_data_32" }) |field| {
                    if (@field(transfer, field) == 0) @field(transfer, field) = @field(pending_transfer, field);
                }
            } else {
                if (transfer.ledger == 0) return .ledger_must_not_be_zero;
                if (transfer.code == 0) return .code_must_not_be_zero;
            }
            const dr_index = self.account_index(transfer.debit_account_id) orelse return .debit_account_not_found;
            const cr_index = self.account_index(transfer.credit_account_id) orelse return .credit_account_not_found;
            const dr = self.accounts[dr_index];
            const cr = self.accounts[cr_index];

            if (dr.ledger != cr.ledger) return .accounts_must_have_the_same_ledger;
            if (transfer.ledger != dr.ledger) return .transfer_must_have_the_same_ledger_as_accounts;
            if (flags.imported) {
                if (transfer.timestamp <= self.watermark())
                    return .imported_event_timestamp_must_not_regress;
                for (self.accounts_used()) |old| {
                    if (transfer.timestamp == old.timestamp)
                        return .imported_event_timestamp_must_not_regress;
                }
                if (transfer.timestamp <= dr.timestamp)
                    return .imported_event_timestamp_must_postdate_debit_account;
                if (transfer.timestamp <= cr.timestamp)
                    return .imported_event_timestamp_must_postdate_credit_account;
                if (transfer.timeout != 0) return .imported_event_timeout_must_be_zero;
            } else transfer.timestamp = now;
            if (!flags.void_pending_transfer) {
                if (dr.flags.closed) return .debit_account_already_closed;
                if (cr.flags.closed) return .credit_account_already_closed;
            }

            if (!void_or_post) {
                if (flags.balancing_debit) {
                    transfer.amount = @min(transfer.amount, dr.credits_posted -| (dr.debits_posted + dr.debits_pending));
                }
                if (flags.balancing_credit) {
                    transfer.amount = @min(transfer.amount, cr.debits_posted -| (cr.credits_posted + cr.credits_pending));
                }

                const amount: u256 = transfer.amount;

                if (flags.pending and amount + dr.debits_pending > amount_max)
                    return .overflows_debits_pending;
                if (flags.pending and amount + cr.credits_pending > amount_max)
                    return .overflows_credits_pending;
                if (amount + dr.debits_posted > amount_max)
                    return .overflows_debits_posted;
                if (amount + cr.credits_posted > amount_max)
                    return .overflows_credits_posted;
                if (amount + dr.debits_pending + dr.debits_posted > amount_max)
                    return .overflows_debits;
                if (amount + cr.credits_pending + cr.credits_posted > amount_max)
                    return .overflows_credits;

                if (deadline(transfer) >= timestamp_max) return .overflows_timeout;
                if (dr.flags.debits_must_not_exceed_credits and dr.debits_pending + dr.debits_posted + transfer.amount > dr.credits_posted)
                    return .exceeds_credits;
                if (cr.flags.credits_must_not_exceed_debits and cr.credits_pending + cr.credits_posted + transfer.amount > cr.debits_posted)
                    return .exceeds_debits;
            }

            // All validation is complete. Commit balances, pending state, and the new record.

            if (pending_index) |index| {
                const pending_transfer = self.transfers[index].transfer;
                const debit = &self.accounts[dr_index];
                const credit = &self.accounts[cr_index];
                release_pending(debit, credit, pending_transfer);
                self.transfers[index].pending_state = if (flags.post_pending_transfer) .posted else .voided;
                if (flags.void_pending_transfer) {
                    if (pending_transfer.flags.closing_debit) debit.flags.closed = false;
                    if (pending_transfer.flags.closing_credit) credit.flags.closed = false;
                }
            }

            if (flags.pending) {
                self.accounts[dr_index].debits_pending += transfer.amount;
                self.accounts[cr_index].credits_pending += transfer.amount;
            }

            if (!flags.pending and !flags.void_pending_transfer) {
                self.accounts[dr_index].debits_posted += transfer.amount;
                self.accounts[cr_index].credits_posted += transfer.amount;
            }
            if (flags.closing_debit) self.accounts[dr_index].flags.closed = true;
            if (flags.closing_credit) self.accounts[cr_index].flags.closed = true;

            // Queries scan storage directly in timestamp order.
            if (self.transfers_count > 0) {
                assert(transfer.timestamp > self.transfers[self.transfers_count - 1].transfer.timestamp);
            }
            self.transfers[self.transfers_count] = .{
                .transfer = transfer,
                .pending_state = if (flags.pending) .pending else .none,
            };
            self.transfers_count += 1;
            self.record(transfer, transfer.timestamp, false);
            return .created;
        }

        fn record(self: *Self, transfer: tb.Transfer, timestamp: u64, expired: bool) void {
            assert(timestamp > self.watermark());
            self.journal[self.journal_count] = .{
                .timestamp = timestamp,
                .kind = if (expired) .two_phase_expired else if (transfer.flags.pending)
                    .two_phase_pending
                else if (transfer.flags.post_pending_transfer)
                    .two_phase_posted
                else if (transfer.flags.void_pending_transfer)
                    .two_phase_voided
                else
                    .single_phase,
                .transfer = transfer,
                .debit_account = self.accounts[self.account_index(transfer.debit_account_id).?],
                .credit_account = self.accounts[self.account_index(transfer.credit_account_id).?],
            };
            self.journal_count += 1;
        }

        fn watermark(self: *const Self) u64 {
            return if (self.journal_count == 0) 0 else self.journal[self.journal_count - 1].timestamp;
        }

        fn transient_failure(comptime Result: type, result: Result) bool {
            if (comptime Result != tb.CreateTransferResult) return false;
            return switch (result.status) {
                .debit_account_not_found,
                .credit_account_not_found,
                .pending_transfer_not_found,
                .exceeds_credits,
                .exceeds_debits,
                .debit_account_already_closed,
                .credit_account_already_closed,
                => true,
                else => false,
            };
        }
    };
}

test "state_machine reference model conformance test CreateAccountStatus" {
    var create_account_status_map: std.EnumArray(tb.CreateAccountStatus, bool) = .initFill(false);
    // This legacy status is no longer returned; successful creation returns .created.
    create_account_status_map.set(.deprecated_ok, true);

    const linked: tb.AccountFlags = .{ .linked = true };
    const imported: tb.AccountFlags = .{ .imported = true };
    // Each triplet contains overrides for two valid accounts and their expected statuses.
    // The fresh model starts at time 100, so the two-account import cutoff is 101.
    inline for (.{
        .{ .{}, .{}, .{ .created, .created } },
        .{ .{ .flags = linked }, .{ .id = 0 }, .{ .linked_event_failed, .id_must_not_be_zero } },
        .{ .{ .flags = linked, .id = 0 }, .{}, .{ .id_must_not_be_zero, .linked_event_failed } },
        .{ .{ .flags = linked }, .{ .flags = linked }, .{ .linked_event_failed, .linked_event_chain_open } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{}, .{ .created, .imported_event_expected } },
        .{ .{}, .{ .flags = imported, .timestamp = 1 }, .{ .created, .imported_event_not_expected } },
        .{ .{}, .{ .timestamp = 1 }, .{ .created, .timestamp_must_be_zero } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{ .flags = imported, .timestamp = 0 }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{ .flags = imported, .timestamp = timestamp_max }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{ .flags = imported, .timestamp = timestamp_max + 1 }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{ .flags = imported, .timestamp = 102 }, .{ .created, .imported_event_timestamp_must_not_advance } },
        .{ .{}, .{ .reserved = 1 }, .{ .created, .reserved_field } },
        .{ .{}, .{ .flags = tb.AccountFlags{ .padding = 1 } }, .{ .created, .reserved_flag } },
        .{ .{}, .{ .id = 0 }, .{ .created, .id_must_not_be_zero } },
        .{ .{}, .{ .id = account_max_id }, .{ .created, .id_must_not_be_int_max } },
        .{ .{}, .{ .id = 1, .flags = tb.AccountFlags{ .history = true } }, .{ .created, .exists_with_different_flags } },
        .{ .{}, .{ .id = 1, .user_data_128 = 1 }, .{ .created, .exists_with_different_user_data_128 } },
        .{ .{}, .{ .id = 1, .user_data_64 = 1 }, .{ .created, .exists_with_different_user_data_64 } },
        .{ .{}, .{ .id = 1, .user_data_32 = 1 }, .{ .created, .exists_with_different_user_data_32 } },
        .{ .{}, .{ .id = 1, .ledger = 2 }, .{ .created, .exists_with_different_ledger } },
        .{ .{}, .{ .id = 1, .code = 2 }, .{ .created, .exists_with_different_code } },
        .{ .{}, .{ .id = 1 }, .{ .created, .exists } },
        .{ .{}, .{ .flags = tb.AccountFlags{ .debits_must_not_exceed_credits = true, .credits_must_not_exceed_debits = true } }, .{ .created, .flags_are_mutually_exclusive } },
        .{ .{}, .{ .debits_pending = 1 }, .{ .created, .debits_pending_must_be_zero } },
        .{ .{}, .{ .debits_posted = 1 }, .{ .created, .debits_posted_must_be_zero } },
        .{ .{}, .{ .credits_pending = 1 }, .{ .created, .credits_pending_must_be_zero } },
        .{ .{}, .{ .credits_posted = 1 }, .{ .created, .credits_posted_must_be_zero } },
        .{ .{}, .{ .ledger = 0 }, .{ .created, .ledger_must_not_be_zero } },
        .{ .{}, .{ .code = 0 }, .{ .created, .code_must_not_be_zero } },
        .{ .{ .flags = imported, .timestamp = 2 }, .{ .flags = imported, .timestamp = 1 }, .{ .created, .imported_event_timestamp_must_not_regress } },
        .{ .{ .flags = imported, .timestamp = 1 }, .{ .flags = imported, .timestamp = 1 }, .{ .created, .imported_event_timestamp_must_not_regress } },
        .{ .{ .flags = imported, .timestamp = 100 }, .{ .flags = imported, .timestamp = 101 }, .{ .created, .created } },
    }) |triplet| {
        var model = test_model();
        var accounts = [2]tb.Account{ test_account(1), test_account(2) };
        inline for (0..2) |index| {
            const overrides = triplet[index];
            inline for (std.meta.fields(@TypeOf(overrides))) |field| {
                @field(accounts[index], field.name) = @field(overrides, field.name);
            }
        }
        var results: [2]tb.CreateAccountResult = undefined;
        try model.create_accounts(&accounts, &results);
        inline for (triplet[2], 0..) |expected, index| {
            try expectEqual(@as(tb.CreateAccountStatus, expected), results[index].status);
            create_account_status_map.set(results[index].status, true);
        }
    }
    for (std.enums.values(tb.CreateAccountStatus)) |status| {
        if (!create_account_status_map.get(status)) {
            std.debug.print("CreateAccountStatus not covered: {s}\n", .{@tagName(status)});
            return error.TestExpectedEqual;
        }
    }
}

test "state_machine reference model conformance test CreateTransferStatus" {
    @setEvalBranchQuota(10_000);
    var create_transfer_status_map: std.enums.EnumFieldStruct(tb.CreateTransferStatus, bool, false) = .{};
    // Neither legacy success nor the removed amount-must-not-be-zero error is returned.
    create_transfer_status_map.deprecated_ok = true;
    create_transfer_status_map.deprecated_18 = true;

    const linked: tb.TransferFlags = .{ .linked = true };
    const imported: tb.TransferFlags = .{ .imported = true };
    const pending: tb.TransferFlags = .{ .pending = true };
    const post: tb.TransferFlags = .{ .post_pending_transfer = true };
    const void_pending: tb.TransferFlags = .{ .void_pending_transfer = true };

    const check = struct {
        fn pair(
            model: *TestModel,
            coverage: anytype,
            transfers: [2]tb.Transfer,
            expected: [2]tb.CreateTransferStatus,
        ) !void {
            var results: [2]tb.CreateTransferResult = undefined;
            try model.create_transfers(&transfers, &results);
            for (results, expected) |result, status_expected| {
                try expectEqual(status_expected, result.status);
                inline for (std.enums.values(tb.CreateTransferStatus)) |status| {
                    if (result.status == status) @field(coverage, @tagName(status)) = true;
                }
            }
        }
    }.pair;

    var base = test_model();
    var accounts = [2]tb.Account{ test_account(1), test_account(2) };
    for (&accounts, 0..) |*account, index| {
        account.flags.imported = true;
        account.timestamp = (index + 1) * 10;
    }
    var account_results: [2]tb.CreateAccountResult = undefined;
    try base.create_accounts(&accounts, &account_results);
    for (account_results) |result| try expectEqual(.created, result.status);

    // Each triplet contains overrides for two valid transfers and their expected statuses.
    // Imported accounts have timestamps 10 and 20; the normal batch's import cutoff is 103.
    inline for (.{
        .{ .{}, .{}, .{ .created, .created } },
        .{ .{ .amount = 0 }, .{ .amount = 0 }, .{ .created, .created } },
        .{ .{ .flags = linked }, .{ .id = 0 }, .{ .linked_event_failed, .id_must_not_be_zero } },
        .{ .{ .flags = linked, .id = 0 }, .{}, .{ .id_must_not_be_zero, .linked_event_failed } },
        .{ .{ .flags = linked }, .{ .flags = linked }, .{ .linked_event_failed, .linked_event_chain_open } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{}, .{ .created, .imported_event_expected } },
        .{ .{}, .{ .flags = imported, .timestamp = 90 }, .{ .created, .imported_event_not_expected } },
        .{ .{}, .{ .timestamp = 1 }, .{ .created, .timestamp_must_be_zero } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported, .timestamp = timestamp_max }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported, .timestamp = timestamp_max + 1 }, .{ .created, .imported_event_timestamp_out_of_range } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported, .timestamp = 104 }, .{ .created, .imported_event_timestamp_must_not_advance } },
        .{ .{}, .{ .flags = tb.TransferFlags{ .padding = 1 } }, .{ .created, .reserved_flag } },
        .{ .{}, .{ .id = 0 }, .{ .created, .id_must_not_be_zero } },
        .{ .{}, .{ .id = transfer_id_max }, .{ .created, .id_must_not_be_int_max } },
        .{ .{}, .{ .id = 1, .flags = pending }, .{ .created, .exists_with_different_flags } },
        .{ .{}, .{ .id = 1, .pending_id = 1 }, .{ .created, .exists_with_different_pending_id } },
        .{ .{}, .{ .id = 1, .timeout = 1 }, .{ .created, .exists_with_different_timeout } },
        .{ .{}, .{ .id = 1, .debit_account_id = 3 }, .{ .created, .exists_with_different_debit_account_id } },
        .{ .{}, .{ .id = 1, .credit_account_id = 3 }, .{ .created, .exists_with_different_credit_account_id } },
        .{ .{}, .{ .id = 1, .amount = 11 }, .{ .created, .exists_with_different_amount } },
        .{ .{}, .{ .id = 1, .user_data_128 = 1 }, .{ .created, .exists_with_different_user_data_128 } },
        .{ .{}, .{ .id = 1, .user_data_64 = 1 }, .{ .created, .exists_with_different_user_data_64 } },
        .{ .{}, .{ .id = 1, .user_data_32 = 1 }, .{ .created, .exists_with_different_user_data_32 } },
        .{ .{}, .{ .id = 1, .ledger = 2 }, .{ .created, .exists_with_different_ledger } },
        .{ .{}, .{ .id = 1, .code = 2 }, .{ .created, .exists_with_different_code } },
        .{ .{}, .{ .id = 1 }, .{ .created, .exists } },
        .{ .{ .debit_account_id = 3 }, .{ .id = 1, .debit_account_id = 3 }, .{ .debit_account_not_found, .id_already_failed } },
        .{ .{}, .{ .flags = tb.TransferFlags{ .pending = true, .post_pending_transfer = true } }, .{ .created, .flags_are_mutually_exclusive } },
        .{ .{}, .{ .debit_account_id = 0 }, .{ .created, .debit_account_id_must_not_be_zero } },
        .{ .{}, .{ .debit_account_id = transfer_id_max }, .{ .created, .debit_account_id_must_not_be_int_max } },
        .{ .{}, .{ .credit_account_id = 0 }, .{ .created, .credit_account_id_must_not_be_zero } },
        .{ .{}, .{ .credit_account_id = transfer_id_max }, .{ .created, .credit_account_id_must_not_be_int_max } },
        .{ .{}, .{ .credit_account_id = 1 }, .{ .created, .accounts_must_be_different } },
        .{ .{}, .{ .pending_id = 1 }, .{ .created, .pending_id_must_be_zero } },
        .{ .{ .flags = pending }, .{ .flags = post }, .{ .created, .pending_id_must_not_be_zero } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = transfer_id_max }, .{ .created, .pending_id_must_not_be_int_max } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 2 }, .{ .created, .pending_id_must_be_different } },
        .{ .{}, .{ .timeout = 1 }, .{ .created, .timeout_reserved_for_pending_transfer } },
        .{ .{}, .{ .flags = tb.TransferFlags{ .closing_debit = true } }, .{ .created, .closing_transfer_must_be_pending } },
        .{ .{}, .{ .ledger = 0 }, .{ .created, .ledger_must_not_be_zero } },
        .{ .{}, .{ .code = 0 }, .{ .created, .code_must_not_be_zero } },
        .{ .{}, .{ .debit_account_id = 3 }, .{ .created, .debit_account_not_found } },
        .{ .{}, .{ .credit_account_id = 3 }, .{ .created, .credit_account_not_found } },
        .{ .{}, .{ .ledger = 2 }, .{ .created, .transfer_must_have_the_same_ledger_as_accounts } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 3 }, .{ .created, .pending_transfer_not_found } },
        .{ .{}, .{ .flags = post, .pending_id = 1 }, .{ .created, .pending_transfer_not_pending } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 1, .debit_account_id = 3 }, .{ .created, .pending_transfer_has_different_debit_account_id } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 1, .credit_account_id = 3 }, .{ .created, .pending_transfer_has_different_credit_account_id } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 1, .ledger = 2 }, .{ .created, .pending_transfer_has_different_ledger } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 1, .code = 2 }, .{ .created, .pending_transfer_has_different_code } },
        .{ .{ .flags = pending }, .{ .flags = post, .pending_id = 1, .amount = 11 }, .{ .created, .exceeds_pending_transfer_amount } },
        .{ .{ .flags = pending }, .{ .flags = void_pending, .pending_id = 1, .amount = 9 }, .{ .created, .pending_transfer_has_different_amount } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported, .timestamp = 89 }, .{ .created, .imported_event_timestamp_must_not_regress } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = imported, .timestamp = 90 }, .{ .created, .imported_event_timestamp_must_not_regress } },
        .{ .{ .flags = imported, .timestamp = 10 }, .{ .flags = imported, .timestamp = 20 }, .{ .imported_event_timestamp_must_not_regress, .imported_event_timestamp_must_not_regress } },
        .{ .{ .flags = imported, .timestamp = 5 }, .{ .flags = imported, .timestamp = 15 }, .{ .imported_event_timestamp_must_postdate_debit_account, .imported_event_timestamp_must_postdate_credit_account } },
        .{ .{ .flags = imported, .timestamp = 90 }, .{ .flags = tb.TransferFlags{ .imported = true, .pending = true }, .timestamp = 91, .timeout = 1 }, .{ .created, .imported_event_timeout_must_be_zero } },
        .{ .{ .flags = imported, .timestamp = 102 }, .{ .flags = imported, .timestamp = 103 }, .{ .created, .created } },
        .{ .{ .flags = tb.TransferFlags{ .pending = true, .closing_debit = true } }, .{}, .{ .created, .debit_account_already_closed } },
        .{ .{ .flags = tb.TransferFlags{ .pending = true, .closing_credit = true } }, .{}, .{ .created, .credit_account_already_closed } },
        .{ .{ .flags = pending, .amount = amount_max }, .{ .flags = pending, .amount = 1 }, .{ .created, .overflows_debits_pending } },
        .{ .{ .amount = amount_max }, .{ .amount = 1 }, .{ .created, .overflows_debits_posted } },
        .{ .{ .flags = pending, .amount = amount_max }, .{ .amount = 1 }, .{ .created, .overflows_debits } },
    }) |triplet| {
        var model = base;
        var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
        inline for (0..2) |index| {
            const overrides = triplet[index];
            inline for (std.meta.fields(@TypeOf(overrides))) |field| {
                @field(transfers[index], field.name) = @field(overrides, field.name);
            }
        }
        try check(&model, &create_transfer_status_map, transfers, .{ triplet[2][0], triplet[2][1] });
    }

    // These failures depend on account configuration, which transfers cannot change.
    {
        var model = base;
        model.accounts[1].ledger = 2;
        try check(&model, &create_transfer_status_map, .{ test_transfer(1), test_transfer(2) }, .{
            .accounts_must_have_the_same_ledger, .accounts_must_have_the_same_ledger,
        });
    }
    inline for (.{
        .{ 0, "debits_must_not_exceed_credits", .exceeds_credits },
        .{ 1, "credits_must_not_exceed_debits", .exceeds_debits },
    }) |case| {
        var model = base;
        @field(model.accounts[case[0]].flags, case[1]) = true;
        var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
        transfers[0].amount = 0;
        try check(&model, &create_transfer_status_map, transfers, .{ .created, case[2] });
    }

    // Seed the credit account's opposite balances equally to isolate credit overflow
    // from the debit overflow that would otherwise take precedence.
    inline for (.{
        .{ "debits_pending", "credits_pending", true, .overflows_credits_pending },
        .{ "debits_posted", "credits_posted", false, .overflows_credits_posted },
        .{ "debits_pending", "credits_pending", false, .overflows_credits },
    }) |case| {
        var model = base;
        @field(model.accounts[1], case[0]) = amount_max - 10;
        @field(model.accounts[1], case[1]) = amount_max - 10;
        model.invariants();
        var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
        for (&transfers) |*transfer| transfer.flags.pending = case[2];
        transfers[1].amount = 1;
        try check(&model, &create_transfer_status_map, transfers, .{ .created, case[3] });
    }

    // Already-resolved statuses need a pending transfer before the pair of resolutions.
    {
        var pending_model = base;
        var seeds = [2]tb.Transfer{ test_transfer(3), test_transfer(4) };
        seeds[0].flags.pending = true;
        seeds[0].timeout = 1;
        seeds[1].amount = 0;
        try check(&pending_model, &create_transfer_status_map, seeds, .{ .created, .created });

        inline for (.{
            .{ post, .pending_transfer_already_posted },
            .{ void_pending, .pending_transfer_already_voided },
        }) |case| {
            var model = pending_model;
            var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
            for (&transfers) |*transfer| {
                transfer.flags = case[0];
                transfer.pending_id = 3;
            }
            try check(&model, &create_transfer_status_map, transfers, .{ .created, case[1] });
        }

        // Expiration requires advancing time between creation and resolution.
        var model = pending_model;
        model.now = model.transfers[0].transfer.timestamp + 1_000_000_000;
        try expectEqual(@as(usize, 1), model.pulse(1));
        var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
        transfers[0].flags = post;
        transfers[1].flags = void_pending;
        for (&transfers) |*transfer| transfer.pending_id = 3;
        try check(&model, &create_transfer_status_map, transfers, .{
            .pending_transfer_expired, .pending_transfer_expired,
        });
    }

    // A timeout overflows only when the clock is near its upper bound.
    {
        var model = base;
        model.now = timestamp_max - 1_000_000_000;
        var transfers = [2]tb.Transfer{ test_transfer(1), test_transfer(2) };
        transfers[1].flags = pending;
        transfers[1].timeout = 1;
        try check(&model, &create_transfer_status_map, transfers, .{ .created, .overflows_timeout });
    }

    inline for (std.enums.values(tb.CreateTransferStatus)) |status| {
        if (!@field(create_transfer_status_map, @tagName(status))) {
            std.debug.print("CreateTransferStatus not covered: {s}\n", .{@tagName(status)});
            return error.TestExpectedEqual;
        }
    }
}

// Keep fixtures small enough that every test can start with an independent model.
const TestModel = StateMachineReferenceType(8, 32);
const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;

fn test_model() TestModel {
    return .{
        .accounts = undefined,
        .transfers = undefined,
        .retired_ids = undefined,
        .now = 100,
    };
}

fn test_account(id: u128) tb.Account {
    var account = std.mem.zeroes(tb.Account);
    account.id = id;
    account.ledger = 1;
    account.code = 1;
    return account;
}

fn test_transfer(id: u128) tb.Transfer {
    var transfer = std.mem.zeroes(tb.Transfer);
    transfer.id = id;
    transfer.debit_account_id = 1;
    transfer.credit_account_id = 2;
    transfer.amount = 10;
    transfer.ledger = 1;
    transfer.code = 1;
    return transfer;
}

fn test_model_with_accounts() !TestModel {
    var model = test_model();
    var results: [2]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{ test_account(1), test_account(2) }, &results);
    for (results) |result| try expectEqual(.created, result.status);
    return model;
}

fn test_submit(model: *TestModel, transfer: tb.Transfer) !tb.CreateTransferResult {
    var results: [1]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{transfer}, &results);
    return results[0];
}

// Compare only initialized state, including the append-only journal's visible prefix.
fn test_state_equal(expected: *const TestModel, actual: *const TestModel) !void {
    try expectEqual(expected.accounts_count, actual.accounts_count);
    try expectEqual(expected.transfers_count, actual.transfers_count);
    try expectEqual(expected.retired_ids_count, actual.retired_ids_count);
    try expectEqual(expected.journal_count, actual.journal_count);
    try std.testing.expectEqualDeep(expected.accounts[0..expected.accounts_count], actual.accounts[0..actual.accounts_count]);
    try std.testing.expectEqualDeep(expected.transfers[0..expected.transfers_count], actual.transfers[0..actual.transfers_count]);
    try std.testing.expectEqualSlices(u128, expected.retired_ids[0..expected.retired_ids_count], actual.retired_ids[0..actual.retired_ids_count]);
    try std.testing.expectEqualDeep(expected.journal[0..expected.journal_count], actual.journal[0..actual.journal_count]);
}

fn test_account_rejected(account: tb.Account, status: tb.CreateAccountStatus) !void {
    var model = test_model();
    const before = model;
    try expectEqual(status, model.create_account(account, model.now, model.now));
    try test_state_equal(&before, &model);
}

fn test_transfer_rejected(model: *TestModel, transfer: tb.Transfer, status: tb.CreateTransferStatus) !void {
    const before = model.*;
    try expectEqual(status, model.create_transfer(transfer, model.now, model.now));
    try test_state_equal(&before, model);
}

test "state_machine reference model timestamp boundaries" {
    inline for (.{ tb.CreateAccountStatus, tb.CreateTransferStatus }) |Status| {
        try expectEqual(null, TestModel.time_valid(Status, false, 0, 100));
        try expectEqual(Status.timestamp_must_be_zero, TestModel.time_valid(Status, false, 1, 100).?);
        for ([_]u64{ 0, timestamp_max, timestamp_max + 1 }) |timestamp| {
            try expectEqual(Status.imported_event_timestamp_out_of_range, TestModel.time_valid(Status, true, timestamp, 100).?);
        }
        try expectEqual(Status.imported_event_timestamp_must_not_advance, TestModel.time_valid(Status, true, 101, 100).?);
        try expectEqual(null, TestModel.time_valid(Status, true, 99, 100));
        try expectEqual(null, TestModel.time_valid(Status, true, 100, 100));
        try expectEqual(null, TestModel.time_valid(Status, true, timestamp_max - 1, timestamp_max - 1));
    }
}

test "state_machine reference model account validation and duplicates" {
    inline for (.{
        .{ "timestamp", 1, .timestamp_must_be_zero },
        .{ "reserved", 1, .reserved_field },
        .{ "id", 0, .id_must_not_be_zero },
        .{ "id", account_max_id, .id_must_not_be_int_max },
        .{ "debits_pending", 1, .debits_pending_must_be_zero },
        .{ "debits_posted", 1, .debits_posted_must_be_zero },
        .{ "credits_pending", 1, .credits_pending_must_be_zero },
        .{ "credits_posted", 1, .credits_posted_must_be_zero },
        .{ "ledger", 0, .ledger_must_not_be_zero },
        .{ "code", 0, .code_must_not_be_zero },
    }) |case| {
        var account = test_account(1);
        @field(account, case[0]) = case[1];
        try test_account_rejected(account, case[2]);
    }
    var account = test_account(1);
    account.flags.padding = 1;
    try test_account_rejected(account, .reserved_flag);
    account.flags = .{ .debits_must_not_exceed_credits = true, .credits_must_not_exceed_debits = true };
    try test_account_rejected(account, .flags_are_mutually_exclusive);

    var model = try test_model_with_accounts();
    const before = model;
    inline for (.{ "user_data_128", "user_data_64", "user_data_32", "ledger", "code" }) |field| {
        account = test_account(1);
        @field(account, field) += 1;
        try expectEqual(@field(tb.CreateAccountStatus, "exists_with_different_" ++ field), model.create_account(account, model.now, model.now));
    }
    account = test_account(1);
    account.flags.history = true;
    try expectEqual(.exists_with_different_flags, model.create_account(account, model.now, model.now));
    var results: [1]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{test_account(1)}, &results);
    try expectEqual(.exists, results[0].status);
    try expectEqual(@as(u64, 101), results[0].timestamp);
    try test_state_equal(&before, &model);
}

test "state_machine reference model transfer validation" {
    inline for (.{
        .{ "timestamp", 1, .timestamp_must_be_zero },
        .{ "id", 0, .id_must_not_be_zero },
        .{ "id", transfer_id_max, .id_must_not_be_int_max },
        .{ "debit_account_id", 0, .debit_account_id_must_not_be_zero },
        .{ "debit_account_id", transfer_id_max, .debit_account_id_must_not_be_int_max },
        .{ "credit_account_id", 0, .credit_account_id_must_not_be_zero },
        .{ "credit_account_id", transfer_id_max, .credit_account_id_must_not_be_int_max },
        .{ "credit_account_id", 1, .accounts_must_be_different },
        .{ "pending_id", 1, .pending_id_must_be_zero },
        .{ "timeout", 1, .timeout_reserved_for_pending_transfer },
        .{ "ledger", 0, .ledger_must_not_be_zero },
        .{ "code", 0, .code_must_not_be_zero },
        .{ "debit_account_id", 3, .debit_account_not_found },
        .{ "credit_account_id", 3, .credit_account_not_found },
        .{ "ledger", 2, .transfer_must_have_the_same_ledger_as_accounts },
    }) |case| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        @field(transfer, case[0]) = case[1];
        try test_transfer_rejected(&model, transfer, case[2]);
    }
    inline for (.{
        .{ tb.TransferFlags{ .padding = 1 }, .reserved_flag },
        .{ tb.TransferFlags{ .pending = true, .post_pending_transfer = true }, .flags_are_mutually_exclusive },
        .{ tb.TransferFlags{ .pending = true, .void_pending_transfer = true }, .flags_are_mutually_exclusive },
        .{ tb.TransferFlags{ .post_pending_transfer = true, .void_pending_transfer = true }, .flags_are_mutually_exclusive },
        .{ tb.TransferFlags{ .closing_debit = true }, .closing_transfer_must_be_pending },
        .{ tb.TransferFlags{ .closing_credit = true }, .closing_transfer_must_be_pending },
    }) |case| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        transfer.flags = case[0];
        try test_transfer_rejected(&model, transfer, case[1]);
    }
    inline for (.{ "post_pending_transfer", "void_pending_transfer" }) |resolve| {
        inline for (.{ "balancing_debit", "balancing_credit", "closing_debit", "closing_credit" }) |flag| {
            var model = try test_model_with_accounts();
            var transfer = test_transfer(1);
            @field(transfer.flags, resolve) = true;
            @field(transfer.flags, flag) = true;
            try test_transfer_rejected(&model, transfer, .flags_are_mutually_exclusive);
        }
    }
    var model = try test_model_with_accounts();
    model.accounts[1].ledger = 2;
    try test_transfer_rejected(&model, test_transfer(1), .accounts_must_have_the_same_ledger);
}

test "state_machine reference model single phase and duplicate comparisons" {
    var model = try test_model_with_accounts();
    const transfer = test_transfer(1);
    const created = try test_submit(&model, transfer);
    try expectEqual(.created, created.status);
    try expectEqual(@as(u64, 103), created.timestamp);
    try expectEqual(@as(u128, 10), model.accounts[0].debits_posted);
    try expectEqual(@as(u128, 10), model.accounts[1].credits_posted);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 0), model.accounts[1].credits_pending);
    try expectEqual(TestModel.PendingState.none, model.transfers[0].pending_state);
    try expectEqual(TestModel.ChangeEventKind.single_phase, model.journal[0].kind);
    try expectEqual(created.timestamp, model.watermark());
    try std.testing.expectEqualDeep(model.accounts[0], model.journal[0].debit_account);
    try std.testing.expectEqualDeep(model.accounts[1], model.journal[0].credit_account);
    try std.testing.expectEqualDeep(model.transfers[0].transfer, model.journal[0].transfer);
    const before = model;
    const exists = try test_submit(&model, transfer);
    try expectEqual(.exists, exists.status);
    try expectEqual(created.timestamp, exists.timestamp);
    try test_state_equal(&before, &model);
    inline for (.{ "pending_id", "timeout", "debit_account_id", "credit_account_id", "amount", "user_data_128", "user_data_64", "user_data_32", "ledger", "code" }) |field| {
        var different = transfer;
        @field(different, field) += 1;
        try test_transfer_rejected(&model, different, @field(tb.CreateTransferStatus, "exists_with_different_" ++ field));
    }
    var different = transfer;
    different.flags.pending = true;
    try test_transfer_rejected(&model, different, .exists_with_different_flags);
}

test "state_machine reference model balance limits and overflow boundaries" {
    inline for (.{
        .{ 0, "debits_pending", true, .overflows_debits_pending, "credits_pending" },
        .{ 1, "credits_pending", true, .overflows_credits_pending, "debits_pending" },
        .{ 0, "debits_posted", false, .overflows_debits_posted, "credits_posted" },
        .{ 1, "credits_posted", false, .overflows_credits_posted, "debits_posted" },
        .{ 0, "debits_pending", false, .overflows_debits, "credits_pending" },
        .{ 1, "credits_pending", false, .overflows_credits, "debits_pending" },
    }) |case| {
        var model = try test_model_with_accounts();
        @field(model.accounts[case[0]], case[1]) = amount_max - 10;
        // Offset the seeded balance on the same account's opposite side, which
        // this transfer does not change, preserving overflow error precedence.
        @field(model.accounts[case[0]], case[4]) = amount_max - 10;
        model.invariants();
        var transfer = test_transfer(1);
        transfer.flags.pending = case[2];
        transfer.amount = 11;
        try test_transfer_rejected(&model, transfer, case[3]);
        transfer.amount = 10;
        try expectEqual(.created, (try test_submit(&model, transfer)).status);
    }
    inline for (.{ false, true }) |credit| {
        var model = try test_model_with_accounts();
        if (credit) {
            model.accounts[1].flags.credits_must_not_exceed_debits = true;
            model.accounts[1].debits_posted = 10;
            model.accounts[0].credits_posted = 10;
        } else {
            model.accounts[0].flags.debits_must_not_exceed_credits = true;
            model.accounts[0].credits_posted = 10;
            model.accounts[1].debits_posted = 10;
        }
        var transfer = test_transfer(1);
        transfer.amount = 11;
        try test_transfer_rejected(&model, transfer, if (credit) .exceeds_debits else .exceeds_credits);
        transfer.amount = 10;
        try expectEqual(.created, (try test_submit(&model, transfer)).status);
    }
    var model = try test_model_with_accounts();
    model.now = timestamp_max - 1_000_000_000;
    var transfer = test_transfer(1);
    transfer.flags.pending = true;
    transfer.timeout = 1;
    try test_transfer_rejected(&model, transfer, .overflows_timeout);
    model.now -= 2;
    try expectEqual(.created, (try test_submit(&model, transfer)).status);
}

test "state_machine reference model balancing amounts and retries" {
    inline for (.{ "balancing_debit", "balancing_credit" }) |flag| {
        for ([_]u128{ 0, 5, 10, 20 }) |available| {
            var model = try test_model_with_accounts();
            model.accounts[0].credits_posted = available + 5;
            model.accounts[0].debits_posted = 3;
            model.accounts[0].debits_pending = 2;
            model.accounts[1].debits_posted = available + 5;
            model.accounts[1].credits_posted = 3;
            model.accounts[1].credits_pending = 2;
            var transfer = test_transfer(1);
            @field(transfer.flags, flag) = true;
            try expectEqual(.created, (try test_submit(&model, transfer)).status);
            const amount = @min(available, 10);
            try expectEqual(amount, model.transfers[0].transfer.amount);
            try expectEqual(amount + 3, model.accounts[0].debits_posted);
            try expectEqual(amount + 3, model.accounts[1].credits_posted);
            try expectEqual(.exists, (try test_submit(&model, transfer)).status);
            transfer.amount = amount;
            try expectEqual(.exists, (try test_submit(&model, transfer)).status);
            if (amount > 0) {
                transfer.amount = amount - 1;
                try test_transfer_rejected(&model, transfer, .exists_with_different_amount);
            }
        }
    }
    var model = try test_model_with_accounts();
    model.accounts[0].debits_posted = 20;
    model.accounts[1].credits_posted = 20;
    var transfer = test_transfer(1);
    transfer.flags = .{ .balancing_debit = true, .balancing_credit = true };
    try expectEqual(.created, (try test_submit(&model, transfer)).status);
    try expectEqual(@as(u128, 0), model.transfers[0].transfer.amount);
}

fn test_pending(model: *TestModel) !tb.Transfer {
    var transfer = test_transfer(1);
    transfer.flags.pending = true;
    transfer.user_data_128 = 128;
    transfer.user_data_64 = 64;
    transfer.user_data_32 = 32;
    try expectEqual(.created, (try test_submit(model, transfer)).status);
    return transfer;
}

fn test_resolution(id: u128, post: bool) tb.Transfer {
    var transfer = std.mem.zeroes(tb.Transfer);
    transfer.id = id;
    transfer.pending_id = 1;
    transfer.flags.post_pending_transfer = post;
    transfer.flags.void_pending_transfer = !post;
    transfer.amount = if (post) amount_max else 0;
    return transfer;
}

test "state_machine reference model pending resolution validation" {
    inline for (.{
        .{ "pending_id", 0, .pending_id_must_not_be_zero },
        .{ "pending_id", transfer_id_max, .pending_id_must_not_be_int_max },
        .{ "pending_id", 2, .pending_id_must_be_different },
        .{ "pending_id", 3, .pending_transfer_not_found },
        .{ "timeout", 1, .timeout_reserved_for_pending_transfer },
        .{ "debit_account_id", 3, .pending_transfer_has_different_debit_account_id },
        .{ "credit_account_id", 3, .pending_transfer_has_different_credit_account_id },
        .{ "ledger", 2, .pending_transfer_has_different_ledger },
        .{ "code", 2, .pending_transfer_has_different_code },
        .{ "amount", 11, .exceeds_pending_transfer_amount },
    }) |case| {
        var model = try test_model_with_accounts();
        _ = try test_pending(&model);
        var resolution = test_resolution(2, true);
        @field(resolution, case[0]) = case[1];
        try test_transfer_rejected(&model, resolution, case[2]);
    }
    var model = try test_model_with_accounts();
    try expectEqual(.created, (try test_submit(&model, test_transfer(1))).status);
    try test_transfer_rejected(&model, test_resolution(2, true), .pending_transfer_not_pending);

    model = try test_model_with_accounts();
    _ = try test_pending(&model);
    var resolution = test_resolution(2, false);
    resolution.amount = 9;
    try test_transfer_rejected(&model, resolution, .pending_transfer_has_different_amount);
    resolution.amount = 11;
    try test_transfer_rejected(&model, resolution, .exceeds_pending_transfer_amount);
    model.transfers[0].pending_state = .expired;
    try test_transfer_rejected(&model, test_resolution(2, true), .pending_transfer_expired);
}

test "state_machine reference model pending post and void transitions" {
    for ([_]bool{ false, true }) |post| {
        for ([_]u128{ 0, 4, 10, amount_max }) |requested| {
            if (!post and requested != 0 and requested != 10) continue;
            var model = try test_model_with_accounts();
            _ = try test_pending(&model);
            try expectEqual(@as(u128, 10), model.accounts[0].debits_pending);
            try expectEqual(@as(u128, 10), model.accounts[1].credits_pending);
            try expectEqual(@as(u128, 0), model.accounts[0].debits_posted);
            try expectEqual(TestModel.PendingState.pending, model.transfers[0].pending_state);
            try expectEqual(TestModel.ChangeEventKind.two_phase_pending, model.journal[0].kind);
            var resolution = test_resolution(2, post);
            resolution.amount = requested;
            const result = try test_submit(&model, resolution);
            try expectEqual(.created, result.status);
            const amount: u128 = if (!post or requested == amount_max) 10 else requested;
            try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
            try expectEqual(@as(u128, 0), model.accounts[1].credits_pending);
            try expectEqual(if (post) amount else 0, model.accounts[0].debits_posted);
            try expectEqual(if (post) amount else 0, model.accounts[1].credits_posted);
            try expectEqual(if (post) TestModel.PendingState.posted else .voided, model.transfers[0].pending_state);
            try expectEqual(if (post) TestModel.ChangeEventKind.two_phase_posted else .two_phase_voided, model.journal[1].kind);
            const stored = model.transfers[1].transfer;
            try expectEqual(amount, stored.amount);
            inline for (.{ "debit_account_id", "credit_account_id", "ledger", "code", "user_data_128", "user_data_64", "user_data_32" }) |field| {
                try expectEqual(@field(model.transfers[0].transfer, field), @field(stored, field));
            }
            const before = model;
            const retry = try test_submit(&model, resolution);
            try expectEqual(.exists, retry.status);
            try expectEqual(result.timestamp, retry.timestamp);
            try test_state_equal(&before, &model);
            var explicit = stored;
            explicit.timestamp = 0;
            try expectEqual(.exists, (try test_submit(&model, explicit)).status);
            inline for (.{ "debit_account_id", "credit_account_id", "ledger", "code", "user_data_128", "user_data_64", "user_data_32" }) |field| {
                var different = explicit;
                @field(different, field) += 1;
                try test_transfer_rejected(&model, different, @field(tb.CreateTransferStatus, "exists_with_different_" ++ field));
            }
            try test_transfer_rejected(&model, test_resolution(3, post), if (post) .pending_transfer_already_posted else .pending_transfer_already_voided);
        }
    }
    // Explicit matching account fields and nonzero user data bypass inheritance.
    var model = try test_model_with_accounts();
    _ = try test_pending(&model);
    var resolution = test_transfer(2);
    resolution.pending_id = 1;
    resolution.flags.post_pending_transfer = true;
    resolution.user_data_128 = 9;
    resolution.user_data_64 = 8;
    resolution.user_data_32 = 7;
    try expectEqual(.created, (try test_submit(&model, resolution)).status);
    try expectEqual(@as(u128, 9), model.transfers[1].transfer.user_data_128);
    try expectEqual(@as(u64, 8), model.transfers[1].transfer.user_data_64);
    try expectEqual(@as(u32, 7), model.transfers[1].transfer.user_data_32);
}

test "state_machine reference model closing accounts and void reopening" {
    inline for (.{ "closing_debit", "closing_credit" }) |flag| {
        var model = try test_model_with_accounts();
        var pending = test_transfer(1);
        pending.flags.pending = true;
        @field(pending.flags, flag) = true;
        try expectEqual(.created, (try test_submit(&model, pending)).status);
        const debit = comptime std.mem.eql(u8, flag, "closing_debit");
        try expectEqual(debit, model.accounts[0].flags.closed);
        try expectEqual(!debit, model.accounts[1].flags.closed);
        try test_transfer_rejected(&model, test_transfer(2), if (debit) .debit_account_already_closed else .credit_account_already_closed);
        try test_transfer_rejected(&model, test_resolution(2, true), if (debit) .debit_account_already_closed else .credit_account_already_closed);
        try expectEqual(.created, (try test_submit(&model, test_resolution(2, false))).status);
        try expect(!model.accounts[0].flags.closed);
        try expect(!model.accounts[1].flags.closed);
        try expectEqual(.created, (try test_submit(&model, test_transfer(3))).status);
    }
}

test "state_machine reference model expiration deadline and expired journal" {
    for ([_]u64{ 999_999_999, 1_000_000_000, 1_000_000_001 }) |elapsed| {
        var model = try test_model_with_accounts();
        var pending = test_transfer(1);
        pending.flags.pending = true;
        pending.timeout = 1;
        const created = try test_submit(&model, pending);
        model.now = created.timestamp + elapsed - 1;
        if (elapsed < 1_000_000_000) {
            try expectEqual(.created, (try test_submit(&model, test_resolution(2, true))).status);
        } else {
            const before = model;
            try expectEqual(.pending_transfer_expired, (try test_submit(&model, test_resolution(2, true))).status);
            try expectEqual(.pending_transfer_expired, (try test_submit(&model, test_resolution(2, false))).status);
            try test_state_equal(&before, &model);
        }
    }
    // Exercise the journal's expiration kind directly; pulse behavior is tested below.
    var model = try test_model_with_accounts();
    _ = try test_pending(&model);
    model.advance(1);
    model.record(model.transfers[0].transfer, model.now, true);
    try expectEqual(TestModel.ChangeEventKind.two_phase_expired, model.journal[1].kind);
    try expectEqual(model.now, model.watermark());
}

test "state_machine reference model imported accounts" {
    for ([_]u64{ 0, timestamp_max, timestamp_max + 1, 101 }) |timestamp| {
        var account = test_account(1);
        account.flags.imported = true;
        account.timestamp = timestamp;
        try test_account_rejected(account, if (timestamp == 101)
            .imported_event_timestamp_must_not_advance
        else
            .imported_event_timestamp_out_of_range);
    }
    var model = test_model();
    var account = test_account(1);
    account.flags.imported = true;
    account.timestamp = 10;
    var results: [1]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{account}, &results);
    try expectEqual(.created, results[0].status);
    try expectEqual(@as(u64, 10), results[0].timestamp);
    try model.create_accounts(&.{account}, &results);
    try expectEqual(.exists, results[0].status);
    try expectEqual(@as(u64, 10), results[0].timestamp);
    account.id = 2;
    for ([_]u64{ 9, 10 }) |timestamp| {
        account.timestamp = timestamp;
        try expectEqual(.imported_event_timestamp_must_not_regress, model.create_account(account, model.now, model.now));
    }
    account.timestamp = 11;
    try expectEqual(.created, model.create_account(account, model.now, model.now));
    try expectEqual(.created, (try test_submit(&model, test_transfer(1))).status);
    account.id = 3;
    account.timestamp = model.transfers[0].transfer.timestamp;
    try expectEqual(.imported_event_timestamp_must_not_regress, model.create_account(account, model.now, model.now));
    // Accounts may predate transfers if their timestamps do not collide.
    account.timestamp = 12;
    try expectEqual(.created, model.create_account(account, model.now, model.now));
}

test "state_machine reference model imported transfers" {
    for ([_]u64{ 0, timestamp_max, timestamp_max + 1, 103 }) |timestamp| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        transfer.flags.imported = true;
        transfer.timestamp = timestamp;
        try test_transfer_rejected(&model, transfer, if (timestamp == 103)
            .imported_event_timestamp_must_not_advance
        else
            .imported_event_timestamp_out_of_range);
    }
    var model = try test_model_with_accounts();
    model.now = 200;
    var transfer = test_transfer(1);
    transfer.flags.imported = true;
    transfer.timestamp = 101;
    try test_transfer_rejected(&model, transfer, .imported_event_timestamp_must_not_regress);
    transfer.timestamp = 99;
    try test_transfer_rejected(&model, transfer, .imported_event_timestamp_must_postdate_debit_account);
    // Put the debit account earlier so the credit account's postdate check is reached.
    model.accounts[0].timestamp = 98;
    transfer.timestamp = 100;
    try test_transfer_rejected(&model, transfer, .imported_event_timestamp_must_postdate_credit_account);
    transfer.timestamp = 103;
    transfer.flags.pending = true;
    transfer.timeout = 1;
    try test_transfer_rejected(&model, transfer, .imported_event_timeout_must_be_zero);
    transfer.timeout = 0;
    const created = try test_submit(&model, transfer);
    try expectEqual(.created, created.status);
    try expectEqual(@as(u64, 103), created.timestamp);
    try expectEqual(@as(u64, 103), model.watermark());
    const retry = try test_submit(&model, transfer);
    try expectEqual(.exists, retry.status);
    try expectEqual(created.timestamp, retry.timestamp);
    transfer.id = 2;
    for ([_]u64{ 102, 103 }) |timestamp| {
        transfer.timestamp = timestamp;
        try test_transfer_rejected(&model, transfer, .imported_event_timestamp_must_not_regress);
    }
    transfer.timestamp = 104;
    try expectEqual(.created, (try test_submit(&model, transfer)).status);
}

test "state_machine reference model empty batches and clock limits" {
    var model = test_model();
    const before = model;
    try model.create_accounts(&.{}, &.{});
    try model.create_transfers(&.{}, &.{});
    try test_state_equal(&before, &model);
    try expectEqual(before.now, model.now);
    try expectEqual(@as(u64, 0), model.watermark());
    model.now = timestamp_max - 1;
    var account_results: [1]tb.CreateAccountResult = undefined;
    var transfer_results: [1]tb.CreateTransferResult = undefined;
    try std.testing.expectError(error.InvalidTime, model.create_accounts(&.{test_account(1)}, &account_results));
    try std.testing.expectError(error.InvalidTime, model.create_transfers(&.{test_transfer(1)}, &transfer_results));
    try expectEqual(timestamp_max - 1, model.now);
    try test_state_equal(&before, &model);
    // Empty batches still succeed at the admission limit.
    try model.create_accounts(&.{}, &.{});
    model.now -= 1;
    try model.create_accounts(&.{test_account(1)}, &account_results);
    try expectEqual(.created, account_results[0].status);
    try expectEqual(timestamp_max - 1, account_results[0].timestamp);
    try expectEqual(timestamp_max - 1, model.now);
}

test "state_machine reference model batch import mode and admission cutoff" {
    inline for (.{ tb.Account, tb.Transfer }) |Event| {
        const Result = if (Event == tb.Account) tb.CreateAccountResult else tb.CreateTransferResult;
        for ([_]bool{ false, true }) |imported| {
            var model = try test_model_with_accounts();
            model.now = 200;
            var events: [2]Event = if (Event == tb.Account)
                .{ test_account(3), test_account(4) }
            else
                .{ test_transfer(1), test_transfer(2) };
            events[0].flags.imported = imported;
            events[0].timestamp = if (imported) 150 else 0;
            events[1].flags.imported = !imported;
            events[1].timestamp = if (!imported) 151 else 0;
            var results: [2]Result = undefined;
            try model.execute(Event, &events, Result, &results);
            try expectEqual(.created, results[0].status);
            try expectEqual(if (imported) @as(u64, 150) else 201, results[0].timestamp);
            try expectEqual(if (imported) @FieldType(Result, "status").imported_event_expected else .imported_event_not_expected, results[1].status);
            try expectEqual(@as(u64, 202), results[1].timestamp);
            try expectEqual(@as(u64, 202), model.now);
        }
        var model = try test_model_with_accounts();
        model.now = 200;
        var events: [2]Event = if (Event == tb.Account)
            .{ test_account(3), test_account(4) }
        else
            .{ test_transfer(1), test_transfer(2) };
        for (&events) |*event| event.flags.imported = true;
        // The first event can use the last timestamp admitted by this batch.
        events[0].timestamp = 201;
        events[1].timestamp = 202;
        var results: [2]Result = undefined;
        try model.execute(Event, &events, Result, &results);
        try expectEqual(.created, results[0].status);
        try expectEqual(@as(u64, 201), results[0].timestamp);
        try expectEqual(.imported_event_timestamp_must_not_advance, results[1].status);
        // A new API call has a new cutoff.
        try model.execute(Event, events[1..], Result, results[1..]);
        try expectEqual(.created, results[1].status);
        try expectEqual(@as(u64, 202), results[1].timestamp);
    }
}

test "state_machine reference model account chains rollback and open tails" {
    var model = test_model();
    var accounts = [_]tb.Account{ test_account(1), test_account(2), test_account(3), test_account(4), test_account(5) };
    accounts[1].flags.linked = true;
    accounts[2].flags.linked = true;
    accounts[2].code = 0;
    var results: [5]tb.CreateAccountResult = undefined;
    try model.create_accounts(&accounts, &results);
    const statuses = [_]tb.CreateAccountStatus{ .created, .linked_event_failed, .code_must_not_be_zero, .linked_event_failed, .created };
    for (results, statuses, 0..) |result, status, index| {
        try expectEqual(status, result.status);
        try expectEqual(@as(u64, 101) + index, result.timestamp);
    }
    try expectEqual(@as(usize, 2), model.accounts_count);
    try expectEqual(@as(u128, 1), model.accounts[0].id);
    try expectEqual(@as(u128, 5), model.accounts[1].id);
    try expectEqual(@as(usize, 0), model.retired_ids_count);
    // An existing event also fails its chain and retains its original timestamp.
    accounts[0] = test_account(6);
    accounts[0].flags.linked = true;
    accounts[1] = test_account(1);
    const before = model;
    try model.create_accounts(accounts[0..2], results[0..2]);
    try expectEqual(.linked_event_failed, results[0].status);
    try expectEqual(.exists, results[1].status);
    try expectEqual(@as(u64, 101), results[1].timestamp);
    try test_state_equal(&before, &model);
    for (accounts[0..2]) |*account| account.flags.linked = true;
    try model.create_accounts(accounts[0..2], results[0..2]);
    try expectEqual(.linked_event_failed, results[0].status);
    try expectEqual(.linked_event_chain_open, results[1].status);
    try test_state_equal(&before, &model);
    // An open tail takes precedence even when an earlier event failed.
    accounts[0].id = 0;
    try model.create_accounts(accounts[0..2], results[0..2]);
    try expectEqual(.id_must_not_be_zero, results[0].status);
    try expectEqual(.linked_event_chain_open, results[1].status);
    try test_state_equal(&before, &model);
    accounts[0] = test_account(6);
    accounts[0].flags.linked = true;
    accounts[1] = test_account(7);
    try model.create_accounts(accounts[0..2], results[0..2]);
    for (results[0..2]) |result| try expectEqual(.created, result.status);
    try expectEqual(@as(usize, 4), model.accounts_count);
}

test "state_machine reference model transfer chains restore balances pending states and journal" {
    for ([_]bool{ false, true }) |post| {
        var model = try test_model_with_accounts();
        _ = try test_pending(&model);
        const before = model;
        var resolution = test_resolution(2, post);
        resolution.flags.linked = true;
        var invalid = test_transfer(3);
        invalid.code = 0;
        var results: [2]tb.CreateTransferResult = undefined;
        try model.create_transfers(&.{ resolution, invalid }, &results);
        try expectEqual(.linked_event_failed, results[0].status);
        try expectEqual(.code_must_not_be_zero, results[1].status);
        try test_state_equal(&before, &model);
        resolution.flags.linked = false;
        try expectEqual(.created, (try test_submit(&model, resolution)).status);
        try expectEqual(@as(usize, 2), model.journal_count);
        try expectEqual(model.now, model.journal[1].timestamp);
    }
    var model = try test_model_with_accounts();
    const before = model;
    var closing = test_transfer(1);
    closing.flags = .{ .pending = true, .closing_debit = true, .closing_credit = true, .linked = true };
    var invalid = test_transfer(2);
    invalid.code = 0;
    var results: [2]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{ closing, invalid }, &results);
    try test_state_equal(&before, &model);
    // A successful chain can resolve the pending transfer created by its first event.
    closing.flags.closing_debit = false;
    closing.flags.closing_credit = false;
    try model.create_transfers(&.{ closing, test_resolution(2, true) }, &results);
    for (results) |result| try expectEqual(.created, result.status);
    try expectEqual(TestModel.PendingState.posted, model.transfers[0].pending_state);
    try expectEqual(@as(u128, 10), model.accounts[0].debits_posted);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
    try expectEqual(@as(usize, 2), model.journal_count);
}

test "state_machine reference model transient failure retirement uses chain relative index" {
    var model = try test_model_with_accounts();
    var transfers = [_]tb.Transfer{ test_transfer(1), test_transfer(2), test_transfer(3), test_transfer(4), test_transfer(5) };
    transfers[1].flags.linked = true;
    transfers[2].flags.linked = true;
    transfers[2].debit_account_id = 9;
    var results: [5]tb.CreateTransferResult = undefined;
    try model.create_transfers(&transfers, &results);
    const statuses = [_]tb.CreateTransferStatus{ .created, .linked_event_failed, .debit_account_not_found, .linked_event_failed, .created };
    for (results, statuses, 0..) |result, status, index| {
        try expectEqual(status, result.status);
        try expectEqual(@as(u64, 103) + index, result.timestamp);
    }
    try expectEqual(@as(usize, 1), model.retired_ids_count);
    try expectEqual(@as(u128, 3), model.retired_ids[0]);
    try expectEqual(@as(usize, 2), model.transfers_count);
    try expectEqual(@as(usize, 2), model.journal_count);
    try expectEqual(@as(u128, 20), model.accounts[0].debits_posted);
    try expectEqual(.id_already_failed, (try test_submit(&model, test_transfer(3))).status);
    try expectEqual(.created, (try test_submit(&model, test_transfer(2))).status);
    try expectEqual(.created, (try test_submit(&model, test_transfer(4))).status);

    // Conversely, a previous transient failure must not retire a later permanent failure.
    model = try test_model_with_accounts();
    transfers[0].debit_account_id = 9;
    transfers[1] = test_transfer(2);
    transfers[1].code = 0;
    try model.create_transfers(transfers[0..2], results[0..2]);
    try expectEqual(.debit_account_not_found, results[0].status);
    try expectEqual(.code_must_not_be_zero, results[1].status);
    try expectEqual(@as(usize, 1), model.retired_ids_count);
    try expectEqual(@as(u128, 1), model.retired_ids[0]);
    try expectEqual(.created, (try test_submit(&model, test_transfer(2))).status);
}

test "state_machine reference model transient failures retire only the failed event" {
    inline for (.{
        tb.CreateTransferStatus.debit_account_not_found,
        .credit_account_not_found,
        .pending_transfer_not_found,
        .exceeds_credits,
        .exceeds_debits,
        .debit_account_already_closed,
        .credit_account_already_closed,
    }) |status| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        switch (status) {
            .debit_account_not_found => transfer.debit_account_id = 9,
            .credit_account_not_found => transfer.credit_account_id = 9,
            .pending_transfer_not_found => transfer = test_resolution(2, true),
            .exceeds_credits => model.accounts[0].flags.debits_must_not_exceed_credits = true,
            .exceeds_debits => model.accounts[1].flags.credits_must_not_exceed_debits = true,
            .debit_account_already_closed => model.accounts[0].flags.closed = true,
            .credit_account_already_closed => model.accounts[1].flags.closed = true,
            else => unreachable,
        }
        const before = model;
        const failed = try test_submit(&model, transfer);
        try expectEqual(status, failed.status);
        try expectEqual(before.now + 1, failed.timestamp);
        try expectEqual(@as(usize, 1), model.retired_ids_count);
        try expectEqual(transfer.id, model.retired_ids[0]);
        var expected = before;
        expected.retired_ids[0] = transfer.id;
        expected.retired_ids_count = 1;
        try test_state_equal(&expected, &model);
        // Correcting the request or account cannot revive an already failed ID.
        model.accounts[0].flags = .{};
        model.accounts[1].flags = .{};
        try expectEqual(.id_already_failed, (try test_submit(&model, test_transfer(transfer.id))).status);
        try expectEqual(@as(usize, 1), model.retired_ids_count);
    }
}

test "state_machine reference model imported chain rollback preserves result timestamps" {
    inline for (.{ tb.Account, tb.Transfer }) |Event| {
        const Result = if (Event == tb.Account) tb.CreateAccountResult else tb.CreateTransferResult;
        var model = try test_model_with_accounts();
        model.now = 200;
        const before = model;
        var events: [3]Event = if (Event == tb.Account)
            .{ test_account(3), test_account(4), test_account(5) }
        else
            .{ test_transfer(1), test_transfer(2), test_transfer(3) };
        for (&events, 0..) |*event, index| {
            event.flags.imported = true;
            event.flags.linked = index < 2;
            event.timestamp = 150 + index;
        }
        events[1].code = 0;
        var results: [3]Result = undefined;
        try model.execute(Event, &events, Result, &results);
        try expectEqual(.linked_event_failed, results[0].status);
        try expectEqual(@as(u64, 150), results[0].timestamp);
        try expectEqual(.code_must_not_be_zero, results[1].status);
        try expectEqual(@as(u64, 202), results[1].timestamp);
        try expectEqual(.linked_event_failed, results[2].status);
        try expectEqual(@as(u64, 203), results[2].timestamp);
        try test_state_equal(&before, &model);
        try expectEqual(@as(u64, 203), model.now);
    }
}

test "state_machine reference model creation timestamps advance before validation" {
    var model = test_model();
    model.now = 0;
    var results: [2]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{ test_account(1), test_account(2) }, &results);
    for (results, 0..) |result, index| {
        try expectEqual(.created, result.status);
        try expectEqual(@as(u64, index + 1), result.timestamp);
        try expectEqual(result.timestamp, model.accounts[index].timestamp);
    }
    try expectEqual(@as(u64, 2), model.now);

    // This timestamp is admitted by the import cutoff, but collides with the
    // second account's creation timestamp and must therefore be rejected.
    var transfer = test_transfer(1);
    transfer.flags.imported = true;
    transfer.timestamp = 2;
    const before = model;
    const rejected = try test_submit(&model, transfer);
    try expectEqual(.imported_event_timestamp_must_not_regress, rejected.status);
    try expectEqual(@as(u64, 3), rejected.timestamp);
    try expectEqual(@as(u64, 3), model.now);
    try test_state_equal(&before, &model);

    transfer.timestamp = 3;
    const imported = try test_submit(&model, transfer);
    try expectEqual(.created, imported.status);
    try expectEqual(@as(u64, 3), imported.timestamp);
    try expectEqual(@as(u64, 4), model.now);

    const created = try test_submit(&model, test_transfer(2));
    try expectEqual(.created, created.status);
    try expectEqual(@as(u64, 5), created.timestamp);
    try expectEqual(created.timestamp, model.now);
    try expectEqual(created.timestamp, model.watermark());
}

test "state_machine reference model account validation precedence" {
    // Each row contains competing violations. Keep expected statuses explicit so
    // reordering validation cannot silently change the observable error.
    inline for (.{
        .{ .{ .timestamp = 1, .id = 0 }, .timestamp_must_be_zero },
        .{ .{ .reserved = 1, .flags = tb.AccountFlags{ .padding = 1 } }, .reserved_field },
        .{ .{ .flags = tb.AccountFlags{ .padding = 1 }, .id = 0 }, .reserved_flag },
        .{ .{ .id = 0, .ledger = 0 }, .id_must_not_be_zero },
        .{ .{ .id = account_max_id, .code = 0 }, .id_must_not_be_int_max },
        .{ .{
            .flags = tb.AccountFlags{ .debits_must_not_exceed_credits = true, .credits_must_not_exceed_debits = true },
            .debits_pending = 1,
        }, .flags_are_mutually_exclusive },
        .{ .{ .debits_pending = 1, .debits_posted = 1 }, .debits_pending_must_be_zero },
        .{ .{ .debits_posted = 1, .credits_pending = 1 }, .debits_posted_must_be_zero },
        .{ .{ .credits_pending = 1, .credits_posted = 1 }, .credits_pending_must_be_zero },
        .{ .{ .credits_posted = 1, .ledger = 0 }, .credits_posted_must_be_zero },
        .{ .{ .ledger = 0, .code = 0 }, .ledger_must_not_be_zero },
    }) |case| {
        var account = test_account(1);
        inline for (std.meta.fields(@TypeOf(case[0]))) |field| {
            @field(account, field.name) = @field(case[0], field.name);
        }
        try test_account_rejected(account, case[1]);
    }
}

test "state_machine reference model transfer validation precedence" {
    inline for (.{
        .{ .{ .timestamp = 1, .id = 0 }, .timestamp_must_be_zero },
        .{ .{ .flags = tb.TransferFlags{ .padding = 1 }, .id = 0 }, .reserved_flag },
        .{ .{ .id = 0, .debit_account_id = 0 }, .id_must_not_be_zero },
        .{ .{ .id = transfer_id_max, .debit_account_id = 0 }, .id_must_not_be_int_max },
        .{ .{
            .flags = tb.TransferFlags{ .pending = true, .post_pending_transfer = true },
            .pending_id = 0,
        }, .flags_are_mutually_exclusive },
        .{ .{ .debit_account_id = 0, .credit_account_id = 0 }, .debit_account_id_must_not_be_zero },
        .{ .{ .debit_account_id = transfer_id_max, .credit_account_id = 0 }, .debit_account_id_must_not_be_int_max },
        .{ .{ .credit_account_id = 0, .pending_id = 1 }, .credit_account_id_must_not_be_zero },
        .{ .{ .credit_account_id = transfer_id_max, .pending_id = 1 }, .credit_account_id_must_not_be_int_max },
        .{ .{ .credit_account_id = 1, .pending_id = 1 }, .accounts_must_be_different },
        .{ .{ .pending_id = 1, .timeout = 1 }, .pending_id_must_be_zero },
        .{ .{ .timeout = 1, .flags = tb.TransferFlags{ .closing_debit = true } }, .timeout_reserved_for_pending_transfer },
        .{ .{ .flags = tb.TransferFlags{ .closing_debit = true }, .ledger = 0 }, .closing_transfer_must_be_pending },
        .{ .{ .ledger = 0, .code = 0 }, .ledger_must_not_be_zero },
        .{ .{ .code = 0, .debit_account_id = 3 }, .code_must_not_be_zero },
        .{ .{ .debit_account_id = 3, .credit_account_id = 4 }, .debit_account_not_found },
    }) |case| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        inline for (std.meta.fields(@TypeOf(case[0]))) |field| {
            @field(transfer, field.name) = @field(case[0], field.name);
        }
        try test_transfer_rejected(&model, transfer, case[1]);
    }
}

test "state_machine reference model duplicate comparison precedence" {
    inline for (.{
        .{ .{ .flags = tb.AccountFlags{ .history = true }, .user_data_128 = 1 }, .exists_with_different_flags },
        .{ .{ .user_data_128 = 1, .user_data_64 = 1 }, .exists_with_different_user_data_128 },
        .{ .{ .user_data_64 = 1, .user_data_32 = 1 }, .exists_with_different_user_data_64 },
        .{ .{ .user_data_32 = 1, .ledger = 0 }, .exists_with_different_user_data_32 },
        .{ .{ .ledger = 0, .code = 0 }, .exists_with_different_ledger },
        .{ .{ .code = 0, .debits_pending = 1 }, .exists_with_different_code },
    }) |case| {
        var model = try test_model_with_accounts();
        const before = model;
        var account = test_account(1);
        inline for (std.meta.fields(@TypeOf(case[0]))) |field| {
            @field(account, field.name) = @field(case[0], field.name);
        }
        try expectEqual(@as(tb.CreateAccountStatus, case[1]), model.create_account(account, model.now, model.now));
        try test_state_equal(&before, &model);
    }
    inline for (.{
        .{ .{ .flags = tb.TransferFlags{ .pending = true }, .amount = 11 }, .exists_with_different_flags },
        .{ .{ .pending_id = 1, .timeout = 1 }, .exists_with_different_pending_id },
        .{ .{ .timeout = 1, .debit_account_id = 0 }, .exists_with_different_timeout },
        .{ .{ .debit_account_id = 0, .credit_account_id = 0 }, .exists_with_different_debit_account_id },
        .{ .{ .credit_account_id = 0, .amount = 11 }, .exists_with_different_credit_account_id },
        .{ .{ .amount = 11, .user_data_128 = 1 }, .exists_with_different_amount },
        .{ .{ .user_data_128 = 1, .user_data_64 = 1 }, .exists_with_different_user_data_128 },
        .{ .{ .user_data_64 = 1, .user_data_32 = 1 }, .exists_with_different_user_data_64 },
        .{ .{ .user_data_32 = 1, .ledger = 0 }, .exists_with_different_user_data_32 },
        .{ .{ .ledger = 0, .code = 0 }, .exists_with_different_ledger },
    }) |case| {
        var model = try test_model_with_accounts();
        var transfer = test_transfer(1);
        try expectEqual(.created, (try test_submit(&model, transfer)).status);
        inline for (std.meta.fields(@TypeOf(case[0]))) |field| {
            @field(transfer, field.name) = @field(case[0], field.name);
        }
        try test_transfer_rejected(&model, transfer, case[1]);
    }
}

test "state_machine reference model pending resolution precedence" {
    inline for (.{
        .{ .{ .pending_id = 0, .timeout = 1 }, .pending_id_must_not_be_zero },
        .{ .{ .pending_id = transfer_id_max, .timeout = 1 }, .pending_id_must_not_be_int_max },
        .{ .{ .pending_id = 2, .timeout = 1 }, .pending_id_must_be_different },
        .{ .{ .pending_id = 9, .timeout = 1 }, .timeout_reserved_for_pending_transfer },
        .{ .{ .pending_id = 9, .debit_account_id = 3 }, .pending_transfer_not_found },
        .{ .{ .debit_account_id = 3, .credit_account_id = 4 }, .pending_transfer_has_different_debit_account_id },
        .{ .{ .credit_account_id = 4, .ledger = 2 }, .pending_transfer_has_different_credit_account_id },
        .{ .{ .ledger = 2, .code = 2 }, .pending_transfer_has_different_ledger },
        .{ .{ .code = 2, .amount = 11 }, .pending_transfer_has_different_code },
        // The pending transfer is already posted in every row: amount checks
        // must still win over pending_transfer_already_posted.
        .{ .{ .amount = 11 }, .exceeds_pending_transfer_amount },
        .{ .{ .flags = tb.TransferFlags{ .void_pending_transfer = true }, .amount = 9 }, .pending_transfer_has_different_amount },
    }) |case| {
        var model = try test_model_with_accounts();
        _ = try test_pending(&model);
        try expectEqual(.created, (try test_submit(&model, test_resolution(3, true))).status);
        var transfer = test_resolution(2, true);
        inline for (std.meta.fields(@TypeOf(case[0]))) |field| {
            @field(transfer, field.name) = @field(case[0], field.name);
        }
        try test_transfer_rejected(&model, transfer, case[1]);
    }
}

fn test_query_model() !TestModel {
    var model = test_model();
    var accounts = [_]tb.Account{ test_account(1), test_account(2), test_account(3), test_account(4) };
    accounts[2].ledger = 2;
    accounts[3].ledger = 2;
    var results: [4]tb.CreateAccountResult = undefined;
    try model.create_accounts(&accounts, &results);
    for (results) |result| try expectEqual(.created, result.status);

    var transfers = [_]tb.Transfer{ test_transfer(90), test_transfer(10), test_transfer(70), test_transfer(30) };
    transfers[0].user_data_128 = (@as(u128, 1) << 100) + 7;
    transfers[0].user_data_64 = (@as(u64, 1) << 40) + 8;
    transfers[0].user_data_32 = 9;
    transfers[1].debit_account_id = 2;
    transfers[1].credit_account_id = 1;
    transfers[1].user_data_128 = transfers[0].user_data_128;
    transfers[1].code = 2;
    transfers[2].debit_account_id = 3;
    transfers[2].credit_account_id = 4;
    transfers[2].ledger = 2;
    transfers[2].user_data_64 = transfers[0].user_data_64;
    transfers[2].user_data_32 = 9;
    transfers[3].user_data_32 = 9;
    transfers[3].code = 3;
    for (transfers, 0..) |transfer, index| {
        const result = try test_submit(&model, transfer);
        try expectEqual(.created, result.status);
        try expectEqual(@as(u64, 105) + index, result.timestamp);
    }
    return model;
}

fn test_query(
    model: *const TestModel,
    filter: TestModel.TransferFilter,
    output_len: usize,
    expected_ids: []const u128,
) !void {
    const before = model.*;
    const sentinel = test_transfer(transfer_id_max);
    var output = [_]tb.Transfer{sentinel} ** 32;
    const matches = model.query_transfers(filter, output[0..output_len]);
    try expectEqual(expected_ids.len, matches.len);
    try expect(matches.ptr == output[0..].ptr);
    for (matches, expected_ids) |transfer, id| {
        try expectEqual(id, transfer.id);
        const index = model.transfer_index(id).?;
        try std.testing.expectEqualDeep(model.transfers[index].transfer, transfer);
    }
    // Neither unused buffer slots nor the model may be changed by a query.
    for (output[matches.len..]) |transfer| try std.testing.expectEqualDeep(sentinel, transfer);
    try test_state_equal(&before, model);
    try expectEqual(before.now, model.now);
}

test "state_machine reference model query_transfers empty results and output limits" {
    var model = test_model();
    try test_query(&model, .{}, 8, &.{});
    try test_query(&model, .{}, 0, &.{});
    model = try test_query_model();
    try test_query(&model, .{}, 0, &.{});
    try test_query(&model, .{}, 1, &.{90});
    try test_query(&model, .{}, 2, &.{ 90, 10 });
    try test_query(&model, .{}, 4, &.{ 90, 10, 70, 30 });
    try test_query(&model, .{}, 8, &.{ 90, 10, 70, 30 });
    try test_query(&model, .{ .reversed = true }, 1, &.{30});
    try test_query(&model, .{ .reversed = true }, 2, &.{ 30, 70 });
    try test_query(&model, .{ .reversed = true }, 8, &.{ 30, 70, 10, 90 });
    // Apply the page size after filtering, even when the match occurs late.
    try test_query(&model, .{ .code = 2 }, 1, &.{10});
}

test "state_machine reference model query_transfers imported timestamp ordering" {
    var model = try test_model_with_accounts();
    try expectEqual(.created, (try test_submit(&model, test_transfer(90))).status);
    model.now = 200;

    var imported = test_transfer(10);
    imported.flags.imported = true;
    imported.timestamp = 150;
    try expectEqual(.created, (try test_submit(&model, imported)).status);

    // Imports may predate the clock, but cannot regress behind stored transfers.
    imported.id = 50;
    imported.timestamp = 140;
    try expectEqual(.imported_event_timestamp_must_not_regress, (try test_submit(&model, imported)).status);
    imported.id = 70;
    imported.timestamp = 160;
    try expectEqual(.created, (try test_submit(&model, imported)).status);
    try expectEqual(.created, (try test_submit(&model, test_transfer(30))).status);

    try test_query(&model, .{}, 8, &.{ 90, 10, 70, 30 });
    try test_query(&model, .{ .reversed = true }, 8, &.{ 30, 70, 10, 90 });
    try test_query(&model, .{ .timestamp_min = 150, .timestamp_max = 160 }, 1, &.{10});
    try test_query(&model, .{ .timestamp_min = 150, .timestamp_max = 160, .reversed = true }, 1, &.{70});
}

test "state_machine reference model query_transfers account directions" {
    const model = try test_query_model();
    const Case = struct { filter: TestModel.TransferFilter, ids: []const u128 };
    for ([_]Case{
        .{ .filter = .{ .account_id = 1 }, .ids = &.{ 90, 10, 30 } },
        .{ .filter = .{ .account_id = 1, .credits = false }, .ids = &.{ 90, 30 } },
        .{ .filter = .{ .account_id = 1, .debits = false }, .ids = &.{10} },
        .{ .filter = .{ .account_id = 1, .debits = false, .credits = false }, .ids = &.{} },
        .{ .filter = .{ .account_id = 2, .credits = false }, .ids = &.{10} },
        .{ .filter = .{ .account_id = 2, .debits = false }, .ids = &.{ 90, 30 } },
        .{ .filter = .{ .account_id = 3 }, .ids = &.{70} },
        .{ .filter = .{ .account_id = 4 }, .ids = &.{70} },
        .{ .filter = .{ .account_id = 0 }, .ids = &.{} },
        .{ .filter = .{ .account_id = account_max_id }, .ids = &.{} },
        .{ .filter = .{ .debits = false }, .ids = &.{ 90, 10, 70, 30 } },
        .{ .filter = .{ .credits = false }, .ids = &.{ 90, 10, 70, 30 } },
        .{ .filter = .{ .debits = false, .credits = false }, .ids = &.{ 90, 10, 70, 30 } },
    }) |case| try test_query(&model, case.filter, 8, case.ids);
}

test "state_machine reference model query_transfers metadata filters intersect" {
    const model = try test_query_model();
    const Case = struct { filter: TestModel.TransferFilter, ids: []const u128 };
    for ([_]Case{
        .{ .filter = .{ .user_data_128 = (@as(u128, 1) << 100) + 7 }, .ids = &.{ 90, 10 } },
        .{ .filter = .{ .user_data_64 = (@as(u64, 1) << 40) + 8 }, .ids = &.{ 90, 70 } },
        .{ .filter = .{ .user_data_32 = 9 }, .ids = &.{ 90, 70, 30 } },
        .{ .filter = .{ .ledger = 1 }, .ids = &.{ 90, 10, 30 } },
        .{ .filter = .{ .ledger = 2 }, .ids = &.{70} },
        .{ .filter = .{ .code = 1 }, .ids = &.{ 90, 70 } },
        .{ .filter = .{ .user_data_128 = 7 }, .ids = &.{} },
        .{ .filter = .{ .user_data_64 = 8 }, .ids = &.{} },
        .{ .filter = .{ .user_data_32 = 8 }, .ids = &.{} },
        .{ .filter = .{ .ledger = 3 }, .ids = &.{} },
        .{ .filter = .{ .code = 4 }, .ids = &.{} },
        .{ .filter = .{
            .user_data_128 = (@as(u128, 1) << 100) + 7,
            .user_data_64 = (@as(u64, 1) << 40) + 8,
            .user_data_32 = 9,
            .ledger = 1,
            .code = 1,
        }, .ids = &.{90} },
        // Both fields match individually, but no transfer matches both.
        .{ .filter = .{ .ledger = 2, .code = 2 }, .ids = &.{} },
        .{ .filter = .{ .account_id = 1, .debits = false, .user_data_32 = 9 }, .ids = &.{} },
        .{ .filter = .{
            .account_id = 1,
            .credits = false,
            .user_data_32 = 9,
            .ledger = 1,
            .timestamp_min = 106,
            .timestamp_max = 108,
            .reversed = true,
        }, .ids = &.{30} },
        // Zero metadata fields are wildcards, including records with nonzero values.
        .{ .filter = .{}, .ids = &.{ 90, 10, 70, 30 } },
    }) |case| try test_query(&model, case.filter, 8, case.ids);
}

test "state_machine reference model query_transfers timestamp bounds" {
    var model = try test_query_model();
    const Case = struct { minimum: u64, maximum: u64, ids: []const u128 };
    for ([_]Case{
        .{ .minimum = 0, .maximum = 0, .ids = &.{ 90, 10, 70, 30 } },
        .{ .minimum = 105, .maximum = 108, .ids = &.{ 90, 10, 70, 30 } },
        .{ .minimum = 106, .maximum = 107, .ids = &.{ 10, 70 } },
        .{ .minimum = 106, .maximum = 106, .ids = &.{10} },
        .{ .minimum = 107, .maximum = 0, .ids = &.{ 70, 30 } },
        .{ .minimum = 0, .maximum = 106, .ids = &.{ 90, 10 } },
        .{ .minimum = 0, .maximum = 104, .ids = &.{} },
        .{ .minimum = 109, .maximum = 0, .ids = &.{} },
        .{ .minimum = 108, .maximum = 105, .ids = &.{} },
        .{ .minimum = timestamp_max - 1, .maximum = 0, .ids = &.{} },
        .{ .minimum = 0, .maximum = timestamp_max - 1, .ids = &.{ 90, 10, 70, 30 } },
        .{ .minimum = timestamp_max, .maximum = 0, .ids = &.{} },
        .{ .minimum = timestamp_max + 1, .maximum = 0, .ids = &.{} },
        .{ .minimum = 0, .maximum = timestamp_max, .ids = &.{} },
        .{ .minimum = 0, .maximum = timestamp_max + 1, .ids = &.{} },
    }) |case| {
        const filter: TestModel.TransferFilter = .{ .timestamp_min = case.minimum, .timestamp_max = case.maximum };
        try test_query(&model, filter, 8, case.ids);
    }
    try test_query(&model, .{ .timestamp_min = 106, .timestamp_max = 107, .reversed = true }, 8, &.{ 70, 10 });

    // The largest valid timestamp remains queryable, with inclusive bounds.
    model.now = timestamp_max - 2;
    try expectEqual(.created, (try test_submit(&model, test_transfer(50))).status);
    try test_query(&model, .{ .timestamp_min = timestamp_max - 1, .timestamp_max = timestamp_max - 1 }, 8, &.{50});
}

test "state_machine reference model query_transfers resumable pages" {
    const model = try test_query_model();
    for ([_]bool{ false, true }) |reversed| {
        var filter: TestModel.TransferFilter = .{ .reversed = reversed };
        var page: [2]tb.Transfer = undefined;
        const expected: [4]u128 = if (reversed) .{ 30, 70, 10, 90 } else .{ 90, 10, 70, 30 };
        for (0..2) |page_index| {
            const matches = model.query_transfers(filter, &page);
            try expectEqual(@as(usize, 2), matches.len);
            for (matches, expected[page_index * 2 ..][0..2]) |transfer, id| try expectEqual(id, transfer.id);
            const last = matches[matches.len - 1].timestamp;
            if (reversed) filter.timestamp_max = last - 1 else filter.timestamp_min = last + 1;
        }
        try test_query(&model, filter, 2, &.{});
    }
}

test "state_machine reference model query_transfers includes pending and resolution records only once" {
    var model = try test_model_with_accounts();
    _ = try test_pending(&model);
    try expectEqual(.created, (try test_submit(&model, test_resolution(2, true))).status);
    var pending = test_transfer(3);
    pending.flags.pending = true;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    var resolution = test_resolution(4, false);
    resolution.pending_id = 3;
    try expectEqual(.created, (try test_submit(&model, resolution)).status);
    pending.id = 5;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    // Retries and rejected or rolled-back events must not add query records.
    try expectEqual(.exists, (try test_submit(&model, pending)).status);
    var invalid = test_transfer(6);
    invalid.code = 0;
    var linked = test_transfer(7);
    linked.flags.linked = true;
    var results: [2]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{ linked, invalid }, &results);
    try expectEqual(.linked_event_failed, results[0].status);
    try expectEqual(.code_must_not_be_zero, results[1].status);
    try test_query(&model, .{}, 8, &.{ 1, 2, 3, 4, 5 });
    // Posting inherits metadata, which must also be visible to the query.
    try test_query(&model, .{ .user_data_128 = 128 }, 8, &.{ 1, 2 });
}

test "state_machine reference model pulse expires only due unresolved transfers" {
    var model = try test_model_with_accounts();
    try expectEqual(@as(usize, 0), model.pulse(8));
    var pending = test_transfer(1);
    pending.flags.pending = true;
    pending.timeout = 1;
    const created = try test_submit(&model, pending);
    try expectEqual(.created, created.status);
    pending.id = 2;
    pending.timeout = 0;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    pending.id = 3;
    pending.timeout = 1;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    var resolution = test_resolution(4, true);
    resolution.pending_id = 3;
    try expectEqual(.created, (try test_submit(&model, resolution)).status);
    pending.id = 5;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    resolution = test_resolution(6, false);
    resolution.pending_id = 5;
    try expectEqual(.created, (try test_submit(&model, resolution)).status);
    try expectEqual(.created, (try test_submit(&model, test_transfer(7))).status);

    const before = model;
    model.advance(created.timestamp + 1_000_000_000 - 1 - model.now);
    try expectEqual(@as(usize, 0), model.pulse(8));
    try test_state_equal(&before, &model);
    model.advance(1);
    // Time passing and a zero-sized pulse both leave pending balances intact.
    try expectEqual(@as(usize, 0), model.pulse(0));
    try test_state_equal(&before, &model);
    const pulse_time = model.now;
    try expectEqual(@as(usize, 1), model.pulse(8));
    try expectEqual(pulse_time, model.now);
    try expectEqual(TestModel.PendingState.expired, model.transfers[0].pending_state);
    try expectEqual(TestModel.PendingState.pending, model.transfers[1].pending_state);
    try expectEqual(TestModel.PendingState.posted, model.transfers[2].pending_state);
    try expectEqual(TestModel.PendingState.voided, model.transfers[4].pending_state);
    try expectEqual(@as(u128, 10), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 10), model.accounts[1].credits_pending);
    try expectEqual(@as(u128, 20), model.accounts[0].debits_posted);
    try expectEqual(@as(u128, 20), model.accounts[1].credits_posted);
    try expectEqual(before.transfers_count, model.transfers_count);
    try expectEqual(before.retired_ids_count, model.retired_ids_count);
    try expectEqual(before.journal_count + 1, model.journal_count);
    const event = model.journal[before.journal_count];
    try expectEqual(TestModel.ChangeEventKind.two_phase_expired, event.kind);
    try expectEqual(pulse_time, event.timestamp);
    try std.testing.expectEqualDeep(before.transfers[0].transfer, event.transfer);
    try std.testing.expectEqualDeep(model.accounts[0], event.debit_account);
    try std.testing.expectEqualDeep(model.accounts[1], event.credit_account);
    try std.testing.expectEqualDeep(before.journal[0..before.journal_count], model.journal[0..before.journal_count]);
    try test_transfer_rejected(&model, test_resolution(8, true), .pending_transfer_expired);
    try test_transfer_rejected(&model, test_resolution(8, false), .pending_transfer_expired);
    const after = model;
    // Even after all deadlines pass, resolved and timeout-free records are skipped.
    model.advance(2_000_000_000);
    try expectEqual(@as(usize, 0), model.pulse(8));
    try test_state_equal(&after, &model);
}

test "state_machine reference model pulse limits and deadline ordering" {
    var model = try test_model_with_accounts();
    var pending = test_transfer(1);
    pending.flags.pending = true;
    pending.timeout = 3;
    const first = try test_submit(&model, pending);
    try expectEqual(.created, first.status);
    pending.id = 2;
    pending.amount = 20;
    pending.timeout = 1;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    model.advance(first.timestamp + 1_000_000_000 - 1 - model.now);
    pending.id = 3;
    pending.amount = 30;
    pending.timeout = 2;
    try expectEqual(.created, (try test_submit(&model, pending)).status);
    // Transfers 1 and 3 share a deadline; reverse their physical order to make
    // the creation-time tie breaker observable.
    std.mem.swap(TestModel.TransferEntry, &model.transfers[0], &model.transfers[2]);
    const before = model;
    model.advance(4_000_000_000 - model.now);
    try expectEqual(@as(usize, 1), model.pulse(1));
    try expectEqual(@as(u128, 2), model.journal[3].transfer.id);
    try expectEqual(model.now, model.journal[3].timestamp);
    try expectEqual(@as(u128, 40), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 40), model.accounts[1].credits_pending);
    try expectEqual(TestModel.PendingState.pending, model.transfers[0].pending_state);
    try expectEqual(TestModel.PendingState.pending, model.transfers[2].pending_state);

    model.advance(2);
    try expectEqual(@as(usize, 2), model.pulse(8));
    try expectEqual(@as(u128, 1), model.journal[4].transfer.id);
    try expectEqual(@as(u128, 3), model.journal[5].transfer.id);
    try expectEqual(model.now - 1, model.journal[4].timestamp);
    try expectEqual(model.now, model.journal[5].timestamp);
    for (model.journal[3..6], [_]u128{ 40, 30, 0 }) |event, remaining| {
        try expectEqual(TestModel.ChangeEventKind.two_phase_expired, event.kind);
        try expectEqual(remaining, event.debit_account.debits_pending);
        try expectEqual(remaining, event.credit_account.credits_pending);
    }
    for (model.transfers[0..3]) |entry| try expectEqual(TestModel.PendingState.expired, entry.pending_state);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 0), model.accounts[1].credits_pending);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_posted);
    try expectEqual(@as(u128, 0), model.accounts[1].credits_posted);
    try expectEqual(model.now, model.watermark());
    try expectEqual(@as(usize, 3), model.transfers_count);
    try std.testing.expectEqualDeep(before.journal[0..3], model.journal[0..3]);
    const after = model;
    try expectEqual(@as(usize, 0), model.pulse(8));
    try test_state_equal(&after, &model);
}

test "state_machine reference model pulse reopens closing accounts" {
    for ([_]tb.TransferFlags{
        .{ .pending = true, .closing_debit = true },
        .{ .pending = true, .closing_credit = true },
        .{ .pending = true, .closing_debit = true, .closing_credit = true },
    }) |flags| {
        var model = try test_model_with_accounts();
        var pending = test_transfer(1);
        pending.flags = flags;
        pending.timeout = 1;
        try expectEqual(.created, (try test_submit(&model, pending)).status);
        try expectEqual(flags.closing_debit, model.accounts[0].flags.closed);
        try expectEqual(flags.closing_credit, model.accounts[1].flags.closed);
        model.advance(1_000_000_000);
        try expectEqual(@as(usize, 1), model.pulse(1));
        try expect(!model.accounts[0].flags.closed);
        try expect(!model.accounts[1].flags.closed);
        try expect(!model.journal[1].debit_account.flags.closed);
        try expect(!model.journal[1].credit_account.flags.closed);
        try expectEqual(flags.closing_debit, model.journal[0].debit_account.flags.closed);
        try expectEqual(flags.closing_credit, model.journal[0].credit_account.flags.closed);
        try expectEqual(.created, (try test_submit(&model, test_transfer(2))).status);
    }
}

test "state_machine reference model pulse can fill the journal exactly" {
    var model = StateMachineReferenceType(2, 1){
        .accounts = undefined,
        .transfers = undefined,
        .retired_ids = undefined,
        .now = 100,
    };
    var accounts: [2]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{ test_account(1), test_account(2) }, &accounts);
    for (accounts) |result| try expectEqual(.created, result.status);
    var pending = test_transfer(1);
    pending.flags.pending = true;
    pending.timeout = 1;
    var results: [1]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{pending}, &results);
    try expectEqual(.created, results[0].status);
    model.advance(1_000_000_000);
    try expectEqual(@as(usize, 1), model.pulse(1));
    try expectEqual(model.journal.len, model.journal_count);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 0), model.accounts[1].credits_pending);
    try expectEqual(@as(usize, 0), model.pulse(1));
}

// Adapted from the supplied ledger-model scenarios. The current API advances
// time by a delta, returns a plain count from pulse, and exposes stored records
// directly instead of through lookup methods.
fn test_pulse_scenario_model() !StateMachineReferenceType(2, 8) {
    var model = StateMachineReferenceType(2, 8){
        .accounts = undefined,
        .transfers = undefined,
        .retired_ids = undefined,
        .now = 0,
    };
    var results: [2]tb.CreateAccountResult = undefined;
    try model.create_accounts(&.{ test_account(1), test_account(2) }, &results);
    for (results) |result| try expectEqual(.created, result.status);
    return model;
}

test "state_machine reference model supplied scenario ticks and bounded pulses" {
    var model = try test_pulse_scenario_model();
    var pending = test_transfer(1);
    pending.timeout = 2;
    pending.flags.pending = true;
    var results: [1]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{pending}, &results);
    try expectEqual(.created, results[0].status);
    pending.id = 2;
    pending.timeout = 1;
    try model.create_transfers(&.{pending}, &results);
    try expectEqual(.created, results[0].status);
    model.advance(3_000_000_000 - model.now);
    try expectEqual(@as(u128, 20), model.accounts[0].debits_pending);
    try expectEqual(.pending, model.transfers[1].pending_state);
    var resolution = test_resolution(3, true);
    resolution.pending_id = 2;
    try model.create_transfers(&.{resolution}, &results);
    try expectEqual(.pending_transfer_expired, results[0].status);
    try expectEqual(@as(u128, 20), model.accounts[0].debits_pending);
    try expectEqual(@as(usize, 1), model.pulse(1));
    try expectEqual(.expired, model.transfers[1].pending_state);
    try expectEqual(.pending, model.transfers[0].pending_state);
    try expectEqual(model.now, model.watermark());
    model.advance(1);
    try expectEqual(@as(usize, 1), model.pulse(1));
    try expectEqual(@as(u128, 0), model.accounts[0].debits_pending);
    try expectEqual(@as(usize, 0), model.pulse(1));
}

test "state_machine reference model supplied scenario imports after expiration" {
    var model = try test_pulse_scenario_model();
    var transfer = test_transfer(1);
    transfer.timeout = 1;
    transfer.flags.pending = true;
    var result: [1]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{transfer}, &result);
    try expectEqual(.created, result[0].status);
    const expiry = model.now + 1_000_000_000;
    model.advance(expiry - model.now);
    try expectEqual(@as(usize, 1), model.pulse(8));
    transfer.id = 2;
    transfer.timeout = 0;
    transfer.flags = .{ .imported = true };
    transfer.timestamp = expiry;
    try model.create_transfers(&.{transfer}, &result);
    try expectEqual(.imported_event_timestamp_must_not_regress, result[0].status);
    transfer.timestamp = expiry + 1;
    try model.create_transfers(&.{transfer}, &result);
    try expectEqual(.created, result[0].status);
    try expectEqual(transfer.timestamp, result[0].timestamp);
    try model.create_transfers(&.{transfer}, &result);
    try expectEqual(.exists, result[0].status);
    try expectEqual(transfer.timestamp, result[0].timestamp);
    transfer.id = 3;
    transfer.timestamp = model.now + 1;
    try model.create_transfers(&.{transfer}, &result);
    try expectEqual(.imported_event_timestamp_must_not_advance, result[0].status);
    model.advance(10);
    transfer.timestamp = model.now - 1;
    transfer.flags.linked = true;
    var invalid = transfer;
    invalid.id = 4;
    invalid.timestamp += 1;
    invalid.flags.linked = false;
    invalid.code = 0;
    var linked: [2]tb.CreateTransferResult = undefined;
    const before = model.now;
    try model.create_transfers(&.{ transfer, invalid }, &linked);
    try expectEqual(.linked_event_failed, linked[0].status);
    try expectEqual(transfer.timestamp, linked[0].timestamp);
    try expectEqual(.code_must_not_be_zero, linked[1].status);
    try expectEqual(before + 2, linked[1].timestamp);
    for (model.transfers[0..model.transfers_count]) |entry| try expect(entry.transfer.id != 3);
    try expectEqual(expiry + 1, model.watermark());
    try expectEqual(@as(usize, 3), model.journal_count);
}

test "state_machine reference model supplied scenario linked resolution rollback" {
    var model = try test_pulse_scenario_model();
    var pending = test_transfer(1);
    pending.flags.pending = true;
    var result: [1]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{pending}, &result);
    try expectEqual(.created, result[0].status);
    var missing = test_transfer(3);
    missing.debit_account_id = 42;
    missing.amount = 0;
    var resolution = test_resolution(2, true);
    resolution.flags.linked = true;
    var linked: [2]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{ resolution, missing }, &linked);
    try expectEqual(.linked_event_failed, linked[0].status);
    try expectEqual(.debit_account_not_found, linked[1].status);
    try expectEqual(.pending, model.transfers[0].pending_state);
    try expectEqual(@as(u128, 10), model.accounts[0].debits_pending);
    try expectEqual(@as(u128, 0), model.accounts[0].debits_posted);
    for (model.transfers[0..model.transfers_count]) |entry| try expect(entry.transfer.id != 2);
    try expectEqual(@as(usize, 1), model.journal_count);
    try expectEqual(.two_phase_pending, model.journal[0].kind);
    try model.create_transfers(&.{missing}, &result);
    try expectEqual(.id_already_failed, result[0].status);
}

test "state_machine reference model supplied scenario expiration journal snapshots" {
    var model = try test_pulse_scenario_model();
    var first = test_transfer(1);
    first.timeout = 2;
    first.flags.pending = true;
    var second = first;
    second.id = 2;
    second.amount = 20;
    second.timeout = 1;
    var results: [2]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{ first, second }, &results);
    for (results) |result| try expectEqual(.created, result.status);
    model.advance(3_000_000_000 - model.now);
    try expectEqual(@as(usize, 2), model.pulse(8));
    try expectEqual(@as(u128, 2), model.journal[2].transfer.id);
    try expectEqual(model.now - 1, model.journal[2].timestamp);
    try expectEqual(@as(u128, 10), model.journal[2].debit_account.debits_pending);
    try expectEqual(@as(u128, 10), model.journal[2].credit_account.credits_pending);
    try expectEqual(@as(u128, 1), model.journal[3].transfer.id);
    try expectEqual(@as(u128, 0), model.journal[3].debit_account.debits_pending);
    try expectEqual(@as(u128, 30), model.journal[1].debit_account.debits_pending);
    // The supplied scenario's remaining assertions require changes(), which is
    // not implemented yet. Do not substitute transfer queries for journal pages.
}

test "state_machine reference model supplied scenario query pagination" {
    var model = try test_pulse_scenario_model();
    var first = test_transfer(99);
    first.amount = 0;
    first.user_data_64 = 7;
    var second = first;
    second.id = 1;
    second.debit_account_id = 2;
    second.credit_account_id = 1;
    second.user_data_64 = 8;
    var results: [2]tb.CreateTransferResult = undefined;
    try model.create_transfers(&.{ first, second }, &results);
    for (results) |result| try expectEqual(.created, result.status);
    var page: [1]tb.Transfer = undefined;
    try expectEqual(@as(usize, 1), model.query_transfers(.{}, &page).len);
    try expectEqual(first.id, page[0].id);
    try expectEqual(@as(usize, 1), model.query_transfers(.{ .timestamp_min = page[0].timestamp + 1 }, &page).len);
    try expectEqual(second.id, page[0].id);
    try expectEqual(@as(usize, 1), model.query_transfers(.{ .reversed = true }, &page).len);
    try expectEqual(second.id, page[0].id);
    try expectEqual(@as(usize, 1), model.query_transfers(.{ .account_id = 1, .credits = false }, &page).len);
    try expectEqual(first.id, page[0].id);
    try expectEqual(@as(usize, 1), model.query_transfers(.{ .user_data_64 = 8 }, &page).len);
    try expectEqual(second.id, page[0].id);
    try expectEqual(@as(usize, 0), model.query_transfers(.{ .code = 2 }, &page).len);
}
