const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.test_workload);

const vsr = @import("vsr");
const tigerbeetle = @import("../../tigerbeetle.zig");

const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const constants = vsr.constants;
const Message = vsr.message_pool.MessagePool.Message;

const Client = vsr.Client(StateMachine, MessageBus);
const Account = tigerbeetle.Account;
const Transfer = tigerbeetle.Transfer;
const CreateAccountsResult = tigerbeetle.CreateAccountsResult;
const CreateTransfersResult = tigerbeetle.CreateTransfersResult;
const Operation = StateMachine.Operation;

const send_message_retry_timeout = 200; // TODO what is a reasonable value?
const requests_max = constants.journal_slot_count * 9 / 10;

pub const std_options = struct {
    pub const log_level: std.log.Level = .info;
};

// Always use the OS's random calls directly; this allows Antithesis to fuzz them.
// Applies to std.crypto.random
pub const crypto_always_getrandom = true;

pub fn main() anyerror!void {
    const ticks_max = 1_000_000;
    const accounts_count = 8; // TODO fuzz_get_random()
    assert(accounts_count >= 2);

    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = general_purpose_allocator.allocator();

    const random = std.crypto.random;
    var random_for_uuid = std.rand.DefaultPrng.init(123);
    const random_uuid = random_for_uuid.random();

    // CLI arguments.

    const sleep_seconds_arg = std.os.getenv("SLEEP_SECONDS") orelse fatal("missing SLEEP_SECONDS", .{});
    const cluster_arg = std.os.getenv("CLUSTER") orelse fatal("missing CLUSTER", .{});
    const api_addresses_arg = std.os.getenv("APIS") orelse fatal("missing APIS", .{});

    const sleep_seconds = try std.fmt.parseUnsigned(u64, sleep_seconds_arg, 10);
    const cluster = try std.fmt.parseUnsigned(u32, cluster_arg, 10);

    const api_addresses = try vsr.parse_addresses(allocator, api_addresses_arg, constants.replicas_max);
    defer allocator.free(api_addresses);

    {
        // Recommendation from Connor(Antithesis):
        // Delay execution of the workload until the fault injector is ready.

        // This log line tells Antithesis to begin fault injection.
        log.info("antithesis: start_faults", .{});
        log.info("sleep seconds={}", .{sleep_seconds});
        std.os.nanosleep(sleep_seconds, 0);
    }

    // Setup clients.

    var io = try IO.init(32, 0);
    defer io.deinit();

    var clients = try allocator.alloc(Client, api_addresses.len);
    defer allocator.free(clients);

    var pools = try allocator.alloc(MessagePool, api_addresses.len);
    defer allocator.free(pools);

    for (clients, pools, 0..) |*client, *pool, i| {
        pool.* = try MessagePool.init(allocator, .client);
        const client_id = std.crypto.random.int(u128);
        const address = api_addresses[i .. i + 1];
        std.log.info("Creating client {} pointing to {}", .{ i, address[0] });
        client.* = try Client.init(
            allocator,
            client_id,
            cluster,
            1,
            pool,
            .{
                .configuration = address,
                .io = &io,
            },
        );
    }
    defer for (clients) |*client| client.deinit(allocator);

    // Run workload.

    var workload = try Workload.init(allocator, .{
        .random = random,
        .random_uuid = random_uuid,
        .cluster = cluster,
        .accounts_count = accounts_count,
    });
    defer workload.deinit(allocator);

    var context = try Context.init(allocator, &workload, clients);
    defer context.deinit(allocator);

    log.info("workload starting accounts={} requests={}", .{
        accounts_count,
        requests_max,
    });
    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        if (context.done()) break;
        try context.tick();
        io.tick() catch |err| log.warn("io.tick error={s}", .{@errorName(err)});
        std.time.sleep(constants.tick_ms * std.time.ns_per_ms);
    } else fatal("tick == ticks_max", .{});
    log.info("workload done tick={}", .{tick});
}

fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.os.exit(1);
}

const Request = struct {
    context: *Context,
    client_index: usize,
    client: *Client,
    message: *Message,
};

const Context = struct {
    workload: *Workload,
    clients: []Client,
    requests: []?Request,

    /// It is safe to use a shared id for all clients, because each of the workload's clients
    /// connects to exactly one API service.
    const client_id: u128 = 123;

    fn init(allocator: std.mem.Allocator, workload: *Workload, clients: []Client) !Context {
        var requests = try allocator.alloc(?Request, clients.len);
        errdefer allocator.free(requests);
        @memset(requests, null);

        return Context{
            .workload = workload,
            .clients = clients,
            .requests = requests,
        };
    }

    fn deinit(self: *Context, allocator: std.mem.Allocator) void {
        allocator.free(self.requests);
    }

    fn tick(self: *Context) !void {
        for (self.clients, 0..) |*client, i| {
            client.tick();
            if (self.requests[i] != null) {
                // A request to this client is already pending; keep waiting for a response.
                continue;
            }

            var request = client.get_message();
            defer client.unref(request);

            // `size` and `operation` will be overwritten by `build_request`.
            request.header.* = .{
                .client = client.id,
                .cluster = self.workload.cluster,
                .command = .request,
            };

            if (!self.done_sending()) {
                self.workload.build_request(request);
            } else {
                if (self.done_receiving() and !self.done_verifying()) {
                    // Don't start verification until all transfers have finished.
                    self.workload.build_lookup(request);
                }
            }

            if (request.header.operation == .reserved) {
                // No request to send right now.
                assert(request.header.size == @sizeOf(vsr.Header));
            } else {
                assert(request.header.size > @sizeOf(vsr.Header));
                self.send_request_to_client(i, request);
            }
        }
    }

    fn done(self: *const Context) bool {
        return self.done_sending() and self.done_receiving() and self.done_verifying();
    }

    fn done_sending(self: *const Context) bool {
        // TODO Once the storage engine is merged, change this to just counting the number of
        // requests sent (which is simpler), since the total number of commits won't matter.
        // This is already fuzzy since after `done_sending`, we still need to send the lookups
        // for verification.
        for (self.workload.accounts_created) |created| {
            if (!created) return false;
        }

        var requests_sent: usize = 0;
        for (self.clients) |*client| {
            requests_sent += client.request_number;
        }
        return requests_sent >= requests_max;
    }

    fn done_receiving(self: *const Context) bool {
        for (self.requests) |*request| {
            if (request.*) |_| return false;
        } else return true;
    }

    fn done_verifying(self: *const Context) bool {
        for (self.workload.accounts_verified) |verified| {
            if (!verified) return false;
        } else return true;
    }

    fn send_request_to_client(self: *Context, client_index: usize, message: *Message) void {
        assert(message.header.command == .request);
        assert(self.requests[client_index] == null);

        self.requests[client_index] = .{
            .context = self,
            .client_index = client_index,
            .client = &self.clients[client_index],
            .message = message.ref(),
        };

        self.clients[client_index].request(
            @as(u128, @intCast(@intFromPtr(&self.requests[client_index].?))),
            client_request_callback,
            message.header.operation.cast(StateMachine),
            message,
            message.header.size - @sizeOf(vsr.Header),
        );
    }

    fn client_request_callback(
        user_data: u128,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        const request = @as(*Request, @ptrFromInt(@as(usize, @intCast(user_data))));
        const self = request.context;

        defer {
            request.client.unref(request.message);
            assert(&self.requests[request.client_index].? == request);
            self.requests[request.client_index] = null;
        }

        switch (operation) {
            .create_accounts => self.workload.on_create_accounts(request.message, std.mem.bytesAsSlice(CreateAccountsResult, result)),
            .create_transfers => self.workload.on_create_transfers(request.message, std.mem.bytesAsSlice(CreateTransfersResult, result)),
            .lookup_accounts => self.workload.on_lookup_accounts(request.message, std.mem.bytesAsSlice(Account, result)),
            .lookup_transfers => self.workload.on_lookup_transfers(request.message, std.mem.bytesAsSlice(Transfer, result)),
        }
    }
};

const Workload = struct {
    random: std.rand.Random,
    /// Use a separate RNG for UUID generation, since there's no point fuzzing different UUIDs.
    random_uuid: std.rand.Random,

    cluster: u32,
    accounts: []Account,
    accounts_created: []bool,
    /// Track which accounts' balances have been verified.
    /// (Verification is after the workload is done transferring money between accounts).
    accounts_verified: []bool,

    const ledger: u32 = 1234;
    const code: u16 = 5678;

    fn init(allocator: std.mem.Allocator, options: struct {
        random: std.rand.Random,
        random_uuid: std.rand.Random,
        cluster: u32,
        accounts_count: usize,
    }) !Workload {
        assert(options.accounts_count >= 2);

        var accounts = try allocator.alloc(Account, options.accounts_count);
        errdefer allocator.free(accounts);

        var accounts_created = try allocator.alloc(bool, options.accounts_count);
        errdefer allocator.free(accounts_created);

        var accounts_verified = try allocator.alloc(bool, options.accounts_count);
        errdefer allocator.free(accounts_verified);

        for (accounts, 0..) |*account, i| {
            account.* = std.mem.zeroInit(Account, .{
                .id = index_to_id(i),
                .ledger = ledger,
                .code = code,
            });
        }
        @memset(accounts_created, false);
        @memset(accounts_verified, false);

        return Workload{
            .random = options.random,
            .random_uuid = options.random_uuid,
            .cluster = options.cluster,
            .accounts = accounts,
            .accounts_created = accounts_created,
            .accounts_verified = accounts_verified,
        };
    }

    fn deinit(self: *Workload, allocator: std.mem.Allocator) void {
        allocator.free(self.accounts);
        allocator.free(self.accounts_created);
        allocator.free(self.accounts_verified);
    }

    // TODO test two-phase transfers
    fn build_request(self: *Workload, request: *Message) void {
        assert(request.header.operation == .reserved);
        assert(self.accounts.len >= 2);

        const account_src = self.random.uintLessThan(usize, self.accounts.len);
        const account_dst = self.random.uintLessThan(usize, self.accounts.len);
        if (account_dst == account_src) return;

        if (!self.accounts_created[account_src]) {
            self.request_create_accounts(request, account_src);
        } else if (!self.accounts_created[account_dst]) {
            self.request_create_accounts(request, account_dst);
        } else {
            self.request_create_transfers(request, account_src, account_dst);
        }

        // No request to send this tick.
        if (request.header.operation == .reserved) return;
        assert(request.header.size != 0);
    }

    fn build_lookup(self: *Workload, request: *Message) void {
        assert(request.header.operation == .reserved);

        const account = for (self.accounts, 0..) |*account, i| {
            assert(self.accounts_created[i]);
            if (!self.accounts_verified[i]) break account;
        } else unreachable;
        log.info("build_lookup verify account={}", .{account.id});

        const lookup_bytes = std.mem.sliceAsBytes(&[_]u128{account.id});
        std.mem.copy(u8, request.buffer[@sizeOf(vsr.Header)..], lookup_bytes);
        request.header.operation = vsr.Operation.from(StateMachine, .lookup_accounts);
        request.header.size = @as(u32, @intCast(@sizeOf(vsr.Header) + lookup_bytes.len));
    }

    fn request_create_accounts(
        self: *const Workload,
        request: *Message,
        account_index: usize,
    ) void {
        assert(!self.accounts_created[account_index]);
        assert(request.header.operation == .reserved);

        const account = &self.accounts[account_index];
        assert(account.id == index_to_id(account_index));

        const account_bytes = std.mem.asBytes(account);
        log.info("request_create_accounts account={}", .{account.id});

        std.mem.copy(u8, request.buffer[@sizeOf(vsr.Header)..], account_bytes);
        request.header.operation = vsr.Operation.from(StateMachine, .create_accounts);
        request.header.size = @sizeOf(vsr.Header) + account_bytes.len;
    }

    fn request_create_transfers(
        self: *const Workload,
        request: *Message,
        account_src: usize,
        account_dst: usize,
    ) void {
        assert(self.accounts_created[account_src]);
        assert(self.accounts_created[account_dst]);
        assert(request.header.operation == .reserved);

        const transfer = Transfer{
            .id = self.random_uuid.int(u128),
            .debit_account_id = index_to_id(account_src),
            .credit_account_id = index_to_id(account_dst),
            .user_data_32 = 0,
            .user_data_64 = 0,
            .user_data_128 = 0,
            .pending_id = 0,
            .timeout = 0,
            .ledger = ledger,
            .code = code,
            .flags = .{},
            .amount = self.random.int(u8),
        };
        const transfer_bytes = std.mem.asBytes(&transfer);
        log.info("request_create_transfers account_src={} account_dst={} amount={}\n", .{
            account_src,
            account_dst,
            transfer.amount,
        });

        std.mem.copy(u8, request.buffer[@sizeOf(vsr.Header)..], transfer_bytes);
        request.header.operation = vsr.Operation.from(StateMachine, .create_transfers);
        request.header.size = @sizeOf(vsr.Header) + transfer_bytes.len;

        self.accounts[account_src].debits_posted += transfer.amount;
        self.accounts[account_dst].credits_posted += transfer.amount;
    }

    fn on_create_accounts(self: *Workload, request: *const Message, results: []align(1) const CreateAccountsResult) void {
        const request_body = request.buffer[@sizeOf(vsr.Header)..request.header.size];
        const request_accounts = std.mem.bytesAsSlice(Account, request_body);
        assert(request_accounts.len >= results.len);

        for (request_accounts, 0..) |account, i| {
            for (results) |result| {
                if (result.index == i) {
                    log.warn("on_create_accounts error account={} result={}", .{
                        account.id,
                        result.result,
                    });
                    break;
                }
            } else {
                log.info("on_create_accounts account={}", .{account.id});
                self.accounts_created[id_to_index(account.id)] = true;
            }
        }
    }

    fn on_create_transfers(self: *Workload, request: *const Message, results: []align(1) const CreateTransfersResult) void {
        _ = self;
        const request_body = request.buffer[@sizeOf(vsr.Header)..request.header.size];
        const request_transfers = std.mem.bytesAsSlice(Transfer, request_body);
        assert(request_transfers.len >= results.len);

        for (request_transfers, 0..) |transfer, i| {
            for (results) |result| {
                if (result.index == i) {
                    log.warn("on_create_transfers error transfer={} result={}", .{
                        transfer.id,
                        result.result,
                    });
                    break;
                }
            } else {
                log.info("on_create_transfers ok transfer={}", .{transfer.id});
            }
        }
    }

    fn on_lookup_accounts(self: *Workload, request: *const Message, results: []align(1) const Account) void {
        const request_body = request.buffer[@sizeOf(vsr.Header)..request.header.size];
        const request_account_ids = std.mem.bytesAsSlice(u128, request_body);
        assert(request_account_ids.len == results.len);

        for (request_account_ids, 0..) |request_account_id, i| {
            const account_expect = &self.accounts[id_to_index(request_account_id)];
            const account_actual = &results[i];
            log.info("on_lookup_accounts expect {}", .{account_expect});
            log.info("on_lookup_accounts actual {}", .{account_actual});

            assert(account_expect.id == account_actual.id);
            assert(account_expect.user_data_128 == account_actual.user_data_128);
            assert(account_expect.ledger == account_actual.ledger);
            assert(account_expect.code == account_actual.code);
            assert(account_expect.flags.linked == account_actual.flags.linked);
            assert(account_expect.flags.debits_must_not_exceed_credits ==
                account_actual.flags.debits_must_not_exceed_credits);
            assert(account_expect.flags.credits_must_not_exceed_debits ==
                account_actual.flags.credits_must_not_exceed_debits);
            assert(account_expect.debits_pending == account_actual.debits_pending);
            assert(account_expect.debits_posted == account_actual.debits_posted);
            assert(account_expect.credits_pending == account_actual.credits_pending);
            assert(account_expect.credits_posted == account_actual.credits_posted);

            self.accounts_verified[id_to_index(request_account_id)] = true;
            log.info("on_lookup_accounts verified account={}", .{request_account_id});
        }
    }

    fn on_lookup_transfers(self: *Workload, request: *const Message, results: []align(1) const Transfer) void {
        // TODO
        _ = self;
        _ = request;
        _ = results;
        unreachable;
    }
};

fn id_to_index(id: u128) usize {
    return @as(usize, @intCast(id - 1));
}

fn index_to_id(index: usize) u128 {
    return index + 1;
}
