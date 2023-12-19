const std = @import("std");

const Docs = @import("../docs_types.zig").Docs;
const run = @import("../shutil.zig").run;

// Caller is responsible for resetting to a good cwd after this completes.
fn find_node_client_tar(arena: *std.heap.ArenaAllocator, root: []const u8) ![]const u8 {
    var tries: usize = 2;
    while (tries > 0) {
        try std.os.chdir(root);

        const node_dir = try std.fs.cwd().realpathAlloc(arena.allocator(), "src/clients/node");

        var dir = try std.fs.cwd().openIterableDir(node_dir, .{});
        defer dir.close();

        var walker = try dir.walk(arena.allocator());
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (std.mem.startsWith(u8, entry.path, "tigerbeetle-node-") and std.mem.endsWith(u8, entry.path, ".tgz")) {
                return std.fmt.allocPrint(
                    arena.allocator(),
                    "{s}/{s}",
                    .{ node_dir, entry.path },
                );
            }
        }

        try std.os.chdir(node_dir);
        try run(arena, &[_][]const u8{ "npm", "install" });
        try run(arena, &[_][]const u8{ "npm", "pack" });
        tries -= 1;
    }

    std.debug.print("Could not find src/clients/node/tigerbeetle-node-*.tgz, run npm install && npm pack in src/clients/node\n", .{});
    return error.PackageNotFound;
}

fn node_current_commit_post_install_hook(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    root: []const u8,
) !void {
    const package = try find_node_client_tar(arena, root);

    try std.os.chdir(sample_dir);

    // Swap out the normal tigerbeetle-node with our local version.
    try run(arena, &[_][]const u8{
        "npm",
        "uninstall",
        "tigerbeetle-node",
    });
    try run(arena, &[_][]const u8{
        "npm",
        "install",
        package,
    });
}

pub const NodeDocs = Docs{
    .directory = "node",

    .markdown_name = "javascript",
    .extension = "js",
    .proper_name = "Node.js",

    .test_source_path = "",

    .name = "tigerbeetle-node",
    .description =
    \\The TigerBeetle client for Node.js.
    ,
    .prerequisites =
    \\* NodeJS >= `14`
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "",

    .install_prereqs = "apk add -U python3",

    .install_sample_file =
    \\const { createClient } = require("tigerbeetle-node");
    \\console.log("Import ok!");
    ,

    .install_commands = "npm install tigerbeetle-node",
    .build_commands =
    \\npm install typescript @types/node
    \\npx tsc --allowJs --noEmit main.js
    ,
    .run_commands = "node main.js",

    .current_commit_install_commands_hook = null,
    .current_commit_build_commands_hook = null,
    .current_commit_run_commands_hook = null,

    .current_commit_pre_install_hook = null,
    .current_commit_post_install_hook = node_current_commit_post_install_hook,

    .install_documentation = "",

    .examples =
    \\### Sidenote: `BigInt`
    \\TigerBeetle uses 64-bit integers for many fields while JavaScript's
    \\builtin `Number` maximum value is `2^53-1`. The `n` suffix in JavaScript
    \\means the value is a `BigInt`. This is useful for literal numbers. If
    \\you already have a `Number` variable though, you can call the `BigInt`
    \\constructor to get a `BigInt` from it. For example, `1n` is the same as
    \\`BigInt(1)`.
    ,

    .client_object_example =
    \\const client = createClient({
    \\  cluster_id: 0n,
    \\  replica_addresses: [process.env.TB_ADDRESS || '3000']
    \\});
    ,
    .client_object_documentation = "",
    .create_accounts_example =
    \\let account = {
    \\  id: 137n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 718,
    \\  flags: 0,
    \\  timestamp: 0n,
    \\};
    \\
    \\let accountErrors = await client.createAccounts([account]);
    ,
    .create_accounts_documentation = "",
    .account_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`AccountFlags` object (in TypeScript it is an actual enum) with
    \\bitwise-or:
    \\
    \\* `AccountFlags.linked`
    \\* `AccountFlags.debits_must_not_exceed_credits`
    \\* `AccountFlags.credits_must_not_exceed_credits`
    \\
    ,

    .account_flags_example =
    \\let account0 = {
    \\  id: 100n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 1,
    \\  timestamp: 0n,
    \\  flags: 0,
    \\};
    \\let account1 = {
    \\  id: 101n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 1,
    \\  timestamp: 0n,
    \\  flags: 0,
    \\};
    \\account0.flags = AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits;
    \\accountErrors = await client.createAccounts([account0, account1]);
    ,
    .create_accounts_errors_example =
    \\let account2 = {
    \\  id: 102n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 1,
    \\  timestamp: 0n,
    \\  flags: 0,
    \\};
    \\let account3 = {
    \\  id: 103n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 1,
    \\  timestamp: 0n,
    \\  flags: 0,
    \\};
    \\let account4 = {
    \\  id: 104n,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  reserved: 0,
    \\  ledger: 1,
    \\  code: 1,
    \\  timestamp: 0n,
    \\  flags: 0,
    \\};
    \\accountErrors = await client.createAccounts([account2, account3, account4]);
    \\for (const error of accountErrors) {
    \\  switch (error.result) {
    \\    case CreateAccountError.exists:
    \\      console.error(`Batch account at ${error.index} already exists.`);
    \\	  break;
    \\    default:
    \\      console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`);
    \\  }
    \\}
    ,
    .create_accounts_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createAccounts` with enum values in the
    \\`CreateAccountError` object, or you can 2) look up the error code in
    \\the `CreateAccountError` object for a human-readable string.
    ,
    .lookup_accounts_example =
    \\const accounts = await client.lookupAccounts([137n, 138n]);
    \\console.log(accounts);
    \\/*
    \\ * [{
    \\ *   id: 137n,
    \\ *   debits_pending: 0n,
    \\ *   debits_posted: 0n,
    \\ *   credits_pending: 0n,
    \\ *   credits_posted: 0n,
    \\ *   user_data_128: 0n,
    \\ *   user_data_64: 0n,
    \\ *   user_data_32: 0,
    \\ *   reserved: 0,
    \\ *   ledger: 1,
    \\ *   code: 718,
    \\ *   flags: 0,
    \\ *   timestamp: 1623062009212508993n,
    \\ * }]
    \\ */
    ,

    .create_transfers_example =
    \\let transfer = {
    \\  id: 1n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: 0,
    \\  timestamp: 0n,
    \\};
    \\let transferErrors = await client.createTransfers([transfer]);
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_example =
    \\for (const error of transferErrors) {
    \\  switch (error.result) {
    \\    case CreateTransferError.exists:
    \\      console.error(`Batch transfer at ${error.index} already exists.`);
    \\	  break;
    \\    default:
    \\      console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
    \\  }
    \\}
    ,
    .create_transfers_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createTransfers` with enum values in the
    \\`CreateTransferError` object, or you can 2) look up the error code in
    \\the `CreateTransferError` object for a human-readable string.
    ,

    .no_batch_example =
    \\for (let i = 0; i < transfers.len; i++) {
    \\  const transferErrors = await client.createTransfers(transfers[i]);
    \\  // error handling omitted
    \\}
    ,

    .batch_example =
    \\const BATCH_SIZE = 8190;
    \\for (let i = 0; i < transfers.length; i += BATCH_SIZE) {
    \\  const transferErrors = await client.createTransfers(transfers.slice(i, Math.min(transfers.length, BATCH_SIZE)));
    \\  // error handling omitted
    \\}
    ,

    .transfer_flags_documentation =
    \\To toggle behavior for a transfer, combine enum values stored in the
    \\`TransferFlags` object (in TypeScript it is an actual enum) with
    \\bitwise-or:
    \\
    \\* `TransferFlags.linked`
    \\* `TransferFlags.pending`
    \\* `TransferFlags.post_pending_transfer`
    \\* `TransferFlags.void_pending_transfer`
    ,
    .transfer_flags_link_example =
    \\let transfer0 = {
    \\  id: 2n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: 0,
    \\  timestamp: 0n,
    \\};
    \\let transfer1 = {
    \\  id: 3n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: 0,
    \\  timestamp: 0n,
    \\};
    \\transfer0.flags = TransferFlags.linked;
    \\// Create the transfer
    \\transferErrors = await client.createTransfers([transfer0, transfer1]);
    ,
    .transfer_flags_post_example =
    \\let transfer2 = {
    \\  id: 4n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: TransferFlags.pending,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer2]);
    \\
    \\let transfer3 = {
    \\  id: 5n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 4n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: TransferFlags.post_pending_transfer,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer3]);
    ,
    .transfer_flags_void_example =
    \\let transfer4 = {
    \\  id: 4n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 0n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: TransferFlags.pending,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer4]);
    \\
    \\let transfer5 = {
    \\  id: 7n,
    \\  debit_account_id: 102n,
    \\  credit_account_id: 103n,
    \\  amount: 10n,
    \\  pending_id: 6n,
    \\  user_data_128: 0n,
    \\  user_data_64: 0n,
    \\  user_data_32: 0,
    \\  timeout: 0,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: TransferFlags.void_pending_transfer,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer5]);
    ,

    .lookup_transfers_example =
    \\const transfers = await client.lookupTransfers([1n, 2n]);
    \\console.log(transfers);
    \\/*
    \\ * [{
    \\ *   id: 1n,
    \\ *   debit_account_id: 102n,
    \\ *   credit_account_id: 103n,
    \\ *   amount: 10n,
    \\ *   pending_id: 0n,
    \\ *   user_data_128: 0n,
    \\ *   user_data_64: 0n,
    \\ *   user_data_32: 0,
    \\ *   timeout: 0,
    \\ *   ledger: 1,
    \\ *   code: 720,
    \\ *   flags: 0,
    \\ *   timestamp: 1623062009212508993n,
    \\ * }]
    \\ */
    ,

    .get_account_transfers_example =
    \\let filter = {
    \\  account_id: 2n,
    \\  timestamp: 0n, // No filter by Timestamp.
    \\  limit: 10, // Limit to ten transfers at most.
    \\  flags: GetAccountTransfersFlags.debits | // Include transfer from the debit side.
    \\    GetAccountTransfersFlags.credits | // Include transfer from the credit side.
    \\    GetAccountTransfersFlags.reversed, // Sort by timestamp in reverse-chronological order.
    \\}
    \\const account_transfers = await client.getAccountTransfers(filter)
    ,

    .linked_events_example =
    \\const batch = [];
    \\let linkedFlag = 0;
    \\linkedFlag |= TransferFlags.linked;
    \\
    \\// An individual transfer (successful):
    \\batch.push({ id: 1n /* , ... */ });
    \\
    \\// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
    \\batch.push({ id: 2n, /* ..., */ flags: linkedFlag }); // Commit/rollback.
    \\batch.push({ id: 3n, /* ..., */ flags: linkedFlag }); // Commit/rollback.
    \\batch.push({ id: 2n, /* ..., */ flags: linkedFlag }); // Fail with exists
    \\batch.push({ id: 4n, /* ..., */ flags: 0 });          // Fail without committing.
    \\
    \\// An individual transfer (successful):
    \\// This should not see any effect from the failed chain above.
    \\batch.push({ id: 2n, /* ..., */ flags: 0 });
    \\
    \\// A chain of 2 transfers (the first transfer fails the chain):
    \\batch.push({ id: 2n, /* ..., */ flags: linkedFlag });
    \\batch.push({ id: 3n, /* ..., */ flags: 0 });
    \\
    \\// A chain of 2 transfers (successful):
    \\batch.push({ id: 3n, /* ..., */ flags: linkedFlag });
    \\batch.push({ id: 4n, /* ..., */ flags: 0 });
    \\
    \\const errors = await client.createTransfers(batch);
    \\
    \\/**
    \\ * console.log(errors);
    \\ * [
    \\ *  { index: 1, error: 1 },  // linked_event_failed
    \\ *  { index: 2, error: 1 },  // linked_event_failed
    \\ *  { index: 3, error: 25 }, // exists
    \\ *  { index: 4, error: 1 },  // linked_event_failed
    \\ *
    \\ *  { index: 6, error: 17 }, // exists_with_different_flags
    \\ *  { index: 7, error: 1 },  // linked_event_failed
    \\ * ]
    \\ */
    ,

    .developer_setup_documentation = "",

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_sh_commands =
    \\cd src/clients/node
    \\npm install --include dev
    \\npm pack
    ,

    // TODO(phil): node tests are the only ones that expect to have a TigerBeetle instance running.
    // From what I can tell they were never running in CI since I was the first person to add anything Node to CI.
    // Soon what it tests will be replaced with sample code that is integration tested anyway.
    // \\if [ "$TEST" = "true" ]; then npm test; else echo "Skipping client unit tests"; fi

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_pwsh_commands =
    \\cd src/clients/node
    \\npm install --include dev
    \\npm pack
    ,
    .test_main_prefix =
    \\const {
    \\  createClient,
    \\  AccountFlags,
    \\  TransferFlags,
    \\  CreateTransferError,
    \\  CreateAccountError,
    \\} = require("tigerbeetle-node");
    \\
    \\async function main() {
    ,
    .test_main_suffix =
    \\}
    \\main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
    ,
};
