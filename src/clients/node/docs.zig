const Docs = @import("../docs_types.zig").Docs;

pub const NodeDocs = Docs{
    .readme = "node/README.md",
    .markdown_name = "javascript",
    .extension = "js",

    // Phil: While installing TigerBeetle on Node 14 works, building
    // (`npm install --include dev`) seems to require a newer Node
    // version. I've tried this on alpine and debian images and
    // couldn't get the install to work on either with Node 14.
    .test_linux_docker_image = "node:16-alpine",
    .test_source_path = "",

    .name = "tigerbeetle-node",
    .description = 
    \\The TigerBeetle client for Node.js.
    ,
    .prerequisites = 
    \\* NodeJS >= `14`
    \\
    \\> Windows support is not yet available for the Node client.
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "",

    .install_prereqs = "apk add -U python3",

    .install_sample_file = 
    \\const { createClient } = require("tigerbeetle-node");
    \\console.log("Import ok!");
    ,

    .install_sample_file_build_commands = "npm install typescript @types/node && npx tsc --allowJs --noEmit test.js",
    .install_sample_file_test_commands = "node run test.js",

    .current_commit_pre_install_commands = "",
    .current_commit_post_install_commands = 
    \\npm remove tigerbeetle-node
    \\npm install ./tigerbeetle/src/clients/node/tigerbeetle-node-*.tgz
    ,

    .install_commands = "npm install tigerbeetle-node",
    .install_documentation = 
    \\If you run into issues, check out the distribution-specific install
    \\steps that are run in CI to test support:
    \\
    \\* [Alpine](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_alpine.sh)
    \\* [Amazon Linux](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_amazonlinux.sh)
    \\* [Debian](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_debian.sh)
    \\* [Fedora](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_fedora.sh)
    \\* [Ubuntu](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_ubuntu.sh)
    \\* [RHEL](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/clients/node/scripts/scripts/test_install_on_rhelubi.sh)
    \\
    \\### Sidenote: `BigInt`
    \\TigerBeetle uses 64-bit integers for many fields while JavaScript's
    \\builtin `Number` maximum value is `2^53-1`. The `n` suffix in JavaScript
    \\means the value is a `BigInt`. This is useful for literal numbers. If
    \\you already have a `Number` variable though, you can call the `BigInt`
    \\constructor to get a `BigInt` from it. For example, `1n` is the same as
    \\`BigInt(1)`.
    ,
    .examples = "",

    .client_object_example = 
    \\const client = createClient({
    \\  cluster_id: 0,
    \\  replica_addresses: ['3001', '3002', '3003']
    \\});
    ,
    .client_object_documentation = "",
    .create_accounts_example = 
    \\let account = {
    \\  id: 137n,
    \\  user_data: 0n,
    \\  reserved: Buffer.alloc(48, 0),
    \\  ledger: 1,
    \\  code: 718,
    \\  flags: 0,
    \\  debits_pending: 0n,
    \\  debits_posted: 0n,
    \\  credits_pending: 0n,
    \\  credits_posted: 0n,
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
    \\let account0 = { /* ... account values ... */ };
    \\let account1 = { /* ... account values ... */ };
    \\account0.flags = AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits;
    \\accountErrors = await client.createAccounts([account0, account1]);
    ,
    .create_accounts_errors_example = 
    \\let account2 = { /* ... account values ... */ };
    \\let account3 = { /* ... account values ... */ };
    \\let account4 = { /* ... account values ... */ };
    \\accountErrors = await client.createAccounts([account2, account3, account4]);
    \\for (const error of accountErrors) {
    \\  switch (error.code) {
    \\    case CreateAccountError.exists:
    \\      console.error(`Batch account at ${error.index} already exists.`);
    \\	  break;
    \\    default:
    \\      console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.code]}.`);
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
    \\ *   user_data: 0n,
    \\ *   reserved: Buffer,
    \\ *   ledger: 1,
    \\ *   code: 718,
    \\ *   flags: 0,
    \\ *   debits_pending: 0n,
    \\ *   debits_posted: 0n,
    \\ *   credits_pending: 0n,
    \\ *   credits_posted: 0n,
    \\ *   timestamp: 1623062009212508993n,
    \\ * }]
    \\ */
    ,

    .create_transfers_example = 
    \\let transfer = {
    \\  id: 1n,
    \\  pending_id: 0n,
    \\  debit_account_id: 1n,
    \\  credit_account_id: 2n,
    \\  user_data: 0n,
    \\  reserved: 0n,
    \\  timeout: 0n,
    \\  ledger: 1,
    \\  code: 720,
    \\  flags: 0,
    \\  amount: 10n,
    \\  timestamp: 0n,
    \\};
    \\let transferErrors = await client.createTransfers([transfer]);
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_example = 
    \\for (const error of transferErrors) {
    \\  switch (error.code) {
    \\    case CreateTransferError.exists:
    \\      console.error(`Batch transfer at ${error.index} already exists.`);
    \\	  break;
    \\    default:
    \\      console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.code]}.`);
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
    \\const BATCH_SIZE = 8191;
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
    \\transfer0 = { /* ... transfer values ... */ };
    \\transfer1 = { /* ... transfer values ... */ };
    \\transfer0.flags = TransferFlags.linked;
    \\// Create the transfer
    \\transferErrors = await client.createTransfers([transfer0, transfer1]);
    ,
    .transfer_flags_post_example = 
    \\transfer = {
    \\  id: 2n,
    \\  pending_id: 1n,
    \\  flags: TransferFlags.post_pending_transfer,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer]);
    ,
    .transfer_flags_void_example = 
    \\transfer = {
    \\  id: 2n,
    \\  pending_id: 1n,
    \\  flags: TransferFlags.void_pending_transfer,
    \\  timestamp: 0n,
    \\};
    \\transferErrors = await client.createTransfers([transfer]);
    ,

    .lookup_transfers_example = 
    \\const transfers = await client.lookupTransfers([1n, 2n]);
    \\console.log(transfers);
    \\/*
    \\ * [{
    \\ *   id: 1n,
    \\ *   pending_id: 0n,
    \\ *   debit_account_id: 1n,
    \\ *   credit_account_id: 2n,
    \\ *   user_data: 0n,
    \\ *   reserved: 0n,
    \\ *   timeout: 0n,
    \\ *   ledger: 1,
    \\ *   code: 720,
    \\ *   flags: 0,
    \\ *   amount: 10n,
    \\ *   timestamp: 1623062009212508993n,
    \\ * }]
    \\ */
    ,

    .linked_events_example = 
    \\const batch = [];
    \\let linkedFlag = 0;
    \\linkedFlag |= CreateTransferFlags.linked;
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

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_sh_commands = 
    \\git clone https://github.com/${GITHUB_REPOSITY:-tigerbeetledb/tigerbeetle}
    \\cd tigerbeetle
    \\git checkout $GIT_SHA
    \\./scripts/install_zig.sh
    \\cd src/clients/node
    \\npm install --include dev
    \\npm pack
    \\[ "$TEST" = "true" ] && mvn test || echo "Skipping client unit tests"
    ,

    // Extra steps to determine commit and repo so this works in
    // CI against forks and pull requests.
    .developer_setup_pwsh_commands = "",
    .test_main_prefix = 
    \\const { createClient } = require("tigerbeetle-node");
    \\
    \\async function main() {
    ,
    .test_main_suffix = 
    \\}
    \\main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
    ,
};
