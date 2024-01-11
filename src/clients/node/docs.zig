const std = @import("std");

const Docs = @import("../docs_types.zig").Docs;

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
    .test_file_name = "main",

    .install_commands = "npm install tigerbeetle-node",
    .run_commands = "node main.js",

    .examples =
    \\### Sidenote: `BigInt`
    \\TigerBeetle uses 64-bit integers for many fields while JavaScript's
    \\builtin `Number` maximum value is `2^53-1`. The `n` suffix in JavaScript
    \\means the value is a `BigInt`. This is useful for literal numbers. If
    \\you already have a `Number` variable though, you can call the `BigInt`
    \\constructor to get a `BigInt` from it. For example, `1n` is the same as
    \\`BigInt(1)`.
    ,

    .client_object_documentation = "",
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

    .create_accounts_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createAccounts` with enum values in the
    \\`CreateAccountError` object, or you can 2) look up the error code in
    \\the `CreateAccountError` object for a human-readable string.
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_documentation =
    \\To handle errors you can either 1) exactly match error codes returned
    \\from `client.createTransfers` with enum values in the
    \\`CreateTransferError` object, or you can 2) look up the error code in
    \\the `CreateTransferError` object for a human-readable string.
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
};
