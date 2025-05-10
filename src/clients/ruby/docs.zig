const Docs = @import("../docs_types.zig").Docs;

pub const RubyDocs = Docs{
    .directory = "ruby",

    .markdown_name = "ruby",
    .extension = "rb",
    .proper_name = "Ruby",

    .test_source_path = "",

    .name = "tigerbeetle",
    .description =
    \\The TigerBeetle client for Ruby.
    ,
    .prerequisites =
    \\* Ruby >= `3.4`
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "main",

    .install_commands = "gem install tigerbeetle",
    .run_commands = "",

    .examples = "",

    .client_object_documentation = "",
    .create_accounts_documentation = "",
    .account_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`AccountFlags` object (it's an `enum.IntFlag`) with bitwise-or:
    \\
    \\* `AccountFlags.linked`
    \\* `AccountFlags.debits_must_not_exceed_credits`
    \\* `AccountFlags.credits_must_not_exceed_credits`
    \\* `AccountFlags.history`
    \\
    ,

    .create_accounts_errors_documentation =
    \\To handle errors you can compare the result code returned
    \\from `client.create_accounts` with enum values in the
    \\`CreateAccountResult` object.
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_documentation =
    \\To handle errors you can compare the result code returned
    \\from `client.create_transfers` with enum values in the
    \\`CreateTransferResult` object.
    ,

    .transfer_flags_documentation =
    \\To toggle behavior for a transfer, combine enum values stored in the
    \\`TransferFlags` object (it's an `enum.IntFlag`) with bitwise-or:
    \\
    \\* `TransferFlags.linked`
    \\* `TransferFlags.pending`
    \\* `TransferFlags.post_pending_transfer`
    \\* `TransferFlags.void_pending_transfer`
    ,
};
