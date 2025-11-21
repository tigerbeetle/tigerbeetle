const Docs = @import("../docs_types.zig").Docs;

pub const PythonDocs = Docs{
    .directory = "python",

    .markdown_name = "python",
    .extension = "py",
    .proper_name = "Python",

    .test_source_path = "",

    .name = "tigerbeetle-python",
    .description =
    \\The TigerBeetle client for Python.
    ,
    .prerequisites =
    \\* Python (or PyPy, etc) >= `3.7`
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "main",

    .install_commands = "pip install tigerbeetle",
    .run_commands = "python3 main.py",

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
