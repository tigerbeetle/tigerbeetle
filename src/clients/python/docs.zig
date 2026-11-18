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
    \\`AccountFlags` object (it's an `enum.IntFlag`) with bitwise-or. Here are a
    \\few examples:
    \\
    \\* `AccountFlags.LINKED`
    \\* `AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS`
    \\* `AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS`
    \\* `AccountFlags.HISTORY`
    \\
    ,

    .create_accounts_errors_documentation =
    \\To handle errors you can compare the result code returned
    \\from `client.create_accounts` with enum values in the
    \\`CreateAccountStatus` object.
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_documentation =
    \\To handle errors you can compare the result code returned
    \\from `client.create_transfers` with enum values in the
    \\`CreateTransferStatus` object.
    ,

    .transfer_flags_documentation =
    \\To toggle behavior for a transfer, combine enum values stored in the
    \\`TransferFlags` object (it's an `enum.IntFlag`) with bitwise-or. Here are a
    \\few examples:
    \\
    \\* `TransferFlags.LINKED`
    \\* `TransferFlags.PENDING`
    \\* `TransferFlags.POST_PENDING_TRANSFER`
    \\* `TransferFlags.VOID_PENDING_TRANSFER`
    ,
};
