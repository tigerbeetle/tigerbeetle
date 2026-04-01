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
    \\* Ruby >= `3.0`
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "main",

    .install_commands = "gem install tigerbeetle",
    .run_commands = "",

    .examples = "",

    .client_object_documentation = "",
    .create_accounts_documentation = "",
    .account_flags_documentation = "",

    .create_accounts_errors_documentation = "",
    .create_transfers_documentation = "",
    .create_transfers_errors_documentation = "",

    .transfer_flags_documentation = "",
};
