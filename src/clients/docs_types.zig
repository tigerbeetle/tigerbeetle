pub const Docs = struct {
    readme: [:0]const u8,
    name: []const u8,
    markdown_name: []const u8,
    extension: []const u8,
    description: []const u8,

    prerequisites: []const u8,

    install_commands: []const u8,
    install_sample_file: []const u8,
    install_sample_file_build_commands: []const u8,
    install_sample_file_test_commands: []const u8,
    install_documentation: []const u8,

    examples: []const u8,

    client_object_example: []const u8,
    client_object_documentation: []const u8,

    create_accounts_example: []const u8,
    create_accounts_documentation: []const u8,
    create_accounts_errors_example: []const u8,
    create_accounts_errors_documentation: []const u8,

    account_flags_documentation: []const u8,
    account_flags_example: []const u8,

    lookup_accounts_example: []const u8,

    create_transfers_example: []const u8,
    create_transfers_documentation: []const u8,
    create_transfers_errors_example: []const u8,
    create_transfers_errors_documentation: []const u8,

    batch_example: []const u8,
    no_batch_example: []const u8,

    transfer_flags_documentation: []const u8,
    transfer_flags_link_example: []const u8,
    transfer_flags_post_example: []const u8,
    transfer_flags_void_example: []const u8,

    lookup_transfers_example: []const u8,

    linked_events_example: []const u8,

    developer_setup_bash_commands: []const u8,
    developer_setup_windows_commands: []const u8,
    test_linux_docker_image: []const u8,

    test_main_prefix: []const u8,
    test_main_suffix: []const u8,

    code_format_commands: []const u8,
};
