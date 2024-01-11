const std = @import("std");

// The purpose of these types is to help in reading this doc, not
// because the types matter.
const String = []const u8;

// All Code variables are potentially tested and run in CI.
const Code = []const u8;

// All Markdown strings are never tested and run in CI.
const Markdown = []const u8;

pub const Docs = struct {
    // Name of the directory (relative to /src/clients)
    directory: String,

    // Package name (i.e. tigerbeetle-go, tigerbeetle-node, etc.)
    name: String,

    // Name for syntax highlighting (i.e. javascript for node, go for go, etc.)
    markdown_name: String,

    // File extension without dot (i.e. js, go, etc.)
    extension: String,

    // For the title of the page on the docs site.
    proper_name: String,

    // Introduction to the client. Links to docs or build badges or
    // whatnot.
    description: Markdown,

    // Any libraries or languages and their required versions for
    // using, not necessarily hacking on, this client.
    prerequisites: Markdown,

    // If you need an additional project file like pom.xml or
    // package.json. Leave blank if not needed.
    project_file_name: String,
    // The actual contents of the file. Leave blank if not needed.
    project_file: Code,

    // If you need to override the default name of test.${extension}
    // such as when file names have meaning (i.e. Java).
    test_file_name: String,

    // Any setup needed for a project before compiling and running
    // such as `go mod init myProject && go mod tidy` or `npm install
    // tigerbeetle-node`.
    install_commands: Code,

    // Commands for building and running code.
    run_commands: Code,

    // If you want to include links to examples.
    examples: Markdown,

    client_object_documentation: Markdown,

    create_accounts_documentation: Markdown,

    create_accounts_errors_documentation: Markdown,

    account_flags_documentation: Markdown,

    create_transfers_documentation: Markdown,

    create_transfers_errors_documentation: Markdown,

    // Good example of using batches to create transfers.
    // Bad example of not using batches well to create transfers.

    transfer_flags_documentation: Markdown,

    // Optional prefix if test code must be in a certain directory
    // (e.g. Java and `src/main/java`).
    test_source_path: String,
};

pub const Sample = struct {
    // Capitalized name of the sample program
    proper_name: String,

    // e.g. `basic`, `two-phase`, etc.
    directory: String,

    // For use in the language primary README
    short_description: String,

    // For use as the introduction on the individual sample README
    long_description: String,
};
