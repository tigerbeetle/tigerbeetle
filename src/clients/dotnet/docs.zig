const builtin = @import("builtin");
const std = @import("std");

const assert = std.debug.assert;

const Docs = @import("../docs_types.zig").Docs;

pub const DotnetDocs = Docs{
    .directory = "dotnet",

    .markdown_name = "cs",
    .extension = "cs",
    .proper_name = ".NET",

    .test_source_path = "",

    .name = "tigerbeetle-dotnet",
    .description =
    \\The TigerBeetle client for .NET.
    ,

    .prerequisites =
    \\* .NET >= 7.0.
    \\
    \\And if you do not already have NuGet.org as a package
    \\source, make sure to add it:
    \\
    \\```console
    \\dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org
    \\```
    ,

    .project_file_name = "",
    .project_file = "",

    .test_file_name = "Program",

    .install_commands =
    \\dotnet new console
    \\dotnet add package tigerbeetle
    ,
    .run_commands = "dotnet run",

    .examples = "",

    .client_object_documentation =
    \\The `Client` class is thread-safe and for better performance, a
    \\single instance should be shared between multiple concurrent
    \\tasks. Multiple clients can be instantiated in case of connecting
    \\to more than one TigerBeetle cluster.
    ,
    .create_accounts_documentation =
    \\The `UInt128` fields like `ID`, `UserData128`, `Amount` and
    \\account balances have a few extension methods to make it easier
    \\to convert 128-bit little-endian unsigned integers between
    \\`BigInteger`, `byte[]`, and `Guid`.
    \\
    \\See the class [UInt128Extensions](/src/clients/dotnet/TigerBeetle/UInt128Extensions.cs) for more details.
    ,

    .account_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`AccountFlags` object with bitwise-or:
    \\
    \\* `AccountFlags.None`
    \\* `AccountFlags.Linked`
    \\* `AccountFlags.DebitsMustNotExceedCredits`
    \\* `AccountFlags.CreditsMustNotExceedDebits`
    ,

    .create_accounts_errors_documentation = "",

    .create_transfers_documentation = "",

    .create_transfers_errors_documentation = "",

    .transfer_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`TransferFlags` object with bitwise-or:
    \\
    \\* `TransferFlags.None`
    \\* `TransferFlags.Linked`
    \\* `TransferFlags.Pending`
    \\* `TransferFlags.PostPendingTransfer`
    \\* `TransferFlags.VoidPendingTransfer`
    ,
};
