const builtin = @import("builtin");
const std = @import("std");

const Docs = @import("../docs_types.zig").Docs;
const run = @import("../shutil.zig").run;
const run_shell = @import("../shutil.zig").run_shell;
const script_filename = @import("../shutil.zig").script_filename;
const write_shell_newlines_into_single_line = @import("../shutil.zig").write_shell_newlines_into_single_line;

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
    \\* .NET >= 6.0
    ,

    .project_file_name = "Test.csproj",
    .project_file = 
    \\<Project Sdk="Microsoft.NET.Sdk">
    \\  <PropertyGroup>
    \\    <OutputType>Exe</OutputType>
    \\    <TargetFramework>net7.0</TargetFramework>
    \\    <ImplicitUsings>enable</ImplicitUsings>
    \\    <Nullable>enable</Nullable>
    \\  </PropertyGroup>
    \\
    \\  <ItemGroup>
    \\    <PackageReference Include="tigerbeetle" Version="0.0.1.3814" />
    \\  </ItemGroup>
    \\
    \\</Project>
    ,

    .test_file_name = "Program",

    .install_prereqs = "",
    .install_sample_file = 
    \\using System;
    \\
    \\using TigerBeetle;
    \\
    \\public class Program {
    \\  public static void Main() {
    \\    Console.WriteLine("SUCCESS");
    \\  }
    \\}
    ,

    .current_commit_pre_install_hook = null,
    .current_commit_post_install_hook = null,

    .install_commands = "",
    .build_commands = "dotnet build",
    .run_commands = "dotnet run",

    .current_commit_install_commands_hook = null,
    .current_commit_build_commands_hook = null,
    .current_commit_run_commands_hook = null,

    .install_documentation = "",

    .examples = "",

    .client_object_example = 
    \\var tbAddress = Environment.GetEnvironmentVariable("TB_ADDRESS");
    \\var client = new Client(
    \\  clusterID: 0,
    \\  addresses: new[] {tbAddress.Length > 0 ? tbAddress : "3000"}
    \\);
    ,

    .client_object_documentation = 
    \\If you create a `Client` like this, don't forget to call
    \\`client.Dispose()` when you are done with it. Otherwise you
    \\can use the `using` syntax:
    \\```csharp
    \\using (var client = new Client(...)) {
    \\  // Use client
    \\}
    \\```
    \\
    \\The `Client` class is thread-safe and for better performance, a
    \\single instance should be shared between multiple concurrent
    \\tasks. Multiple clients can be instantiated in case of connecting
    \\to more than one TigerBeetle cluster.
    ,

    .create_accounts_example = 
    \\var accounts = new[] {
    \\  new Account
    \\  {
    \\    Id = 137,
    \\    UserData = Guid.NewGuid(),
    \\    Ledger = 1,
    \\    Code = 718,
    \\    Flags = AccountFlags.None,
    \\  },     
    \\};
    \\
    \\var createAccountsErrors = client.CreateAccounts(accounts);
    ,

    .create_accounts_documentation = 
    \\All TigerBeetle's IDs are 128-bit integers, and the .NET client
    \\accepts a wide range of values: `int`, `uint`, `long`, `ulong`,
    \\`Guid`, `byte[]` and `TigerBeetle.UInt128`.
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

    .account_flags_example = 
    \\var account0 = new Account{ /* ... account values ... */ };
    \\var account1 = new Account{ /* ... account values ... */ };
    \\account0.Flags = AccountFlags.Linked;
    \\
    \\createAccountsErrors = client.CreateAccounts(new []{account0, account1});
    ,

    .create_accounts_errors_example = 
    \\var account2 = tb_types.Account{ /* ... account values ... */ };
    \\var account3 = tb_types.Account{ /* ... account values ... */ };
    \\var account4 = tb_types.Account{ /* ... account values ... */ };
    \\
    \\createAccountsErrors = client.CreateAccounts(new []{account2, account3, account4});
    \\if err != nil {
    \\	log.Printf("Error creating accounts: %s", err);
    \\	return;
    \\}
    \\for _, err := range accountErrors {
    \\	log.Printf("Error creating account %d: %s", err.Index, err.Result)
    \\	return
    \\}
    ,

    .create_accounts_errors_documentation = "",

    .lookup_accounts_example = 
    \\accounts = client.LookupAccounts(new UInt128[] { 137, 138 });
    ,

    .create_transfers_example = 
    \\var transfers = new[] {
    \\  new Transfer
    \\  {
    \\    Id = 1,
    \\    DebitAccountId = 1,
    \\    CreditAccountId = 2,
    \\    Timeout = 0,
    \\    UserData = 2,
    \\    Ledger = 1,
    \\    Code = 1,
    \\    Flags = 0,
    \\    Amount = 10,
    \\  }
    \\};
    \\
    \\var createTransfersErrors = client.CreateTransfers(transfers);
    ,

    .create_transfers_documentation = "",

    .create_transfers_errors_example = "",

    .create_transfers_errors_documentation = "",

    .no_batch_example = "",

    .batch_example = "",

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

    .transfer_flags_link_example = "",

    .transfer_flags_post_example = "",

    .transfer_flags_void_example = "",

    .lookup_transfers_example = 
    \\transfers = client.LookupTransfers(new UInt128[] {1, 2});
    ,

    .linked_events_example = "",

    .developer_setup_sh_commands = 
    \\cd src/clients/dotnet
    \\dotnet build
    \\if [ "$TEST" = "true" ]; then dotnet clean; dotnet test; else echo "Skipping client unit tests"; fi
    ,

    .developer_setup_pwsh_commands = 
    \\cd src/clients/dotnet
    \\dotnet build
    \\if ($env:TEST -eq 'true') { dotnet clean; dotnet test } else { echo "Skipping client unit test" }
    ,

    .test_main_prefix = 
    \\using System;
    \\using System.Diagnostics;
    \\
    \\using TigerBeetle;
    \\
    ,

    .test_main_suffix = "",
};
