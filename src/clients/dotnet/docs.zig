const Docs = @import("../docs_types.zig").Docs;

pub const DotnetDocs = Docs{
    .readme = "dotnet/README.md",

    .markdown_name = "csharp",
    .extension = "cs",
    .proper_name = ".NET",

    .test_linux_docker_image = "alpine",

    .test_source_path = "",

    .name = "tigerbeetle-dotnet",
    .description =
    \\The TigerBeetle client for .NET.
    ,

    .prerequisites =
    \\* .NET >= 6
    ,

    .project_file_name = "",
    .project_file = "",

    .test_file_name = "Main",

    .install_sample_file =
    \\package com.tigerbeetle.examples;
    \\
    \\import com.tigerbeetle.*;
    \\
    \\public final class Main {
    \\    public static void main(String[] args) {
    \\        System.out.println("Import ok!");
    \\    }
    \\}
    ,

    .install_prereqs = "apk add -U maven openjdk11",

    .current_commit_pre_install_commands =
    \\mvn deploy:deploy-file -Durl=file://$(pwd) -Dfile=$(find . -name '*-SNAPSHOT.jar') -DgroupId=com.tigerbeetle -DartifactId=tigerbeetle-dotnet -Dpackaging=jar -Dversion=0.0.1-3431
    \\mkdir -p $HOME/.m2/
    \\echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"><localRepository>'$(pwd)'</localRepository></settings>' > $HOME/.m2/settings.xml
    ,

    .current_commit_post_install_commands = "",

    .install_commands =
    \\mvn install
    ,

    .install_sample_file_build_commands =
    \\mvn compile
    ,
    .install_sample_file_test_commands = "mvn exec:dotnet",

    .install_documentation = "",

    .examples = "",

    .client_object_example =
    \\Client client = new Client(
    \\  0,
    \\  new String[] {"3001", "3002", "3003"}
    \\);
    ,

    .client_object_documentation =
    \\If you create a `Client` like this, don't forget to call
    \\`client.close()` when you are done with it. Otherwise you
    \\can use the
    \\[try-with-resources](https://docs.oracle.com/dotnetse/tutorial/essential/exceptions/tryResourceClose.html)
    \\syntax:
    \\```dotnet
    \\try (var client = new Client(...)) {
    \\  // Use client
    \\} catch (Exception e) {
    \\  // Handle exception
    \\}
    \\```
    ,

    .create_accounts_example =
    \\AccountBatch accounts = new AccountBatch(1);
    \\accounts.add();
    \\accounts.setId(137);
    \\accounts.setUserData(UInt128.asBytes(new dotnet.math.BigInteger("92233720368547758070")));
    \\accounts.setLedger(1);
    \\accounts.setCode(718);
    \\accounts.setFlags(0);
    \\
    \\CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
    ,

    .create_accounts_documentation =
    \\The 128-bit fields like `id` and `user_data` have a few
    \\overrides to make it easier to integrate. You can either
    \\pass in a long, a pair of longs (least and most
    \\significant bits), or a `byte[]`.
    \\
    \\There is also a `com.tigerbeetle.UInt128` helper with static
    \\methods for converting 128-bit little-endian unsigned integers
    \\between instances of `long`, `UUID`, `BigInteger` and `byte[]`.
    ,

    .account_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`AccountFlags` object with bitwise-or:
    \\
    \\* `AccountFlags.LINKED`
    \\* `AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS`
    \\* `AccountFlags.CREDITS_MUST_NOT_EXCEED_CREDITS`
    ,

    .account_flags_example =
    \\accounts = new AccountBatch(3);
    \\
    \\// First account
    \\accounts.add();
    \\// Code to fill out fields for first account
    \\accounts.setFlags(AccountFlags.LINKED | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);
    \\
    \\// Second account
    \\accounts.add();
    \\// Code to fill out fields for second account
    \\
    \\accountErrors = client.createAccounts(accounts);
    ,

    .create_accounts_errors_example =
    \\accounts = new AccountBatch(3);
    \\
    \\// First account
    \\accounts.add();
    \\// Code to fill out fields for first account
    \\
    \\// Second account
    \\accounts.add();
    \\// Code to fill out fields for second account
    \\
    \\// Third account
    \\accounts.add();
    \\// Code to fill out fields for third account
    \\
    \\accountErrors = client.createAccounts(accounts);
    \\while (accountErrors.next()) {
    \\    switch (accountErrors.getResult()) {
    \\        case Exists:
    \\            System.err.printf("Account at %d already exists.\n",
    \\                accountErrors.getIndex());        
    \\            break;
    \\
    \\        default:
    \\            System.err.printf("Error creating account at %d: %s\n",
    \\                accountErrors.getIndex(),
    \\                accountErrors.getResult());
    \\            break;
    \\    }
    \\}
    ,

    .create_accounts_errors_documentation = "",

    .lookup_accounts_example =
    \\IdBatch ids = new IdBatch(2);
    \\ids.add(137);
    \\ids.add(138);
    \\accounts = client.lookupAccounts(ids);
    ,

    .create_transfers_example =
    \\TransferBatch transfers = new TransferBatch(1);
    \\transfers.add();
    \\transfers.setId(1);
    \\transfers.setDebitAccountId(1);
    \\transfers.setCreditAccountId(2);
    \\transfers.setUserData(2);
    \\transfers.setTimeout(0);
    \\transfers.setLedger(1);
    \\transfers.setCode(1);
    \\transfers.setFlags(0);
    \\transfers.setAmount(10);
    \\
    \\CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
    ,

    .create_transfers_documentation = "",

    .create_transfers_errors_example =
    \\while (transferErrors.next()) {
    \\    switch (transferErrors.getResult()) {
    \\        case ExceedsCredits:
    \\            System.err.printf("Transfer at %d exceeds credits.\n",
    \\                transferErrors.getIndex());        
    \\            break;
    \\
    \\        default:
    \\            System.err.printf("Error creating transfer at %d: %s\n",
    \\                transferErrors.getIndex(),
    \\                transferErrors.getResult());
    \\            break;
    \\    }
    \\}
    ,

    .create_transfers_errors_documentation = "",

    .no_batch_example =
    \\for (int i = 0; i < transfers.length; i++) {
    \\  TransferBatch batch = new TransferBatch(1);
    \\  batch.add();
    \\  batch.setId(transfers[i].getId());
    \\  batch.setDebitAccountId(transfers[i].getDebitAccountId());
    \\  batch.setCreditAccountId(transfers[i].getCreditAccountId());
    \\
    \\  CreateTransferResultBatch errors = client.createTransfers(batch);
    \\  // error handling omitted
    \\}
    ,

    .batch_example =
    \\var BATCH_SIZE = 8191;
    \\for (int i = 0; i < transfers.length; i += BATCH_SIZE) {
    \\  TransferBatch batch = new TransferBatch(BATCH_SIZE);
    \\
    \\  for (int j = 0; j < BATCH_SIZE && i + j < transfers.length; j++) {
    \\    batch.add();
    \\    batch.setId(transfers[i + j].getId());
    \\    batch.setDebitAccountId(transfers[i + j].getDebitAccountId());
    \\    batch.setCreditAccountId(transfers[i + j].getCreditAccountId());
    \\  }
    \\
    \\  CreateTransferResultBatch errors = client.createTransfers(batch);
    \\  // error handling omitted
    \\}
    ,

    .transfer_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`TransferFlags` object with bitwise-or:
    \\
    \\* `TransferFlags.NONE`
    \\* `TransferFlags.LINKED`
    \\* `TransferFlags.PENDING`
    \\* `TransferFlags.POST_PENDING_TRANSFER`
    \\* `TransferFlags.VOID_PENDING_TRANSFER`
    ,

    .transfer_flags_link_example =
    \\transfers = new TransferBatch(2);
    \\
    \\// First transfer
    \\transfers.add();
    \\// Code to fill out fields for first transfer
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\// Second transfer
    \\transfers.add();
    \\// Code to fill out fields for second transfer
    \\transferErrors = client.createTransfers(transfers);
    ,

    .transfer_flags_post_example =
    \\transfers = new TransferBatch(1);
    \\
    \\// First transfer
    \\transfers.add();
    \\// Code to fill out fields for first transfer
    \\transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER);
    \\transferErrors = client.createTransfers(transfers);
    ,

    .transfer_flags_void_example =
    \\transfers = new TransferBatch(1);
    \\
    \\// First transfer
    \\transfers.add();
    \\// Code to fill out fields for first transfer
    \\transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
    \\transferErrors = client.createTransfers(transfers);
    ,

    .lookup_transfers_example =
    \\ids = new IdBatch(2);
    \\ids.add(1);
    \\ids.add(2);
    \\transfers = client.lookupTransfers(ids);
    ,

    .linked_events_example =
    \\transfers = new TransferBatch(10);
    \\
    \\// An individual transfer (successful):
    \\transfers.add();
    \\transfers.setId(1);
    \\
    \\// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
    \\transfers.add();
    \\transfers.setId(2); // Commit/rollback.
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\transfers.add();
    \\transfers.setId(3); // Commit/rollback.
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\transfers.add();
    \\transfers.setId(2); // Fail with exists
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\transfers.add();
    \\transfers.setId(4); // Fail without committing
    \\
    \\// An individual transfer (successful):
    \\// This should not see any effect from the failed chain above.
    \\transfers.add();
    \\transfers.setId(2);
    \\
    \\// A chain of 2 transfers (the first transfer fails the chain):
    \\transfers.add();
    \\transfers.setId(2);
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\transfers.add();
    \\transfers.setId(3);
    \\
    \\// A chain of 2 transfers (successful):
    \\transfers.add();
    \\transfers.setId(3);
    \\transfers.setFlags(TransferFlags.LINKED);
    \\
    \\transfers.add();
    \\transfers.setId(4);
    \\
    \\transferErrors = client.createTransfers(transfers);
    ,

    .developer_setup_sh_commands =
    \\git clone https://github.com/tigerbeetledb/tigerbeetle
    \\cd tigerbeetle
    \\git checkout $GIT_SHA
    \\cd src/clients/dotnet
    \\dotnet build
    ,

    .developer_setup_pwsh_commands =
    \\git clone https://github.com/tigerbeetledb/tigerbeetle
    \\cd tigerbeetle
    \\git checkout $GIT_SHA
    \\cd src/clients/dotnet
    \\dotnet build
    ,

    .test_main_prefix = "",

    .test_main_suffix = "",
};
