// section:imports
using System;

using TigerBeetle;

// Validate import works.
Console.WriteLine("SUCCESS");
// endsection:imports

// section:client
var tbAddress = Environment.GetEnvironmentVariable("TB_ADDRESS");
var clusterID = UInt128.Zero;
var addresses = new[] { tbAddress != null ? tbAddress : "3000" };
using (var client = new Client(clusterID, addresses))
{
    // Use client
}
// endsection:client

using (var client = new Client(clusterID, addresses))
{

    // section:create-accounts
    var accounts = new[] {
        new Account
        {
            Id = 137,
            UserData128 = Guid.NewGuid().ToUInt128(),
            UserData64 = 1000,
            UserData32 = 100,
            Ledger = 1,
            Code = 718,
            Flags = AccountFlags.None,
        },
    };

    var createAccountsError = client.CreateAccounts(accounts);
    // endsection:create-accounts

    // section:account-flags
    var account0 = new Account { /* ... account values ... */ };
    var account1 = new Account { /* ... account values ... */ };
    account0.Flags = AccountFlags.Linked;

    createAccountsError = client.CreateAccounts(new[] { account0, account1 });
    // endsection:account-flags

    // section:create-accounts-errors
    var account2 = new Account { /* ... account values ... */ };
    var account3 = new Account { /* ... account values ... */ };
    var account4 = new Account { /* ... account values ... */ };

    createAccountsError = client.CreateAccounts(new[] { account2, account3, account4 });
    foreach (var error in createAccountsError)
    {
        Console.WriteLine("Error creating account {0}: {1}", error.Index, error.Result);
        return;
    }
    // endsection:create-accounts-errors

    // section:lookup-accounts
    accounts = client.LookupAccounts(new UInt128[] { 137, 138 });
    // endsection:lookup-accounts

    // section:create-transfers
    var transfers = new[] {
        new Transfer
        {
            Id = 1,
            DebitAccountId = 1,
            CreditAccountId = 2,
            Amount = 10,
            UserData128 = 2000,
            UserData64 = 200,
            UserData32 = 2,
            Timeout = 0,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        }
    };

    var createTransfersError = client.CreateTransfers(transfers);
    // endsection:create-transfers

    // section:create-transfers-errors
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating account {0}: {1}", error.Index, error.Result);
        return;
    }
    // endsection:create-transfers-errors

    // section:no-batch
    foreach (var t in transfers)
    {
        createTransfersError = client.CreateTransfers(new[] { t });
        // Error handling omitted.
    }
    // endsection:no-batch

    // section:batch
    var BATCH_SIZE = 8190;
    for (int i = 0; i < transfers.Length; i += BATCH_SIZE)
    {
        var batchSize = BATCH_SIZE;
        if (i + BATCH_SIZE > transfers.Length)
        {
            batchSize = transfers.Length - i;
        }
        createTransfersError = client.CreateTransfers(transfers[i..batchSize]);
        // Error handling omitted.
    }
    // endsection:batch

    // section:transfer-flags-link
    var transfer0 = new Transfer { /* ... account values ... */ };
    var transfer1 = new Transfer { /* ... account values ... */ };
    transfer0.Flags = TransferFlags.Linked;
    createTransfersError = client.CreateTransfers(new Transfer[] { transfer0, transfer1 });

    // endsection:transfer-flags-link

    // section:transfer-flags-post
    transfers = new Transfer[] { new Transfer {
        Id = 2,
        PendingId = 1,
        Flags = TransferFlags.PostPendingTransfer,
    }};

    createTransfersError = client.CreateTransfers(transfers);
    // Error handling omitted.
    // endsection:transfer-flags-post

    // section:transfer-flags-void
    transfers = new Transfer[] { new Transfer {
        Id = 2,
        PendingId = 1,
        Flags = TransferFlags.PostPendingTransfer,
    }};

    createTransfersError = client.CreateTransfers(transfers);
    // Error handling omitted.
    // endsection:transfer-flags-void

    // section:lookup-transfers
    transfers = client.LookupTransfers(new UInt128[] { 1, 2 });
    // endsection:lookup-transfers

    // section:get-account-transfers
    var filter = new AccountFilter
    {
        AccountId = 2,
        TimestampMin = 0, // No filter by Timestamp.
        TimestampMax = 0, // No filter by Timestamp.
        Limit = 10, // Limit to ten transfers at most.
        Flags = AccountFilterFlags.Debits | // Include transfer from the debit side.
            AccountFilterFlags.Credits | // Include transfer from the credit side.
            AccountFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
    };

    transfers = client.GetAccountTransfers(filter);
    // endsection:get-account-transfers

    // section:get-account-balances
    filter = new AccountFilter
    {
        AccountId = 2,
        TimestampMin = 0, // No filter by Timestamp.
        TimestampMax = 0, // No filter by Timestamp.
        Limit = 10, // Limit to ten balances at most.
        Flags = AccountFilterFlags.Debits | // Include transfer from the debit side.
            AccountFilterFlags.Credits | // Include transfer from the credit side.
            AccountFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
    };

    var account_balances = client.GetAccountBalances(filter);
    // endsection:get-account-balances

    // section:query-accounts
    var query_filter = new QueryFilter
    {
        UserData128 = 1000, // Filter by UserData.
        UserData64 = 100,
        UserData32 = 10,
        Code = 1, // Filter by Code.
        Ledger = 0, // No filter by Ledger.
        TimestampMin = 0, // No filter by Timestamp.
        TimestampMax = 0, // No filter by Timestamp.
        Limit = 10, // Limit to ten balances at most.
        Flags = QueryFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
    };

    var query_accounts = client.QueryAccounts(query_filter);
    // endsection:query-accounts

    // section:query-transfers
    query_filter = new QueryFilter
    {
        UserData128 = 1000, // Filter by UserData
        UserData64 = 100,
        UserData32 = 10,
        Code = 1, // Filter by Code
        Ledger = 0, // No filter by Ledger
        TimestampMin = 0, // No filter by Timestamp.
        TimestampMax = 0, // No filter by Timestamp.
        Limit = 10, // Limit to ten balances at most.
        Flags = QueryFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
    };

    var query_transfers = client.QueryTransfers(query_filter);
    // endsection:query-transfers

    // section:linked-events
    var batch = new System.Collections.Generic.List<Transfer>();

    // An individual transfer (successful):
    batch.Add(new Transfer { Id = 1, /* ... rest of transfer ... */ });

    // A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
    batch.Add(new Transfer { Id = 2, /* ... rest of transfer ... */ Flags = TransferFlags.Linked }); // Commit/rollback.
    batch.Add(new Transfer { Id = 3, /* ... rest of transfer ... */ Flags = TransferFlags.Linked }); // Commit/rollback.
    batch.Add(new Transfer { Id = 2, /* ... rest of transfer ... */ Flags = TransferFlags.Linked }); // Fail with exists
    batch.Add(new Transfer { Id = 4, /* ... rest of transfer ... */ }); // Fail without committing

    // An individual transfer (successful):
    // This should not see any effect from the failed chain above.
    batch.Add(new Transfer { Id = 2, /* ... rest of transfer ... */ });

    // A chain of 2 transfers (the first transfer fails the chain):
    batch.Add(new Transfer { Id = 2, /* ... rest of transfer ... */ Flags = TransferFlags.Linked });
    batch.Add(new Transfer { Id = 3, /* ... rest of transfer ... */ });

    // A chain of 2 transfers (successful):
    batch.Add(new Transfer { Id = 3, /* ... rest of transfer ... */ Flags = TransferFlags.Linked });
    batch.Add(new Transfer { Id = 4, /* ... rest of transfer ... */ });

    createTransfersError = client.CreateTransfers(batch.ToArray());
    // Error handling omitted.
    // endsection:linked-events

    // External source of time
    ulong historicalTimestamp = 0UL;
    var historicalAccounts = Array.Empty<Account>();
    var historicalTransfers = Array.Empty<Transfer>();

    // section:imported-events
    // First, load and import all accounts with their timestamps from the historical source.
    var accountsBatch = new System.Collections.Generic.List<Account>();
    for (var index = 0; index < historicalAccounts.Length; index++)
    {
        var account = historicalAccounts[index];

        // Set a unique and strictly increasing timestamp.
        historicalTimestamp += 1;
        account.Timestamp = historicalTimestamp;
        // Set the account as `imported`.
        account.Flags = AccountFlags.Imported;
        // To ensure atomicity, the entire batch (except the last event in the chain)
        // must be `linked`.
        if (index < historicalAccounts.Length - 1)
        {
            account.Flags |= AccountFlags.Linked;
        }

        accountsBatch.Add(account);
    }

    createAccountsError = client.CreateAccounts(accountsBatch.ToArray());
    // Error handling omitted.

    // Then, load and import all transfers with their timestamps from the historical source.
    var transfersBatch = new System.Collections.Generic.List<Transfer>();
    for (var index = 0; index < historicalTransfers.Length; index++)
    {
        var transfer = historicalTransfers[index];

        // Set a unique and strictly increasing timestamp.
        historicalTimestamp += 1;
        transfer.Timestamp = historicalTimestamp;
        // Set the account as `imported`.
        transfer.Flags = TransferFlags.Imported;
        // To ensure atomicity, the entire batch (except the last event in the chain)
        // must be `linked`.
        if (index < historicalTransfers.Length - 1)
        {
            transfer.Flags |= TransferFlags.Linked;
        }

        transfersBatch.Add(transfer);
    }

    createTransfersError = client.CreateTransfers(transfersBatch.ToArray());
    // Error handling omitted.
    // Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
    // with the same historical timestamps without regressing the cluster timestamp.
    // endsection:imported-events
}
