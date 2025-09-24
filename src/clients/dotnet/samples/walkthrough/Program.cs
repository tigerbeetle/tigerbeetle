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

// The examples currently throws because the batch is actually invalid (most of fields are
// undefined). Ideally, we prepare a correct batch here while keeping the syntax compact,
// for the example, but for the time being lets prioritize a readable example and just
// swallow the error.

using (var client = new Client(clusterID, addresses))
{
    try
    {
        // section:create-accounts
        var accounts = new[] {
            new Account
            {
                Id = ID.Create(), // TigerBeetle time-based ID.
                UserData128 = 0,
                UserData64 = 0,
                UserData32 = 0,
                Ledger = 1,
                Code = 718,
                Flags = AccountFlags.None,
                Timestamp = 0,
            },
        };

        var accountErrors = client.CreateAccounts(accounts);
        // Error handling omitted.
        // endsection:create-accounts
    }
    catch { }

    try
    {
        // section:account-flags
        var account0 = new Account
        {
            Id = 100,
            Ledger = 1,
            Code = 1,
            Flags = AccountFlags.Linked | AccountFlags.DebitsMustNotExceedCredits,
        };
        var account1 = new Account
        {
            Id = 101,
            Ledger = 1,
            Code = 1,
            Flags = AccountFlags.History,
        };

        var accountErrors = client.CreateAccounts(new[] { account0, account1 });
        // Error handling omitted.
        // endsection:account-flags
    }
    catch { }

    try
    {
        // section:create-accounts-errors
        var account0 = new Account
        {
            Id = 102,
            Ledger = 1,
            Code = 1,
            Flags = AccountFlags.None,
        };
        var account1 = new Account
        {
            Id = 103,
            Ledger = 1,
            Code = 1,
            Flags = AccountFlags.None,
        };
        var account2 = new Account
        {
            Id = 104,
            Ledger = 1,
            Code = 1,
            Flags = AccountFlags.None,
        };

        var accountErrors = client.CreateAccounts(new[] { account0, account1, account2 });
        foreach (var error in accountErrors)
        {
            switch (error.Result)
            {
                case CreateAccountResult.Exists:
                    Console.WriteLine($"Batch account at ${error.Index} already exists.");
                    break;
                default:
                    Console.WriteLine($"Batch account at ${error.Index} failed to create ${error.Result}");
                    break;
            }
            return;
        }
        // endsection:create-accounts-errors
    }
    catch { }

    try
    {
        // section:lookup-accounts
        Account[] accounts = client.LookupAccounts(new UInt128[] { 100, 101 });
        // endsection:lookup-accounts
    }
    catch { }

    try
    {
        // section:create-transfers
        var transfers = new[] {
            new Transfer
            {
                Id = ID.Create(), // TigerBeetle time-based ID.
                DebitAccountId = 102,
                CreditAccountId = 103,
                Amount = 10,
                UserData128 = 0,
                UserData64 = 0,
                UserData32 = 0,
                Timeout = 0,
                Ledger = 1,
                Code = 1,
                Flags = TransferFlags.None,
                Timestamp = 0,
            }
        };

        var transferErrors = client.CreateTransfers(transfers);
        // Error handling omitted.
        // endsection:create-transfers
    }
    catch { }

    try
    {
        // section:create-transfers-errors
        var transfers = new[] {
            new Transfer
            {
                Id = 1,
                DebitAccountId = 102,
                CreditAccountId = 103,
                Amount = 10,
                Ledger = 1,
                Code = 1,
                Flags = TransferFlags.None,
            },
            new Transfer
            {
                Id = 2,
                DebitAccountId = 102,
                CreditAccountId = 103,
                Amount = 10,
                Ledger = 1,
                Code = 1,
                Flags = TransferFlags.None,
            },
            new Transfer
            {
                Id = 3,
                DebitAccountId = 102,
                CreditAccountId = 103,
                Amount = 10,
                Ledger = 1,
                Code = 1,
                Flags = TransferFlags.None,
            },
        };

        var transferErrors = client.CreateTransfers(transfers);
        foreach (var error in transferErrors)
        {
            switch (error.Result)
            {
                case CreateTransferResult.Exists:
                    Console.WriteLine($"Batch transfer at ${error.Index} already exists.");
                    break;
                default:
                    Console.WriteLine($"Batch transfer at ${error.Index} failed to create: ${error.Result}");
                    break;
            }
        }
        // endsection:create-transfers-errors
    }
    catch { }

    try
    {
        // section:no-batch
        var batch = new Transfer[] { }; // Array of transfer to create.
        foreach (var t in batch)
        {
            var transferErrors = client.CreateTransfer(t);
            // Error handling omitted.
        }
        // endsection:no-batch
    }
    catch { }

    try
    {
        // section:batch
        var batch = new Transfer[] { }; // Array of transfer to create.
        var BATCH_SIZE = 8189;
        for (int firstIndex = 0; firstIndex < batch.Length; firstIndex += BATCH_SIZE)
        {
            var lastIndex = firstIndex + BATCH_SIZE;
            if (lastIndex > batch.Length)
            {
                lastIndex = batch.Length;
            }
            var transferErrors = client.CreateTransfers(batch[firstIndex..lastIndex]);
            // Error handling omitted.
        }
        // endsection:batch
    }
    catch { }

    try
    {
        // section:transfer-flags-link
        var transfer0 = new Transfer
        {
            Id = 4,
            DebitAccountId = 102,
            CreditAccountId = 103,
            Amount = 10,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Linked,
        };
        var transfer1 = new Transfer
        {
            Id = 5,
            DebitAccountId = 102,
            CreditAccountId = 103,
            Amount = 10,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        };

        var transferErrors = client.CreateTransfers(new[] { transfer0, transfer1 });
        // Error handling omitted.
        // endsection:transfer-flags-link
    }
    catch { }

    try
    {
        // section:transfer-flags-post
        var transfer0 = new Transfer
        {
            Id = 6,
            DebitAccountId = 102,
            CreditAccountId = 103,
            Amount = 10,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var transferErrors = client.CreateTransfers(new[] { transfer0 });
        // Error handling omitted.

        var transfer1 = new Transfer
        {
            Id = 7,
            // Post the entire pending amount.
            Amount = Transfer.AmountMax,
            PendingId = 6,
            Flags = TransferFlags.PostPendingTransfer,
        };

        transferErrors = client.CreateTransfers(new[] { transfer1 });
        // Error handling omitted.
        // endsection:transfer-flags-post
    }
    catch { }

    try
    {
        // section:transfer-flags-void
        var transfer0 = new Transfer
        {
            Id = 8,
            DebitAccountId = 102,
            CreditAccountId = 103,
            Amount = 10,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var transferErrors = client.CreateTransfers(new[] { transfer0 });
        // Error handling omitted.

        var transfer1 = new Transfer
        {
            Id = 9,
            Amount = 0,
            PendingId = 8,
            Flags = TransferFlags.VoidPendingTransfer,
        };

        transferErrors = client.CreateTransfers(new[] { transfer1 });
        // Error handling omitted.
        // endsection:transfer-flags-void
    }
    catch { }

    try
    {
        // section:lookup-transfers
        Transfer[] transfers = client.LookupTransfers(new UInt128[] { 1, 2 });
        // endsection:lookup-transfers
    }
    catch { }

    try
    {
        // section:get-account-transfers
        var filter = new AccountFilter
        {
            AccountId = 101,
            UserData128 = 0, // No filter by UserData.
            UserData64 = 0,
            UserData32 = 0,
            Code = 0, // No filter by Code.
            TimestampMin = 0, // No filter by Timestamp.
            TimestampMax = 0, // No filter by Timestamp.
            Limit = 10, // Limit to ten transfers at most.
            Flags = AccountFilterFlags.Debits | // Include transfer from the debit side.
                AccountFilterFlags.Credits | // Include transfer from the credit side.
                AccountFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
        };

        Transfer[] transfers = client.GetAccountTransfers(filter);
        // endsection:get-account-transfers
    }
    catch { }

    try
    {
        // section:get-account-balances
        var filter = new AccountFilter
        {
            AccountId = 101,
            UserData128 = 0, // No filter by UserData.
            UserData64 = 0,
            UserData32 = 0,
            Code = 0, // No filter by Code.
            TimestampMin = 0, // No filter by Timestamp.
            TimestampMax = 0, // No filter by Timestamp.
            Limit = 10, // Limit to ten balances at most.
            Flags = AccountFilterFlags.Debits | // Include transfer from the debit side.
                AccountFilterFlags.Credits | // Include transfer from the credit side.
                AccountFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
        };

        AccountBalance[] accountBalances = client.GetAccountBalances(filter);
        // endsection:get-account-balances
    }
    catch { }

    try
    {
        // section:query-accounts
        var filter = new QueryFilter
        {
            UserData128 = 1000, // Filter by UserData.
            UserData64 = 100,
            UserData32 = 10,
            Code = 1, // Filter by Code.
            Ledger = 0, // No filter by Ledger.
            TimestampMin = 0, // No filter by Timestamp.
            TimestampMax = 0, // No filter by Timestamp.
            Limit = 10, // Limit to ten accounts at most.
            Flags = QueryFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
        };

        Account[] accounts = client.QueryAccounts(filter);
        // endsection:query-accounts
    }
    catch { }

    try
    {
        // section:query-transfers
        var filter = new QueryFilter
        {
            UserData128 = 1000, // Filter by UserData
            UserData64 = 100,
            UserData32 = 10,
            Code = 1, // Filter by Code
            Ledger = 0, // No filter by Ledger
            TimestampMin = 0, // No filter by Timestamp.
            TimestampMax = 0, // No filter by Timestamp.
            Limit = 10, // Limit to ten transfers at most.
            Flags = QueryFilterFlags.Reversed, // Sort by timestamp in reverse-chronological order.
        };

        Transfer[] transfers = client.QueryTransfers(filter);
        // endsection:query-transfers
    }
    catch { }

    try
    {
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

        var transferErrors = client.CreateTransfers(batch.ToArray());
        // Error handling omitted.
        // endsection:linked-events
    }
    catch { }

    try
    {
        // section:imported-events
        // External source of time
        ulong historicalTimestamp = 0UL;
        var historicalAccounts = new Account[] { /* Loaded from an external source */ };
        var historicalTransfers = new Transfer[] { /* Loaded from an external source */ };

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

        var accountErrors = client.CreateAccounts(accountsBatch.ToArray());
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

        var transferErrors = client.CreateTransfers(transfersBatch.ToArray());
        // Error handling omitted.
        // Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
        // with the same historical timestamps without regressing the cluster timestamp.
        // endsection:imported-events
    }
    catch { }
}
