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
        // error handling omitted
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
        var segment = new ArraySegment<Transfer>(transfers, i, batchSize);
        createTransfersError = client.CreateTransfers(segment.Array);
        // error handling omitted
    }
    // endsection:batch

    // section:transfer-flags-link
    var transfer0 = new Transfer { /* ... account values ... */ };
    var transfer1 = new Transfer { /* ... account values ... */ };
    transfer0.Flags = TransferFlags.Linked;
    createTransfersError = client.CreateTransfers(new Transfer[] { transfer0, transfer1 });

    // endsection:transfer-flags-link

    // section:transfer-flags-post
    var transfer = new Transfer
    {
        Id = 2,
        PendingId = 1,
        Flags = TransferFlags.PostPendingTransfer,
    };
    createTransfersError = client.CreateTransfers(new Transfer[] { transfer });
    // error handling omitted
    // endsection:transfer-flags-post

    // section:transfer-flags-void
    transfer = new Transfer
    {
        Id = 2,
        PendingId = 1,
        Flags = TransferFlags.PostPendingTransfer,
    };
    createTransfersError = client.CreateTransfers(new Transfer[] { transfer });
    // error handling omitted
    // endsection:transfer-flags-void

    // section:lookup-transfers
    transfers = client.LookupTransfers(new UInt128[] { 1, 2 });
    // endsection:lookup-transfers

    // section:get-account-transfers
    var filter = new GetAccountTransfers
    {
        AccountId = 2,
        Timestamp = 0, // No filter by Timestamp.
        Limit = 10, // Limit to ten transfers at most.
        Flags = GetAccountTransfersFlags.Debits | // Include transfer from the debit side.
            GetAccountTransfersFlags.Credits | // Include transfer from the credit side.
            GetAccountTransfersFlags.Reversed, // Sort by timestamp in reverse-chronological order.
    };
    transfers = client.GetAccountTransfers(filter);
    // endsection:get-account-transfers

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
    // error handling omitted
    // endsection:linked-events
}
