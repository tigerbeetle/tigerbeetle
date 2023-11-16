using System;
using System.Diagnostics;

using TigerBeetle;

var tbAddress = Environment.GetEnvironmentVariable("TB_ADDRESS");
using (var client = new Client(
       clusterID: UInt128.Zero,
       addresses: new[] { tbAddress != null ? tbAddress : "3000" }
       ))
{

    // Create two accounts
    var accounts = new[] {
    new Account
    {
        Id = 1,
        Ledger= 1,
        Code = 1,
    },
    new Account
    {
        Id = 2,
        Ledger = 1,
        Code = 1,
    },
    };

    var createAccountsError = client.CreateAccounts(accounts);
    foreach (var error in createAccountsError)
    {
        Console.WriteLine("Error creating account {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Start a pending transfer
    var createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 1,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Amount = 500,
        Ledger = 1,
        Code = 1,
        Flags = TransferFlags.Pending,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate accounts pending and posted debits/credits before finishing the two-phase transfer
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 500);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 500);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create a second transfer simply posting the first transfer
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 2,
        PendingId = 1,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 500,
        Flags = TransferFlags.PostPendingTransfer,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate the contents of all transfers
    var transfers = client.LookupTransfers(new UInt128[] { 1, 2 });
    Debug.Assert(transfers.Length == 2);
    foreach (var transfer in transfers)
    {
        if (transfer.Id == 1)
        {
            Debug.Assert(transfer.Flags == TransferFlags.Pending);
        }
        else if (transfer.Id == 2)
        {
            Debug.Assert(transfer.Flags == TransferFlags.PostPendingTransfer);
        }
        else
        {
            throw new Exception("Unexpected transfer");
        }
    }

    // Validate accounts pending and posted debits/credits after finishing the two-phase transfer
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 500);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 500);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 0);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    Console.WriteLine("ok");
}
