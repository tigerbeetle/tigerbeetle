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

    // Start five pending transfers.
    var createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 1,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 100,
        Flags = TransferFlags.Pending,
    },
    new Transfer
    {
        Id = 2,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 200,
        Flags = TransferFlags.Pending,
    },
    new Transfer
    {
        Id = 3,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 300,
        Flags = TransferFlags.Pending,
    },
    new Transfer
    {
        Id = 4,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 400,
        Flags = TransferFlags.Pending,
    },
    new Transfer
    {
        Id = 5,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 500,
        Flags = TransferFlags.Pending,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate accounts pending and posted debits/credits before
    // finishing the two-phase transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 1500);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 1500);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create a 6th transfer posting the 1st transfer.
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 6,
        PendingId = 1,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 100,
        Flags = TransferFlags.PostPendingTransfer,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate account balances after posting 1st pending transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 100);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 1400);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 100);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 1400);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create a 7th transfer voiding the 2nd transfer.
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 7,
        PendingId = 2,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 200,
        Flags = TransferFlags.VoidPendingTransfer,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate account balances after voiding 2nd pending transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 100);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 1200);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 100);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 1200);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create an 8th transfer posting the 3rd transfer.
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 8,
        PendingId = 3,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 300,
        Flags = TransferFlags.PostPendingTransfer,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate account balances after posting 3rd pending transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 400);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 900);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 400);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 900);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create a 9th transfer voiding the 4th transfer.
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 9,
        PendingId = 4,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 400,
        Flags = TransferFlags.VoidPendingTransfer,
    }
    });
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating transfer {0}: {1}", error.Index, error.Result);
        throw new Exception("Unexpected error");
    }

    // Validate account balances after voiding 4th pending transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 400);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 500);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 400);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 500);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    // Create a 10th transfer posting the 5th transfer.
    createTransfersError = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = 10,
        PendingId = 5,
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

    // Validate account balances after posting 5th pending transfer.
    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 900);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 900);
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
