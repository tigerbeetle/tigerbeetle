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
        return;
    }

    var transfers = new[] {
    new Transfer
    {
        Id = 1,
        DebitAccountId = 1,
        CreditAccountId = 2,
        Ledger = 1,
        Code = 1,
        Amount = 10,
    }
    };

    var createTransfersError = client.CreateTransfers(transfers);
    foreach (var error in createTransfersError)
    {
        Console.WriteLine("Error creating account {0}: {1}", error.Index, error.Result);
        return;
    }

    accounts = client.LookupAccounts(new UInt128[] { 1, 2 });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == 1)
        {
            Debug.Assert(account.DebitsPosted == 10);
            Debug.Assert(account.CreditsPosted == 0);
        }
        else if (account.Id == 2)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 10);
        }
        else
        {
            throw new Exception("Unexpected account");
        }
    }

    Console.WriteLine("ok");
}
