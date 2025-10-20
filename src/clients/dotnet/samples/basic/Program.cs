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

    var accountsResults = client.CreateAccounts(accounts);
    Debug.Assert(accountsResults.Length == 2);
    Debug.Assert(accountsResults[0].Result == CreateAccountResult.Ok);
    Debug.Assert(accountsResults[1].Result == CreateAccountResult.Ok);

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

    var transfersResults = client.CreateTransfers(transfers);
    Debug.Assert(transfersResults.Length == 1);
    Debug.Assert(transfersResults[0].Result == CreateTransferResult.Ok);

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
}
