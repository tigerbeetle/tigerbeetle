using System;
using System.Diagnostics;

using TigerBeetle;

var tbAddress = Environment.GetEnvironmentVariable("TB_ADDRESS");
using (var client = new Client(
       clusterID: UInt128.Zero,
       addresses: tbAddress != null ? tbAddress : "3000"
       ))
{
    // Create accounts.
    var accountDrId = ID.Create();
    var accountCrId = ID.Create();

    var accounts = new[] {
    new Account
    {
        Id = accountDrId,
        Ledger= 1,
        Code = 1,
    },
    new Account
    {
        Id = accountCrId,
        Ledger = 1,
        Code = 1,
    },
    };

    var accountResults = client.CreateAccounts(accounts);
    Debug.Assert(accountResults.Length == 2);
    Debug.Assert(accountResults[0].Status == CreateAccountStatus.Created);
    Debug.Assert(accountResults[1].Status == CreateAccountStatus.Created);

    // Create pending transfer.
    var transferPendingId = ID.Create();
    var transferResults = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = transferPendingId,
        DebitAccountId = accountDrId,
        CreditAccountId = accountCrId,
        Amount = 500,
        Ledger = 1,
        Code = 1,
        Flags = TransferFlags.Pending,
    }
    });
    Debug.Assert(transferResults.Length == 1);
    Debug.Assert(transferResults[0].Status == CreateTransferStatus.Created);

    // Fetch and validate pending account balances.
    accounts = client.LookupAccounts(new UInt128[] { accountDrId, accountCrId });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == accountDrId)
        {
            Debug.Assert(account.DebitsPosted == 0);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 500);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == accountCrId)
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

    // Post pending transfer.
    transferResults = client.CreateTransfers(new[] {
    new Transfer
    {
        Id = ID.Create(),
        PendingId = transferPendingId,
        DebitAccountId = accountDrId,
        CreditAccountId = accountCrId,
        Ledger = 1,
        Code = 1,
        // You can post any amount between zero and the pending transfer amount,
        // or void the two-phase transfer using the `VoidPendingTransfer` flag.
        Amount = 500,
        Flags = TransferFlags.PostPendingTransfer,
    }
    });
    Debug.Assert(transferResults.Length == 1);
    Debug.Assert(transferResults[0].Status == CreateTransferStatus.Created);

    // Fetch and validate final account balances.
    accounts = client.LookupAccounts(new UInt128[] { accountDrId, accountCrId });
    Debug.Assert(accounts.Length == 2);
    foreach (var account in accounts)
    {
        if (account.Id == accountDrId)
        {
            Debug.Assert(account.DebitsPosted == 500);
            Debug.Assert(account.CreditsPosted == 0);
            Debug.Assert(account.DebitsPending == 0);
            Debug.Assert(account.CreditsPending == 0);
        }
        else if (account.Id == accountCrId)
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
}
