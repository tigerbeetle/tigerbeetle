import os

import tigerbeetle as tb

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    accounts_results = client.create_accounts([
        tb.Account(
            id=1,
            ledger=1,
            code=1,
        ),
        tb.Account(
            id=2,
            ledger=1,
            code=1,
        ),
    ])

    print(accounts_results)
    assert len(accounts_results) == 2
    assert accounts_results[0].status == tb.CreateAccountStatus.CREATED
    assert accounts_results[1].status == tb.CreateAccountStatus.CREATED

    transfers_results = client.create_transfers([
        tb.Transfer(
            id=1,
            debit_account_id=1,
            credit_account_id=2,
            amount=10,
            ledger=1,
            code=1,
        ),
    ])

    print(transfers_results)
    assert len(transfers_results) == 1
    assert transfers_results[0].status == tb.CreateTransferStatus.CREATED

    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 10
            assert account.credits_posted == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 10
        else:
            raise Exception("Unexpected account: " + account)

    print("ok")
