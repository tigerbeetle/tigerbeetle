import os

import tigerbeetle as tb

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    account_errors = client.create_accounts([
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

    print(account_errors)
    assert len(account_errors) == 0

    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=1,
            debit_account_id=1,
            credit_account_id=2,
            amount=10,
            ledger=1,
            code=1,
        ),
    ])

    print(transfer_errors)
    assert len(transfer_errors) == 0

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
