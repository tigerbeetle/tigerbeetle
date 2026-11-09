import os

import tigerbeetle as tb

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    # Create accounts.
    account_dr_id = tb.id()
    account_cr_id = tb.id()

    account_results = client.create_accounts([
        tb.Account(
            id=account_dr_id,
            ledger=1,
            code=1,
        ),
        tb.Account(
            id=account_cr_id,
            ledger=1,
            code=1,
        ),
    ])

    print(account_results)
    assert len(account_results) == 2
    assert account_results[0].status == tb.CreateAccountStatus.CREATED
    assert account_results[1].status == tb.CreateAccountStatus.CREATED

    # Create pending transfer.
    transfer_pending_id = tb.id()
    transfers_results = client.create_transfers([
        tb.Transfer(
            id=transfer_pending_id,
            debit_account_id=account_dr_id,
            credit_account_id=account_cr_id,
            amount=500,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        )
    ])

    print(transfers_results)
    assert len(transfers_results) == 1
    assert transfers_results[0].timestamp > 0
    assert transfers_results[0].status == tb.CreateTransferStatus.CREATED

    # Fetch and validate pending account balances.
    accounts = client.lookup_accounts([account_dr_id, account_cr_id])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == account_dr_id:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 500
            assert account.credits_pending == 0
        elif account.id == account_cr_id:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 500
        else:
            raise Exception("Unexpected account: " + account)

    # Post pending transfer.
    transfers_results = client.create_transfers([
        tb.Transfer(
            id=tb.id(),
            debit_account_id=account_dr_id,
            credit_account_id=account_cr_id,
            pending_id=transfer_pending_id,
            ledger=1,
            code=1,
            # You can post any amount between zero and the pending transfer amount,
            # or void the two-phase transfer using the `VOID_PENDING_TRANSFER` flag.
            amount=500,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        ),
    ])
    print(transfers_results)
    assert len(transfers_results) == 1
    assert transfers_results[0].status == tb.CreateTransferStatus.CREATED

    # Fetch and validate final account balances.
    accounts = client.lookup_accounts([account_dr_id, account_cr_id])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == account_dr_id:
            assert account.debits_posted == 500
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        elif account.id == account_cr_id:
            assert account.debits_posted == 0
            assert account.credits_posted == 500
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        else:
            raise Exception("Unexpected account: " + account)

    print('ok')
