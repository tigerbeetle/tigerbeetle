import os

import tigerbeetle as tb

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    # Create two accounts.
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

    # Start a pending transfer
    transfers_results = client.create_transfers([
        tb.Transfer(
            id=1,
            debit_account_id=1,
            credit_account_id=2,
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

    # Validate accounts pending and posted debits/credits before finishing the two-phase transfer
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 500
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 500
        else:
            raise Exception("Unexpected account: " + account)

    # Create a second transfer simply posting the first transfer
    transfers_results = client.create_transfers([
        tb.Transfer(
            id=2,
            debit_account_id=1,
            credit_account_id=2,
            amount=500,
            pending_id=1,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        ),
    ])
    print(transfers_results)
    assert len(transfers_results) == 1
    assert transfers_results[0].status == tb.CreateTransferStatus.CREATED

    # Validate the contents of all transfers
    transfers = client.lookup_transfers([1, 2])
    assert len(transfers) == 2
    for transfer in transfers:
        if transfer.id == 1:
            assert transfer.flags & tb.TransferFlags.PENDING == tb.TransferFlags.PENDING
        elif transfer.id == 2:
            assert transfer.flags & tb.TransferFlags.POST_PENDING_TRANSFER == tb.TransferFlags.POST_PENDING_TRANSFER
        else:
            raise Exception("Unexpected transfer: " + transfer)


    # Validate accounts pending and posted debits/credits after finishing the two-phase transfer
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 500
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 500
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        else:
            raise Exception("Unexpected account: " + account)

    print('ok')
