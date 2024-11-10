import os

import tigerbeetle as tb

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    # Create two accounts.
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

    # Start five pending transfers.
    transfers = [
        tb.Transfer(
            id=1,
            debit_account_id=1,
            credit_account_id=2,
            amount=100,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        ),
        tb.Transfer(
            id=2,
            debit_account_id=1,
            credit_account_id=2,
            amount=200,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        ),
        tb.Transfer(
            id=3,
            debit_account_id=1,
            credit_account_id=2,
            amount=300,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        ),
        tb.Transfer(
            id=4,
            debit_account_id=1,
            credit_account_id=2,
            amount=400,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        ),
        tb.Transfer(
            id=5,
            debit_account_id=1,
            credit_account_id=2,
            amount=500,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.PENDING,
        ),
    ]
    transfer_errors = client.create_transfers(transfers)
    print(transfer_errors)
    assert len(transfer_errors) == 0


    # Validate accounts pending and posted debits/credits before
    # finishing the two-phase transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 1500
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 1500
        else:
            raise Exception("Unexpected account: " + account)


    # Create a 6th transfer posting the 1st transfer.
    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=6,
            debit_account_id=1,
            credit_account_id=2,
            amount=100,
            pending_id=1,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        )
    ])
    print(transfer_errors)
    assert len(transfer_errors) == 0

    # Validate account balances after posting 1st pending transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 100
            assert account.credits_posted == 0
            assert account.debits_pending == 1400
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 100
            assert account.debits_pending == 0
            assert account.credits_pending == 1400
        else:
            raise Exception("Unexpected account: " + account)

    # Create a 7th transfer voiding the 2d transfer.
    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=7,
            debit_account_id=1,
            credit_account_id=2,
            amount=200,
            pending_id=2,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
        )
    ])
    print(transfer_errors)
    assert len(transfer_errors) == 0

    # Validate account balances after voiding 2d pending transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 100
            assert account.credits_posted == 0
            assert account.debits_pending == 1200
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 100
            assert account.debits_pending == 0
            assert account.credits_pending == 1200
        else:
            raise Exception("Unexpected account: " + account)

    # Create a 8th transfer posting the 3rd transfer.
    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=8,
            debit_account_id=1,
            credit_account_id=2,
            amount=300,
            pending_id=3,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        )
    ])
    print(transfer_errors)
    assert len(transfer_errors) == 0

    # Validate account balances after posting 3rd pending transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 400
            assert account.credits_posted == 0
            assert account.debits_pending == 900
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 400
            assert account.debits_pending == 0
            assert account.credits_pending == 900
        else:
            raise Exception("Unexpected account: " + account)

    # Create a 9th transfer voiding the 4th transfer.
    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=9,
            debit_account_id=1,
            credit_account_id=2,
            amount=400,
            pending_id=4,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
        )
    ])
    print(transfer_errors)
    assert len(transfer_errors) == 0

    # Validate account balances after voiding 4th pending transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 400
            assert account.credits_posted == 0
            assert account.debits_pending == 500
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 400
            assert account.debits_pending == 0
            assert account.credits_pending == 500
        else:
            raise Exception("Unexpected account: " + account)

    # Create a 10th transfer posting the 5th transfer.
    transfer_errors = client.create_transfers([
        tb.Transfer(
            id=10,
            debit_account_id=1,
            credit_account_id=2,
            amount=500,
            pending_id=5,
            ledger=1,
            code=1,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        )
    ])
    print(transfer_errors)
    assert len(transfer_errors) == 0

    # Validate account balances after posting 5th pending transfer.
    accounts = client.lookup_accounts([1, 2])
    assert len(accounts) == 2
    for account in accounts:
        if account.id == 1:
            assert account.debits_posted == 900
            assert account.credits_posted == 0
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        elif account.id == 2:
            assert account.debits_posted == 0
            assert account.credits_posted == 900
            assert account.debits_pending == 0
            assert account.credits_pending == 0
        else:
            raise Exception("Unexpected account: " + account)

    print('ok')
