import os
import sys
import time
from dataclasses import asdict

import pytest

import tigerbeetle as tb
tb.configure_logging(debug=True)

replica_addresses = os.getenv("TB_ADDRESS")
if not replica_addresses:
    print('error: missing TB_ADDRESS environment variable')
    sys.exit(1)

@pytest.fixture
def client():
    client = tb.ClientSync(cluster_id=0, replica_addresses=replica_addresses)
    yield client
    client.close()

BATCH_MAX = 8189

# Test data
account_a = tb.Account(
    id=17,
    debits_pending=0,
    debits_posted=0,
    credits_pending=0,
    credits_posted=0,
    user_data_128=0,
    user_data_64=0,
    user_data_32=0,
    ledger=1,
    code=718,
    flags=0,
    timestamp=0
)
account_b = tb.Account(
    id=19,
    debits_pending=0,
    debits_posted=0,
    credits_pending=0,
    credits_posted=0,
    user_data_128=0,
    user_data_64=0,
    user_data_32=0,
    ledger=1,
    code=719,
    flags=0,
    timestamp=0
)

def test_range_check_code_on_account_to_be_u16(client):
    account = tb.Account(**{ **asdict(account_a), "id": 0, "code": 65535 + 1 })

    try:
        code_error = client.create_accounts([account])
    except tb.IntegerOverflowError:
        pass

    accounts = client.lookup_accounts([account.id])
    assert accounts == []

def test_create_accounts(client):
    errors = client.create_accounts([account_a])
    assert errors == []

def test_return_error_on_account(client):
    errors = client.create_accounts([account_a, account_b])

    assert len(errors) == 1
    assert errors[0] == tb.CreateAccountsResult(index=0, result=tb.CreateAccountResult.EXISTS)

def test_error_if_timestamp_is_not_set_to_0_on_account(client):
    account = { **asdict(account_a), "timestamp": 2, "id": 3 }
    errors = client.create_accounts([tb.Account(**account)])

    assert len(errors) == 1
    assert errors[0] == tb.CreateAccountsResult(
        index=0,
        result=tb.CreateAccountResult.TIMESTAMP_MUST_BE_ZERO
    )

def test_lookup_accounts(client):
    accounts = client.lookup_accounts([account_a.id, account_b.id])

    assert len(accounts) == 2
    account1 = accounts[0]
    assert account1.id == 17
    assert account1.credits_posted == 0
    assert account1.credits_pending == 0
    assert account1.debits_posted == 0
    assert account1.debits_pending == 0
    assert account1.user_data_128 == 0
    assert account1.user_data_64 == 0
    assert account1.user_data_32 == 0
    assert account1.code == 718
    assert account1.ledger == 1
    assert account1.flags == 0
    assert account1.timestamp > 0

    account2 = accounts[1]
    assert account2.id == 19
    assert account2.credits_posted == 0
    assert account2.credits_pending == 0
    assert account2.debits_posted == 0
    assert account2.debits_pending == 0
    assert account2.user_data_128 == 0
    assert account2.user_data_64 == 0
    assert account2.user_data_32 == 0
    assert account2.code == 719
    assert account2.ledger == 1
    assert account2.flags == 0
    assert account2.timestamp > 0

def test_create_a_transfer(client):
    transfer = tb.Transfer(
        id=1,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=100,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=0,
        ledger=1,
        code=1,
        flags=0,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([transfer])
    assert errors == []

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 100
    assert accounts[0].credits_pending == 0
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 100
    assert accounts[1].debits_pending == 0

def test_create_a_two_phase_transfer(client):
    transfer = tb.Transfer(
        id=2,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=50,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=int(2e9),
        ledger=1,
        code=1,
        flags=tb.TransferFlags.PENDING,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([transfer])
    assert errors == []

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 100
    assert accounts[0].credits_pending == 50
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 100
    assert accounts[1].debits_pending == 50

    # Lookup the transfer:
    transfers = client.lookup_transfers([transfer.id])
    assert len(transfers) == 1
    assert transfers[0].id == 2
    assert transfers[0].debit_account_id == account_b.id
    assert transfers[0].credit_account_id == account_a.id
    assert transfers[0].amount == 50
    assert transfers[0].user_data_128 == 0
    assert transfers[0].user_data_64 == 0
    assert transfers[0].user_data_32 == 0
    assert transfers[0].timeout > 0
    assert transfers[0].code == 1
    assert transfers[0].flags == 2
    assert transfers[0].timestamp > 0

def test_post_a_two_phase_transfer(client):
    commit = tb.Transfer(
        id=3,
        debit_account_id=0,
        credit_account_id=0,
        amount=tb.amount_max,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=2,# must match the id of the pending transfer
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.POST_PENDING_TRANSFER,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([commit])
    assert errors == []

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 150
    assert accounts[0].credits_pending == 0
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 150
    assert accounts[1].debits_pending == 0

def test_reject_a_two_phase_transfer(client):
    # Create a two-phase transfer:
    transfer = tb.Transfer(
        id=4,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=50,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=int(1e9),
        ledger=1,
        code=1,
        flags=tb.TransferFlags.PENDING,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )
    transfer_errors = client.create_transfers([transfer])
    assert transfer_errors == []

    # send in the reject
    reject = tb.Transfer(
        id=5,
        debit_account_id=0,
        credit_account_id=0,
        amount=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=4, # must match the id of the pending transfer
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([reject])
    assert errors == []

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 150
    assert accounts[0].credits_pending == 0
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 150
    assert accounts[1].debits_pending == 0

def test_link_transfers(client):
    transfer1 = tb.Transfer(
        id=6,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=100,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.LINKED, # points to transfer2
        timestamp=0, # will be set correctly by the TigerBeetle server
    )
    transfer2 = tb.Transfer(
        id=6,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=100,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=0,
        ledger=1,
        code=1,
        # Does not have linked flag as it is the end of the chain.
        # This will also cause it to fail as this is now a duplicate with different flags
        flags=0,
        timestamp=0, # will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([transfer1, transfer2])
    assert len(errors) == 2
    assert errors[0] == tb.CreateTransfersResult(
        index=0,
        result=tb.CreateTransferResult.LINKED_EVENT_FAILED
    )
    assert errors[1] == tb.CreateTransfersResult(
        index=1,
        result=tb.CreateTransferResult.EXISTS_WITH_DIFFERENT_FLAGS
    )

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 150
    assert accounts[0].credits_pending == 0
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 150
    assert accounts[1].debits_pending == 0

def test_cannot_void_an_expired_transfer(client):
    # Create a two-phase transfer:
    transfer = tb.Transfer(
        id=6,
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=50,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=1,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.PENDING,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )
    transfer_errors = client.create_transfers([transfer])
    assert transfer_errors == []

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 150
    assert accounts[0].credits_pending == 50
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 150
    assert accounts[1].debits_pending == 50

    # We need to wait 1s for the server to expire the transfer, however the
    # server can pulse the expiry operation anytime after the timeout,
    # so adding an extra delay to avoid flaky tests.
    extra_wait_time = 0.25
    time.sleep(transfer.timeout + extra_wait_time)

    # Looking up the accounts again for the updated balance.
    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert accounts[0].credits_posted == 150
    assert accounts[0].credits_pending == 0
    assert accounts[0].debits_posted == 0
    assert accounts[0].debits_pending == 0

    assert accounts[1].credits_posted == 0
    assert accounts[1].credits_pending == 0
    assert accounts[1].debits_posted == 150
    assert accounts[1].debits_pending == 0

    # send in the reject
    reject = tb.Transfer(
        id=7,
        debit_account_id=0,
        credit_account_id=0,
        amount=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=6, # must match the id of the pending transfer
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
        timestamp=0, # this will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([reject])
    assert len(errors) == 1
    assert errors[0] == tb.CreateTransfersResult(
        index=0,
        result=tb.CreateTransferResult.PENDING_TRANSFER_EXPIRED
    )

def test_close_accounts(client):
    closing_transfer = tb.Transfer(
        id=tb.id(),
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.CLOSING_DEBIT | tb.TransferFlags.CLOSING_CREDIT | tb.TransferFlags.PENDING,
        timestamp=0, # will be set correctly by the TigerBeetle server
    )
    errors = client.create_transfers([closing_transfer])
    assert len(errors) == 0

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert account_a.flags != accounts[0].flags
    assert (accounts[0].flags & tb.AccountFlags.CLOSED) != tb.AccountFlags.NONE

    assert account_b.flags != accounts[1].flags
    assert (accounts[1].flags & tb.AccountFlags.CLOSED) != tb.AccountFlags.NONE

    voiding_transfer = tb.Transfer(
        id=tb.id(),
        debit_account_id=account_b.id,
        credit_account_id=account_a.id,
        amount=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
        pending_id=closing_transfer.id,
        timestamp=0, # will be set correctly by the TigerBeetle server
    )

    errors = client.create_transfers([voiding_transfer])
    assert len(errors) == 0

    accounts = client.lookup_accounts([account_a.id, account_b.id])
    assert len(accounts) == 2
    assert account_a.flags == accounts[0].flags
    assert (accounts[0].flags & tb.AccountFlags.CLOSED) == tb.AccountFlags.NONE

    assert account_b.flags == accounts[1].flags
    assert (accounts[1].flags & tb.AccountFlags.CLOSED) ==  tb.AccountFlags.NONE

def test_get_account_transfers(client):
    accountC = tb.Account(
        id=21,
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=1,
        code=718,
        flags=tb.AccountFlags.HISTORY,
        timestamp=0
    )
    account_errors = client.create_accounts([accountC])
    assert account_errors == []

    transfers_created = []
    # Create transfers where the new account is either the debit or credit account:
    for i in range(10):
        transfers_created.append(tb.Transfer(
            id=i + 10000,
            debit_account_id=accountC.id if i % 2 == 0 else account_a.id,
            credit_account_id=account_b.id if i % 2 == 0 else accountC.id,
            amount=100,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            pending_id=0,
            timeout=0,
            ledger=1,
            code=1,
            flags=0,
            timestamp=0,
        ))

    transfers_created_result = client.create_transfers(transfers_created)
    assert transfers_created_result == []

    # Query all transfers for accountC:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)
    assert len(transfers) == len(transfers_created)
    assert len(account_balances) == len(transfers)

    timestamp = 0
    for i, transfer in enumerate(transfers):
        assert timestamp < transfer.timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query only the debit transfers for accountC, descending:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.DEBITS |  tb.AccountFilterFlags.REVERSED,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    timestamp = 1 << 64
    for i, transfer in enumerate(transfers):
        assert transfer.timestamp < timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query only the credit transfers for accountC, descending:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS |  tb.AccountFilterFlags.REVERSED,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    timestamp = 1 << 64
    for i, transfer in enumerate(transfers):
        assert transfer.timestamp < timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query the first 5 transfers for accountC:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    timestamp = 0
    for i, transfer in enumerate(transfers):
        assert timestamp < transfer.timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query the next 5 transfers for accountC, with pagination:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=timestamp + 1,
        timestamp_max=0,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    for i, transfer in enumerate(transfers):
        assert timestamp < transfer.timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query again, no more transfers should be found:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=timestamp + 1,
        timestamp_max=0,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert transfers == []
    assert len(account_balances) == len(transfers)

    # Query the first 5 transfers for accountC ORDER BY DESC:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS | tb.AccountFilterFlags.REVERSED,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    timestamp = 1 << 64
    for i, transfer in enumerate(transfers):
        assert timestamp > transfer.timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query the next 5 transfers for accountC, with pagination:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=timestamp - 1,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS | tb.AccountFilterFlags.REVERSED,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert len(transfers) == len(transfers_created) // 2
    assert len(account_balances) == len(transfers)

    for i, transfer in enumerate(transfers):
        assert timestamp > transfer.timestamp
        timestamp = transfer.timestamp

        assert account_balances[i].timestamp == transfer.timestamp

    # Query again, no more transfers should be found:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=timestamp - 1,
        limit=len(transfers_created) // 2,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS | tb.AccountFilterFlags.REVERSED,
    )
    transfers = client.get_account_transfers(filter)
    account_balances = client.get_account_balances(filter)

    assert transfers == []
    assert len(account_balances) == len(transfers)

    # Invalid account:
    filter = tb.AccountFilter(
        account_id=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Invalid timestamp min:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=(1 << 64) - 1, # ulong max value
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Invalid timestamp max:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=(1 << 64) - 1, # ulong max value
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Invalid timestamp range:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=(1 << 64) - 2, # ulong max - 1
        timestamp_max=1,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Zero limit:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=0,
        flags=tb.AccountFilterFlags.CREDITS | tb.AccountFilterFlags.DEBITS,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Empty flags:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.AccountFilterFlags.NONE,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []

    # Invalid flags:
    filter = tb.AccountFilter(
        account_id=accountC.id,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=0xFFFF,
    )
    assert client.get_account_transfers(filter) == []
    assert client.get_account_balances(filter) == []


def test_query_accounts(client):
    accounts = []
    # Create transfers:
    for i in range(10):
        accounts.append(tb.Account(
            id=tb.id(),
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=1000 if i % 2 == 0 else 2000,
            user_data_64=100 if i % 2 == 0 else 200,
            user_data_32=10 if i % 2 == 0 else 20,
            ledger=1,
            code=999,
            flags=tb.AccountFlags.NONE,
            timestamp=0,
        ))

    create_accounts_result = client.create_accounts(accounts)
    assert create_accounts_result == []

    # Querying accounts where:
    # `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
    # AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
    filter = tb.QueryFilter(
        user_data_128=1000,
        user_data_64=100,
        user_data_32=10,
        ledger=1,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_accounts(filter)
    assert len(query) == 5

    timestamp = 0
    for account in query:
        assert timestamp < account.timestamp
        timestamp = account.timestamp

        assert account.user_data_128 == filter.user_data_128
        assert account.user_data_64 == filter.user_data_64
        assert account.user_data_32 == filter.user_data_32
        assert account.ledger == filter.ledger
        assert account.code == filter.code

    # Querying accounts where:
    # `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
    # AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
    filter = tb.QueryFilter(
        user_data_128=2000,
        user_data_64=200,
        user_data_32=20,
        ledger=1,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.REVERSED,
    )
    query = client.query_accounts(filter)
    assert len(query) == 5

    timestamp = 1 << 64
    for account in query:
        assert timestamp > account.timestamp
        timestamp = account.timestamp

        assert account.user_data_128 == filter.user_data_128
        assert account.user_data_64 == filter.user_data_64
        assert account.user_data_32 == filter.user_data_32
        assert account.ledger == filter.ledger
        assert account.code == filter.code

    # Querying accounts where:
    # `code=999 ORDER BY timestamp ASC`
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_accounts(filter)
    assert len(query) == 10

    timestamp = 0
    for account in query:
        assert timestamp < account.timestamp
        timestamp = account.timestamp

        assert account.code == filter.code

    # Querying accounts where:
    # `code=999 ORDER BY timestamp DESC LIMIT 5`.
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=5,
        flags=tb.QueryFilterFlags.REVERSED,
    )

    # First 5 items:
    query = client.query_accounts(filter)
    assert len(query) == 5

    timestamp = 1 << 64
    for account in query:
        assert timestamp > account.timestamp
        timestamp = account.timestamp

        assert account.code == filter.code

    # Next 5 items:
    filter.timestamp_max = timestamp - 1
    query = client.query_accounts(filter)
    assert len(query) == 5

    for account in query:
        assert timestamp > account.timestamp
        timestamp = account.timestamp

        assert account.code == filter.code

    # No more results:
    filter.timestamp_max = timestamp - 1
    query = client.query_accounts(filter)
    assert len(query) == 0

    # Not found:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=200,
        user_data_32=10,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_accounts(filter)
    assert len(query) == 0

def test_query_transfers(client):
    account = tb.Account(
        id=tb.id(),
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=1,
        code=718,
        flags=tb.AccountFlags.NONE,
        timestamp=0
    )
    create_accounts_result = client.create_accounts([account])
    assert create_accounts_result == []

    transfers_created = []
    # Create transfers:
    for i in range (10):
        transfers_created.append(tb.Transfer(
            id=tb.id(),
            debit_account_id=account.id if i % 2 == 0 else account_a.id,
            credit_account_id=account_b.id if i % 2 == 0 else account.id,
            amount=100,
            user_data_128=1000 if i % 2 == 0 else 2000,
            user_data_64=100 if i % 2 == 0 else 200,
            user_data_32=10 if i % 2 == 0 else 20,
            pending_id=0,
            timeout=0,
            ledger=1,
            code=999,
            flags=0,
            timestamp=0,
        ))

    create_transfers_result = client.create_transfers(transfers_created)
    assert create_transfers_result == []

    # Querying transfers where:
    # `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
    # AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
    filter = tb.QueryFilter(
        user_data_128=1000,
        user_data_64=100,
        user_data_32=10,
        ledger=1,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_transfers(filter)
    assert len(query) == 5

    timestamp = 0
    for transfer in query:
        assert timestamp < transfer.timestamp
        timestamp = transfer.timestamp

        assert transfer.user_data_128 == filter.user_data_128
        assert transfer.user_data_64 == filter.user_data_64
        assert transfer.user_data_32 == filter.user_data_32
        assert transfer.ledger == filter.ledger
        assert transfer.code == filter.code

    # Querying transfers where:
    # `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
    # AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
    filter = tb.QueryFilter(
        user_data_128=2000,
        user_data_64=200,
        user_data_32=20,
        ledger=1,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.REVERSED,
    )
    query = client.query_transfers(filter)
    assert len(query) == 5

    timestamp = 1 << 64
    for transfer in query:
        assert timestamp > transfer.timestamp
        timestamp = transfer.timestamp

        assert transfer.user_data_128 == filter.user_data_128
        assert transfer.user_data_64 == filter.user_data_64
        assert transfer.user_data_32 == filter.user_data_32
        assert transfer.ledger == filter.ledger
        assert transfer.code == filter.code

    # Querying transfers where:
    # `code=999 ORDER BY timestamp ASC`
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_transfers(filter)
    assert len(query) == 10

    timestamp = 0
    for transfer in query:
        assert timestamp < transfer.timestamp
        timestamp = transfer.timestamp

        assert transfer.code == filter.code

    # Querying transfers where:
    # `code=999 ORDER BY timestamp DESC LIMIT 5`.
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=999,
        timestamp_min=0,
        timestamp_max=0,
        limit=5,
        flags=tb.QueryFilterFlags.REVERSED,
    )

    # First 5 items:
    query = client.query_transfers(filter)
    assert len(query) == 5

    timestamp = 1 << 64
    for transfer in query:
        assert timestamp > transfer.timestamp
        timestamp = transfer.timestamp

        assert transfer.code == filter.code

    # Next 5 items:
    filter.timestamp_max = timestamp - 1
    query = client.query_transfers(filter)
    assert len(query) == 5

    for transfer in query:
        assert timestamp > transfer.timestamp
        timestamp = transfer.timestamp

        assert transfer.code == filter.code

    # No more results:
    filter.timestamp_max = timestamp - 1
    query = client.query_transfers(filter)
    assert len(query) == 0

    # Not found:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=200,
        user_data_32=10,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    query = client.query_transfers(filter)
    assert len(query) == 0

def test_query_with_invalid_filter(client):
    # Invalid timestamp min:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=(1 << 64) - 1, # ulong max value
        timestamp_max=0,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    assert client.query_accounts(filter) == []
    assert client.query_transfers(filter) == []

    # Invalid timestamp max:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=(1 << 64) - 1, # ulong max value,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    assert client.query_accounts(filter) == []
    assert client.query_transfers(filter) == []

    # Invalid timestamp range:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=(1 << 64) - 2, # ulong max - 1
        timestamp_max=1,
        limit=BATCH_MAX,
        flags=tb.QueryFilterFlags.NONE,
    )
    assert client.query_accounts(filter) == []
    assert client.query_transfers(filter) == []

    # Zero limit:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=0,
        flags=tb.QueryFilterFlags.NONE,
    )
    assert client.query_accounts(filter) == []
    assert client.query_transfers(filter) == []

    # Invalid flags:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=0,
        flags=0xFFFF,
    )
    assert client.query_accounts(filter) == []
    assert client.query_transfers(filter) == []

def test_import_accounts_and_transfers(client):
    account_tmp = tb.Account(
        id=tb.id(),
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=1,
        code=718,
        flags=0,
        timestamp=0
    )
    accounts_errors = client.create_accounts([account_tmp])
    assert accounts_errors == []

    account_lookup = client.lookup_accounts([account_tmp.id])
    assert len(account_lookup) == 1
    timestamp_max = account_lookup[0].timestamp

    # Wait 10 ms so we can use the account's timestamp as the reference for past time
    # after the last object inserted.
    time.sleep(0.01)

    account_a = tb.Account(
        id=tb.id(),
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=1,
        code=718,
        flags=tb.AccountFlags.IMPORTED,
        timestamp=timestamp_max + 1 # user-defined timestamp
    )
    account_b = tb.Account(
        id=tb.id(),
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=1,
        code=718,
        flags=tb.AccountFlags.IMPORTED,
        timestamp=timestamp_max + 2 # user-defined timestamp
    )
    accounts_errors = client.create_accounts([account_a, account_b])
    assert accounts_errors == []

    account_lookup = client.lookup_accounts([account_a.id, account_b.id])
    assert len(account_lookup) == 2
    assert account_lookup[0].timestamp == account_a.timestamp
    assert account_lookup[1].timestamp == account_b.timestamp

    transfer = tb.Transfer(
        id=tb.id(),
        debit_account_id=account_a.id,
        credit_account_id=account_b.id,
        amount=100,
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        pending_id=0,
        timeout=0,
        ledger=1,
        code=1,
        flags=tb.TransferFlags.IMPORTED,
        timestamp=timestamp_max + 3, # user-defined timestamp.
    )

    errors = client.create_transfers([transfer])
    assert errors == []

    transfers = client.lookup_transfers([transfer.id])
    assert len(transfers) == 1
    assert transfers[0].timestamp == timestamp_max + 3

def test_accept_zero_length_create_accounts(client):
    errors = client.create_accounts([])
    assert errors == []

def test_accept_zero_length_create_transfers(client):
    errors = client.create_transfers([])
    assert errors == []

def test_accept_zero_length_lookup_accounts(client):
    accounts = client.lookup_accounts([])
    assert accounts == []

def test_accept_zero_length_lookup_transfers(client):
    transfers = client.lookup_transfers([])
    assert transfers == []

def test_uint128(client):
    import json
    import subprocess

    account = tb.Account(
        id=2**128-10,
        user_data_128=2**128-1024,
        user_data_64=2**64-1024,
        ledger=1,
        code=1
    )
    result = client.create_accounts([account])
    assert len(result) == 0

    accounts = client.lookup_accounts([account.id])
    assert len(accounts) == 1
    assert accounts[0] == tb.Account(
        id=340282366920938463463374607431768211446,
        debits_pending=0,
        debits_posted=0,
        credits_pending=0,
        credits_posted=0,
        user_data_128=340282366920938463463374607431768210432,
        user_data_64=18446744073709550592,
        user_data_32=0,
        ledger=1,
        code=1,
        timestamp=accounts[0].timestamp,
        flags=tb.AccountFlags.NONE
    )

    expected_repl_response = {
        "id": "340282366920938463463374607431768211446",
        "debits_pending": "0",
        "debits_posted": "0",
        "credits_pending": "0",
        "credits_posted": "0",
        "user_data_128": "340282366920938463463374607431768210432",
        "user_data_64": "18446744073709550592",
        "user_data_32": "0",
        "ledger": "1",
        "code": "1",
        "flags": [],
    }
    expected_repl_response["timestamp"] = str(accounts[0].timestamp)

    expected_repl_response_as_account = tb.Account()
    for k, v in expected_repl_response.items():
        if k == "flags":
            v = tb.AccountFlags.NONE
        else:
            v = int(v)
        setattr(expected_repl_response_as_account, k, v)

    assert accounts[0] == expected_repl_response_as_account

    tigerbeetle = os.getenv("TIGERBEETLE_BINARY", "tigerbeetle")
    repl_output = subprocess.run(
        [
            tigerbeetle,
            "repl",
            "--cluster=0",
            "--addresses=" + replica_addresses,
            "--command=lookup_accounts id=340282366920938463463374607431768211446"
        ],
        check=True,
        capture_output=True
    )
    assert json.loads(repl_output.stdout) == expected_repl_response


def test_ids_random():
    """IDs are different from repeated invocations of the function."""
    samples = [tb.id() for _ in range(10_000)]
    assert len(samples) == len(set(samples))

def test_ids_increasing():
    """IDs are expected to be strictly increasing."""
    id_previous = tb.id()
    for i in range(10_000):
        id = tb.id()
        assert id_previous < id
        id_previous = id
