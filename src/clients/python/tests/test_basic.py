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
    account = tb.Account(**{ **asdict(account_a), "id": tb.id(), "code": 65535 + 1 })

    with pytest.raises(tb.IntegerOverflowError):
        client.create_accounts([account])

    accounts = client.lookup_accounts([account.id])
    assert accounts == []

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
    account_results = client.create_accounts([accountC, account_a, account_b])
    assert len(account_results) == 3
    for result in account_results:
        assert result.timestamp > 0
        assert result.status == tb.CreateAccountStatus.CREATED

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

    transfers_results = client.create_transfers(transfers_created)
    assert len(transfers_results) == len(transfers_created)
    for result in transfers_results:
        assert result.timestamp > 0
        assert result.status == tb.CreateTransferStatus.CREATED

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


def test_query_with_invalid_filter(client):
    # Invalid flags:
    filter = tb.QueryFilter(
        user_data_128=0,
        user_data_64=0,
        user_data_32=0,
        ledger=0,
        code=0,
        timestamp_min=0,
        timestamp_max=0,
        limit=BATCH_MAX,
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
    account_results = client.create_accounts([account_tmp])
    assert len(account_results) == 1
    account_results[0].timestamp > 0
    account_results[0].status == tb.CreateAccountStatus.CREATED

    timestamp_max = account_results[0].timestamp

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
    account_results = client.create_accounts([account_a, account_b])
    assert len(account_results) == 2
    account_results[0].timestamp == account_a.timestamp
    account_results[0].status == tb.CreateAccountStatus.CREATED
    account_results[1].timestamp == account_b.timestamp
    account_results[1].status == tb.CreateAccountStatus.CREATED

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

    transfers_results = client.create_transfers([transfer])
    assert len(transfers_results) == 1
    assert transfers_results[0].timestamp == transfer.timestamp
    assert transfers_results[0].status == tb.CreateTransferStatus.CREATED

    transfers = client.lookup_transfers([transfer.id])
    assert len(transfers) == 1
    assert transfers[0].timestamp == transfers_results[0].timestamp

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
    results = client.create_accounts([account])
    assert len(results) == 1
    assert results[0].timestamp > 0
    assert results[0].status == tb.CreateAccountStatus.CREATED

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
        timestamp=results[0].timestamp,
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
    expected_repl_response["timestamp"] = str(results[0].timestamp)

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
