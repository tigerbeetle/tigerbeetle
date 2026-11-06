import os
import sys

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
