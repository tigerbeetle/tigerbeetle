# section:imports
import os

import tigerbeetle as tb

print("Import OK!")

# To enable debug logging, via Python's built in logging module:
# logging.basicConfig(level=logging.DEBUG)
# tb.configure_logging(debug=True)
# endsection:imports

# Need to wrap in an async function for the async with to be valid Python.
async def example_init():
    # section:client
    with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
        # Use the client.
        pass

    # Alternatively:
    async with tb.ClientAsync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
        # Use the client, async!
        pass
    # endsection:client

# The examples currently throws because the batch is actually invalid (most of fields are
# undefined). Ideally, we prepare a correct batch here while keeping the syntax compact,
# for the example, but for the time being lets prioritize a readable example and just
# swallow the error.

with tb.ClientSync(cluster_id=0, replica_addresses=os.getenv("TB_ADDRESS", "3000")) as client:
    try:
        # section:create-accounts
        account = tb.Account(
            id=tb.id(), # TigerBeetle time-based ID.
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
            timestamp=0,
        )

        account_errors = client.create_accounts([account])
        # Error handling omitted.
        # endsection:create-accounts
    except:
        raise

    try:
        # section:account-flags
        account0 = tb.Account(
            id=100,
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            ledger=1,
            code=1,
            timestamp=0,
            flags=tb.AccountFlags.LINKED | tb.AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS,
        )
        account1 = tb.Account(
            id=101,
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            ledger=1,
            code=1,
            timestamp=0,
            flags=tb.AccountFlags.HISTORY,
        )

        account_errors = client.create_accounts([account0, account1])
        # Error handling omitted.
        # endsection:account-flags
    except:
        raise

    try:
        # section:create-accounts-errors
        account0 = tb.Account(
            id=102,
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            ledger=1,
            code=1,
            timestamp=0,
            flags=0,
        )
        account1 = tb.Account(
            id=103,
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            ledger=1,
            code=1,
            timestamp=0,
            flags=0,
        )
        account2 = tb.Account(
            id=104,
            debits_pending=0,
            debits_posted=0,
            credits_pending=0,
            credits_posted=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            ledger=1,
            code=1,
            timestamp=0,
            flags=0,
        )

        account_errors = client.create_accounts([account0, account1, account2])
        for error in account_errors:
            if error.result == tb.CreateAccountResult.EXISTS:
                print(f"Batch account at {error.index} already exists.")
            else:
                print(f"Batch account at ${error.index} failed to create: {error.result}.")
        # endsection:create-accounts-errors
    except:
        raise

    try:
        # section:lookup-accounts
        accounts = client.lookup_accounts([100, 101])
        # endsection:lookup-accounts
    except:
        raise

    try:
        # section:create-transfers
        transfers = [tb.Transfer(
            id=tb.id(), # TigerBeetle time-based ID.
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=0,
            timestamp=0,
        )]

        transfer_errors = client.create_transfers(transfers)
        # Error handling omitted.
        # endsection:create-transfers
    except:
        raise

    try:
        # section:create-transfers-errors
        batch = [tb.Transfer(
            id=1,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=0,
            timestamp=0,
        ),
            tb.Transfer(
            id=2,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=0,
            timestamp=0,
        ),
            tb.Transfer(
            id=3,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=0,
            timestamp=0,
        )]

        transfer_errors = client.create_transfers(batch)
        for error in transfer_errors:
            if error.result == tb.CreateTransferResult.EXISTS:
                print(f"Batch transfer at {error.index} already exists.")
            else:
                print(f"Batch transfer at {error.index} failed to create: {error.result}.")
        # endsection:create-transfers-errors
    except:
        raise

    try:
        # section:no-batch
        batch = [] # Array of transfer to create.
        for transfer in batch:
            transfer_errors = client.create_transfers([transfer])
            # Error handling omitted.
        # endsection:no-batch
    except:
        raise

    try:
        # section:batch
        batch = [] # Array of transfer to create.
        BATCH_SIZE = 8189 #FIXME
        for i in range(0, len(batch), BATCH_SIZE):
            transfer_errors = client.create_transfers(
                batch[i:min(len(batch), i + BATCH_SIZE)],
            )
            # Error handling omitted.
        # endsection:batch
    except:
        raise

    try:
        # section:transfer-flags-link
        transfer0 = tb.Transfer(
            id=4,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=tb.TransferFlags.LINKED,
            timestamp=0,
        )
        transfer1 = tb.Transfer(
            id=5,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=0,
            timestamp=0,
        )

        # Create the transfer
        transfer_errors = client.create_transfers([transfer0, transfer1])
        # Error handling omitted.
        # endsection:transfer-flags-link
    except:
        raise

    try:
        # section:transfer-flags-post
        transfer0 = tb.Transfer(
            id=6,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=tb.TransferFlags.PENDING,
            timestamp=0,
        )

        transfer_errors = client.create_transfers([transfer0])
        # Error handling omitted.

        transfer1 = tb.Transfer(
            id=7,
            debit_account_id=102,
            credit_account_id=103,
            # Post the entire pending amount.
            amount=tb.amount_max,
            pending_id=6,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=tb.TransferFlags.POST_PENDING_TRANSFER,
            timestamp=0,
        )

        transfer_errors = client.create_transfers([transfer1])
        # Error handling omitted.
        # endsection:transfer-flags-post
    except:
        raise

    try:
        # section:transfer-flags-void
        transfer0 = tb.Transfer(
            id=8,
            debit_account_id=102,
            credit_account_id=103,
            amount=10,
            pending_id=0,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=tb.TransferFlags.PENDING,
            timestamp=0,
        )

        transfer_errors = client.create_transfers([transfer0])
        # Error handling omitted.

        transfer1 = tb.Transfer(
            id=9,
            debit_account_id=102,
            credit_account_id=103,
            amount=0,
            pending_id=8,
            user_data_128=0,
            user_data_64=0,
            user_data_32=0,
            timeout=0,
            ledger=1,
            code=720,
            flags=tb.TransferFlags.VOID_PENDING_TRANSFER,
            timestamp=0,
        )

        transfer_errors = client.create_transfers([transfer1])
        # Error handling omitted.
        # endsection:transfer-flags-void
    except:
        raise

    try:
        # section:lookup-transfers
        transfers = client.lookup_transfers([1, 2])
        # endsection:lookup-transfers
    except:
        raise

    try:
        # section:get-account-transfers
        filter = tb.AccountFilter(
            account_id=2,
            user_data_128=0, # No filter by UserData.
            user_data_64=0,
            user_data_32=0,
            code=0, # No filter by Code.
            timestamp_min=0, # No filter by Timestamp.
            timestamp_max=0, # No filter by Timestamp.
            limit=10, # Limit to ten transfers at most.
            flags=tb.AccountFilterFlags.DEBITS | # Include transfer from the debit side.
            tb.AccountFilterFlags.CREDITS | # Include transfer from the credit side.
            tb.AccountFilterFlags.REVERSED, # Sort by timestamp in reverse-chronological order.
        )

        account_transfers = client.get_account_transfers(filter)
        # endsection:get-account-transfers
    except:
        raise

    try:
        # section:get-account-balances
        filter = tb.AccountFilter(
            account_id=2,
            user_data_128=0, # No filter by UserData.
            user_data_64=0,
            user_data_32=0,
            code=0, # No filter by Code.
            timestamp_min=0, # No filter by Timestamp.
            timestamp_max=0, # No filter by Timestamp.
            limit=10, # Limit to ten balances at most.
            flags=tb.AccountFilterFlags.DEBITS | # Include transfer from the debit side.
            tb.AccountFilterFlags.CREDITS | # Include transfer from the credit side.
            tb.AccountFilterFlags.REVERSED, # Sort by timestamp in reverse-chronological order.
        )

        account_balances = client.get_account_balances(filter)
        # endsection:get-account-balances
    except:
        raise

    try:
        # section:query-accounts
        query_filter = tb.QueryFilter(
            user_data_128=1000, # Filter by UserData.
            user_data_64=100,
            user_data_32=10,
            code=1, # Filter by Code.
            ledger=0, # No filter by Ledger.
            timestamp_min=0, # No filter by Timestamp.
            timestamp_max=0, # No filter by Timestamp.
            limit=10, # Limit to ten accounts at most.
            flags=tb.QueryFilterFlags.REVERSED, # Sort by timestamp in reverse-chronological order.
        )

        query_accounts = client.query_accounts(query_filter)
        # endsection:query-accounts
    except:
        raise

    try:
        # section:query-transfers
        query_filter = tb.QueryFilter(
            user_data_128=1000, # Filter by UserData.
            user_data_64=100,
            user_data_32=10,
            code=1, # Filter by Code.
            ledger=0, # No filter by Ledger.
            timestamp_min=0, # No filter by Timestamp.
            timestamp_max=0, # No filter by Timestamp.
            limit=10, # Limit to ten transfers at most.
            flags=tb.QueryFilterFlags.REVERSED, # Sort by timestamp in reverse-chronological order.
        )

        query_transfers = client.query_transfers(query_filter)
        # endsection:query-transfers
    except:
        raise

    try:
        # section:linked-events
        batch = [] # List of tb.Transfers to create.
        linkedFlag = 0
        linkedFlag |= tb.TransferFlags.LINKED

        # An individual transfer (successful):
        batch.append(tb.Transfer(id=1))

        # A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
        batch.append(tb.Transfer(id=2, flags=linkedFlag)) # Commit/rollback.
        batch.append(tb.Transfer(id=3, flags=linkedFlag)) # Commit/rollback.
        batch.append(tb.Transfer(id=2, flags=linkedFlag)) # Fail with exists
        batch.append(tb.Transfer(id=4, flags=0)) # Fail without committing.

        # An individual transfer (successful):
        # This should not see any effect from the failed chain above.
        batch.append(tb.Transfer(id=2, flags=0 ))

        # A chain of 2 transfers (the first transfer fails the chain):
        batch.append(tb.Transfer(id=2, flags=linkedFlag))
        batch.append(tb.Transfer(id=3, flags=0))

        # A chain of 2 transfers (successful):
        batch.append(tb.Transfer(id=3, flags=linkedFlag))
        batch.append(tb.Transfer(id=4, flags=0))

        transfer_errors = client.create_transfers(batch)
        # Error handling omitted.
        # endsection:linked-events
    except:
        raise

    try:
        # section:imported-events
        # External source of time.
        historical_timestamp = 0
        # Events loaded from an external source.
        historical_accounts = [] # Loaded from an external source.
        historical_transfers = [] # Loaded from an external source.

        # First, load and import all accounts with their timestamps from the historical source.
        accounts = []
        for index, account in enumerate(historical_accounts):
            # Set a unique and strictly increasing timestamp.
            historical_timestamp += 1
            account.timestamp = historical_timestamp
            # Set the account as `imported`.
            account.flags = tb.AccountFlags.IMPORTED
            # To ensure atomicity, the entire batch (except the last event in the chain)
            # must be `linked`.
            if index < len(historical_accounts) - 1:
                account.flags |= tb.AccountFlags.LINKED

            accounts.append(account)

        account_errors = client.create_accounts(accounts)
        # Error handling omitted.

        # The, load and import all transfers with their timestamps from the historical source.
        transfers = []
        for index, transfer in enumerate(historical_transfers):
            # Set a unique and strictly increasing timestamp.
            historical_timestamp += 1
            transfer.timestamp = historical_timestamp
            # Set the account as `imported`.
            transfer.flags = tb.TransferFlags.IMPORTED
            # To ensure atomicity, the entire batch (except the last event in the chain)
            # must be `linked`.
            if index < len(historical_transfers) - 1:
                transfer.flags |= tb.AccountFlags.LINKED

            transfers.append(transfer)

        transfer_errors = client.create_transfers(transfers)
        # Error handling omitted.

        # Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
        # with the same historical timestamps without regressing the cluster timestamp.
        # endsection:imported-events
    except:
        raise
