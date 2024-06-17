#!/bin/bash
source ./tb_function.sh

# Now, we're going to create our first transfer.

# First, we'll create a couple of accounts:
tb "create_accounts id=500 code=10 ledger=50,
                    id=501 code=10 ledger=50;"

# Then, we'll create a transfer between them.
# Unfortunately, this command is missing something! Try running it to see what happens.
tb "create_transfers debit_account_id=500 credit_account_id=501 amount=100;"

# Just like Accounts, all Transfers are uniquely identified by an ID!

# Note that Account IDs and Transfer IDs are in separate namespaces so
# theoretically, you could have an Account and a Transfer with the same ID.
# You can try it here by setting the transfer ID to 500.
# But we wouldn't recommend doing this in production!

# We recommend using TigerBeetle Time-Based Identifiers for most IDs:
# https://docs.tigerbeetle.com/coding/data-modeling#id
