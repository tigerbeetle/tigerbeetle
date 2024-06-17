#!/bin/bash
source ./tb_function.sh

# Now, we're going to create our first transfer.

# First, we'll create a couple of accounts:
tb "create_accounts id=500 code=10 ledger=50,
                    id=501 code=10 ledger=51,
                    id=502 code=10 ledger=51;"

# Then, we'll create a transfer between them:
tb "create_transfers debit_account_id=500 credit_account_id=502 amount=100;"
# Uh oh! Only accounts on the same ledger can transfer directly to one another.
# Try modifying the command to send the transfer from account 501 to account 502 instead.
