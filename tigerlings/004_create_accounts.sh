#!/bin/bash
source ./tb_function.sh

# In the previous exercise, we mentioned that every request is batched.
# Now, let's use that to create multiple accounts at the same time.

# We want to create two accounts.
# However, this command has two problems. Can you spot them?
tb "create_accounts id=300 code=10 ledger=40,
                    id=401 code=10;"

# Hint 1: All account IDs are globally unique (and we created an account in the previous exercise...)
# Hint 2: Accounts are partitioned by `ledgers` (more on what these are in the next exercise!)

# Don't worry about the `code` for now, we'll go over that later.
