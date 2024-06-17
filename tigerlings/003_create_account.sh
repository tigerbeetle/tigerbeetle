#!/bin/bash

# Now that we have our TigerBeetle cluster running, we can start sending requests.

# To connect to TigerBeetle, we normally use one of the client libraries.
# However, for these exercises we're just going to use the REPL command via the CLI.

./tigerbeetle repl --cluster=0 --addresses=3000 --command="create_account id=300 code=10 ledger=30"
# Uh oh! This command isn't quite right. In TigerBeetle, every request can be batched for 
# performance so the type of request is always plural. Here, it should be `create_accounts`!

# To simplify things, we'll save the first part of this command as a bash function `tb`
# that we can use in the next exercises. That's stored in ./tb_function.sh.
