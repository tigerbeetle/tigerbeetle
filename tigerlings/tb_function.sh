#!/bin/bash

# This is a helper function that simplifies sending individual requests
# to TigerBeetle using the REPL command in non-interactive mode.
function tb() {
    # You can start the REPL by leaving off the --command argument
    # but we want to run specific requests in these exercises, so we'll
    # pass the request as an argument to this function.
    output=$(./tigerbeetle repl --cluster=0 --addresses=3000 --command="$1" 2>&1 | sed 's/^/[Client] /')

    echo "$output"

    # For efficiency, TigerBeetle only returns error responses.
    # These will be printed to the console when we use the REPL.
    # For the purposes of these exercises, we want to treat any error responses as script failures.
    # The one exception is if an account or transfer already exists, it will return the "exists" error.
    # We treat the "exists" error as a successful operation here.
    while IFS= read -r line; do
        if [[ $line != *"connected to replica"* && $line != *"Result.exists." ]]; then
            exit 1
        fi
    done <<< "$output"
}