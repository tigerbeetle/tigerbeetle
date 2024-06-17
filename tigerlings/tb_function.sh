#!/bin/bash

# This is a helper function that simplifies sending individual requests
# to TigerBeetle using the REPL command in non-interactive mode.
function tb() {
    # You can start the REPL by leaving off the --command argument
    # but we want to run specific requests in these exercises, so we'll
    # pass the request as an argument to this function.
    output=$(./tigerbeetle repl --cluster=0 --addresses=3000 --command="$1" 2>&1 | sed 's/^/[Client] /')

    echo "$output"
    ./tigerbeetle repl --cluster=0 --addresses=3000 --command="$1"
}
}