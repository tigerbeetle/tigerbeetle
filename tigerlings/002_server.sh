#!/bin/bash

# Now we want to start our server.
# We need to specify the address that the server will listen on
# and the data file that the server will use.

# We'll need to pass the path of the data file we just created to the server as the last argument:
./tigerbeetle start --addresses=3000

# When you run TigerBeetle as a cluster, you'll pass all of the addresses as a comma-separated list
# and the --replica argument passed to the `format` command for creating the data file tells the
# server which address it should use.
