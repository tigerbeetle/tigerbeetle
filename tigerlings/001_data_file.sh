#!/bin/bash

# Every TigerBeetle server operates on a single data file.
# Before starting up the server, we need to create that file.

# Uh oh! Our data file needs a filename!
# By convention, we use the .tigerbeetle file extension.
# You can name it whatever you like but we'd recommend "0_0.tigerbeetle".
data_file=""
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 $data_file

# TigerBeetle is designed to run in production as a cluster of 6 replicas.
# The --replica and --replica-count arguments specify how many replicas
# are in the cluster and which one this is (that info is stored in the
# data file).

# For now, though, we'll just use a single-replica cluster for simplicity.

if [[ $? -eq 0 || -f $data_file ]]; then
    # The previous command succeeded or the data file already exists.
    exit 0
else
    exit 1
fi
