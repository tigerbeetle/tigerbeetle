#!/bin/bash

function remove_data_file() {
    if [ -f "0_0.tigerbeetle" ]; then
        rm "0_0.tigerbeetle"
    fi
}


function revert_patches() {
    for patch_file in ./patches/*.patch; do
        # Revert the patch using git
        git apply -R "$patch_file"
    done
}

remove_data_file

trap 'revert_patches; remove_data_file' EXIT SIGHUP SIGINT SIGQUIT SIGTERM SIGKILL

# Iterate over each patch file in the directory
for patch_file in ./patches/*.patch; do
    # Apply the patch using git
    git apply "$patch_file"
done

./run.sh

# Check the exit status
if [ $? -ne 0 ]; then
    exit 1
fi
