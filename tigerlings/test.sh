#!/bin/bash

# Iterate over each patch file in the directory
for patch_file in ./patches/*.patch; do
    # Apply the patch using git
    git apply "$patch_file"
done

./run.sh

# Check the exit status
if [ $? -ne 0 ]; then
    echo ""
    echo "Uh oh, there is a problem in one of the exercises!"
    exit 1
fi

for patch_file in ./patches/*.patch; do
    # Revert the patch using git
    git apply -R "$patch_file"
done
