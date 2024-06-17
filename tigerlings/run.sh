#!/bin/bash

# We store a helper function in a separate file to simplify sending requests to TigerBeetle.
source ./tb_function.sh

# Iterate over the files in the directory
for file in $(ls [0-9][0-9][0-9]*.sh | grep -v "$0" | sort -n); do
    echo "Running exercise: ./$file"

    # Execute the file
    bash "$file"
    
    # Check the exit status
    if [ $? -ne 0 ]; then
        echo ""
        echo "Uh oh, there is a problem in ./$file!"
        exit 1
    fi

    echo ""
done

echo "Excellent! You finished all the Tigerlings exercises! You're a TigerBeetle expert now!"
