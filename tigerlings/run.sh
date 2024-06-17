#!/bin/bash

# Run each exercise
for file in $(ls [0-9][0-9][0-9]*.sh | grep -v "$0" | sort -n); do
    echo "Running exercise: ./$file"

    # Check if the current file is 002_server.sh
    if [ "$file" = "002_server.sh" ]; then
        # Execute the file in the background and store its process ID
        bash $file &
        server_pid=$!
        sleep 1
        if ! kill -0 $server_pid 2>/dev/null; then
            wait $server_pid
            if [ $? -ne 0 ]; then
            exit 1
            fi
        fi
    else
        # Execute the file normally
        bash "$file"
    fi

    # Check the exit status
    if [ $? -ne 0 ]; then
        echo ""
        echo "Uh oh, there is a problem in ./$file!"
        exit 1
    fi

    echo ""
done

# Code from https://stackoverflow.com/a/26966800
kill_descendant_processes() {
    local pid="$1"
    local and_self="${2:-false}"
    if children="$(pgrep -P "$pid")"; then
        for child in $children; do
            kill_descendant_processes "$child" true
        done
    fi
    if [[ "$and_self" == true ]]; then
        kill -9 "$pid"
    fi
}

# Trap the script exit signal and send SIGINT to the server process
trap 'kill_descendant_processes $$' EXIT SIGHUP SIGINT SIGQUIT SIGTERM SIGKILL

kill_descendant_processes $$
