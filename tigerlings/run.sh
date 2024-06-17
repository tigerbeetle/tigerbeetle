#!/bin/bash

bold=$(tput bold)
normal=$(tput sgr0)

# Run each exercise
for file in $(ls [0-9][0-9][0-9]*.sh | grep -v "$0" | sort -n); do
    echo "${bold}Running exercise: ./$file${normal}"

    if [ "$file" = "000_download.sh" ]; then
        # Only download/build TigerBeetle if it's not already available
        if  ./tigerbeetle version >/dev/null 2>&1; then
            echo "TigerBeetle is already available. Skipping exercise."
        else
            bash "$file"
        fi
    elif [ "$file" = "002_server.sh" ]; then
        # Execute the file in the background and store its process ID
        bash "$file" 2>&1 | sed "s/^/${bold}[Server]${normal} /" &
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
