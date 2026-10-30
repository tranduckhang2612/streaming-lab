#!/usr/bin/env bash
set -uo pipefail

# Keep the JVM available while Python stops the query and records its finish.
submit_pid=""
forward_stop() {
    local query_pid query_name forwarded=0
    while read -r query_pid query_name; do
        if [[ "$query_name" == python* ]]; then
            kill -TERM "$query_pid" 2>/dev/null || true
            forwarded=1
        fi
    done < <(ps -o pid=,comm= --ppid "$submit_pid")
    if [[ "$forwarded" == 0 ]]; then
        # Python has not started yet (for example while resolving packages).
        kill -TERM "$submit_pid" 2>/dev/null || true
    fi
}

"$@" &
submit_pid=$!
trap forward_stop TERM INT

while true; do
    wait "$submit_pid"
    exit_code=$?
    # A trapped signal interrupts wait; resume waiting for Spark to finish.
    if ! kill -0 "$submit_pid" 2>/dev/null; then
        exit "$exit_code"
    fi
done
