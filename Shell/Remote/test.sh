#!/bin/bash
PID=$(ps -ef | grep taildir-kafka -n | grep -v grep | awk "{print \$2}")

if [ -z "$PID" ]; then
    echo "No logs-collect process to kill."
else
    echo "There is logs-collect process."
fi
