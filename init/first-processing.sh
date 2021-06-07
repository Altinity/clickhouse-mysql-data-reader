#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <dump-path>"
    exit -1
fi

echo "Generate binlog timelog"
./run-listener.sh
./stop-listeners.sh

echo "Generating dumps and loading data ..."
./dump-tables.sh $1

echo "Starting listeners"
./run-listener.sh

echo "Done!"