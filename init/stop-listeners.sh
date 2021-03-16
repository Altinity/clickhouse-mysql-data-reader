#!/bin/bash

PID_LOG_FILE=/tmp/listeners-pid.log

count_processes() {
    echo `ps aux | grep clickhouse-mysql-data-reader | wc -l`
}

total_before=$(count_processes)

while IFS= read -r line
do
  echo "$line"
  kill $line
done < "$PID_LOG_FILE"

total_after=$(count_processes)

procs=`echo "$total_after - 1" | bc`

if [ $total_after -gt 1 ]; then
    echo "You still have $procs processes running"
fi