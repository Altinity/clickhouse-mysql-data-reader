#!/bin/bash
# migrate data from MySQL to ClickHouse

# put csv files in this dir
CSV_FILES_DIR="/var/lib/mysql-files"

# dump CSV files into CSV_FILES_DIR
sudo mysqldump \
    -u root \
    --tz-utc \
    --quick \
    --fields-terminated-by=, \
    --fields-optionally-enclosed-by=\" \
    --fields-escaped-by=\\ \
    --tab="$CSV_FILES_DIR"/ \
    airline ontime

# replay CSV files from CSV_FILES_DIR
sudo cat "$CSV_FILES_DIR"/ontime.txt | clickhouse-client --query="INSERT INTO airline.ontime FORMAT CSV"
