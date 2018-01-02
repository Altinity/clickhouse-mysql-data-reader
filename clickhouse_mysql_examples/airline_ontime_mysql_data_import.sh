#!/bin/bash
# import airline.ontime test dataset into MySQL

# looking for csv files in this dir
#FILES_TO_IMPORT_DIR="/mnt/nas/work/ontime"
FILES_TO_IMPORT_DIR="csv"

# how many files to skip from the beginning of the list
FILES_TO_SKIP_NUM=0

# how many files to import
FILES_TO_IMPORT_NUM=10

# which file would be the first to import
FILE_TO_START_IMPORT_FROM=$((FILES_TO_SKIP_NUM+1))

i=1
for file in $(ls "$FILES_TO_IMPORT_DIR"/*.csv|sort|tail -n +"$FILE_TO_START_IMPORT_FROM"|head -n "$FILES_TO_IMPORT_NUM"); do
    echo "$i. Prepare. Make link to $file"
    rm -f ontime
    ln -s $file ontime

    echo "$i. Import. $file"
    time mysqlimport \
        --ignore-lines=1 \
        --fields-terminated-by=, \
        --fields-enclosed-by=\" \
        --local \
        -u root \
        airline ontime

#--local reads files locally on the client host, bot on the server
#--lock-tables Lock all tables for writing before processing any text files. This ensures that all tables are synchronized on the server.

    echo "$i. Cleanup. $file"
    rm -f ontime

    i=$((i+1))
done
