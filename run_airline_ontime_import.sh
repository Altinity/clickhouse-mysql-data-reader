#!/bin/bash
# import airline.ontime test dataset into MySQL

# looking for csv files in this dir
FILES_TO_IMPORT_DIR="/mnt/nas/work/ontime"

# limit import to this number of files
FILES_TO_IMPORT_NUM=3

i=1
for file in $(ls "$FILES_TO_IMPORT_DIR"/*.csv|sort|head -n $FILES_TO_IMPORT_NUM); do
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

    echo "$i. Cleanup. $file"
    rm -f ontime

    i=$((i+1))
done
