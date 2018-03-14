#!/bin/bash

sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse"

PYTHON=""
PYTHON="python3.6"
PYTHON="python3"
PYTHON="/home/user/pypy3.5-5.9-beta-linux_x86_64-portable/bin/pypy"

CH_MYSQL="/usr/bin/clickhouse-mysql"
CH_MYSQL="-m clickhouse_mysql.main"

$PYTHON $CH_MYSQL ${*:1} \
    --src-resume \
    --src-wait \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=qwerty \
    --dst-host=192.168.74.251 \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --column-default-value \
        date_1=2000-01-01 \
        datetime_1=2000-01-01\ 01:02:03 \
        time_1=2001-01-01\ 01:02:03 \
        timestamp_1=2002-01-01\ 01:02:03 \
    --mempool-max-flush-interval=600 \
    --mempool-max-events-num=900000 \
    --pump-data

#	--mempool
#   --mempool-max-events-num=3
#   --mempool-max-flush-interval=30
#	--dst-file=dst.csv
#	--dst-schema=db
#   --dst-table=datatypes
#	--csvpool-keep-files
