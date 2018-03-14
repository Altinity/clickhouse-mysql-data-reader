#!/bin/bash

# ugly stub to suppress unsufficient sockets
#sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse"

# run data reader with specified Python version

PYTHON="python3"

CH_MYSQL="-m clickhouse_mysql.main"

if [ ! -d "clickhouse_mysql" ]; then
    # no clickhouse_mysql dir available - step out of examples dir
    cd ..
fi

$PYTHON $CH_MYSQL ${*:1} \
    --src-server-id=1 \
    --src-resume \
    --src-wait \
    --nice-pause=1 \
    --log-level=debug \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=qwerty \
    --src-tables-prefixes=log_ \
    --dst-host=127.0.0.1 \
    --dst-table=logunified \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=60 \
    --mempool-max-events-num=10000 \
    --pump-data

#    --log-file=ontime.log \
#	--mempool
#   --mempool-max-events-num=3
#   --mempool-max-flush-interval=30
#	--dst-file=dst.csv
#	--dst-schema=db
#   --dst-table=datatypes
#	--csvpool-keep-files
#    --log-level=info \
