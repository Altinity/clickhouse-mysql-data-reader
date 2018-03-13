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

$PYTHON $CH_MYSQL --config-file=clickhouse_mysql.etc/clickhouse-mysql.conf ${*:1}
