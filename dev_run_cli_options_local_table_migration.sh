#!/bin/bash

# This script performs migration of one table test.books from local MySQL
# into one table test.books in local ClickHouse
# Tables are created manually by user and are expected by migrator to be in place
# Migrator exists after all data from migrated table is copied into ClickHouse

# ugly stub to suppress unsufficient sockets
#sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse"

# run data reader with specified Python version

PYTHON="python3"

CH_MYSQL="-m clickhouse_mysql.main"

if [ ! -d "clickhouse_mysql" ]; then
    # no clickhouse_mysql dir available - step out of examples dir
    cd ..
fi

MYSQL_USER=reader
MYSQL_PASSWORD=qwerty
SRC_TABLES=test.books
DST_SCHEMA=test
DST_TABLE=books

MYSQL_USER=user1
MYSQL_PASSWORD=qwerty
SRC_TABLES=repl.foo
DST_SCHEMA=repl1
DST_TABLE=foo1

$PYTHON $CH_MYSQL ${*:1} \
    --src-server-id=1 \
    --nice-pause=1 \
    --log-level=debug \
    \
    --src-host=127.0.0.1 \
    --src-user="${MYSQL_USER}" \
    --src-password="${MYSQL_PASSWORD}" \
    --src-tables="${SRC_TABLES}" \
    \
    --dst-host=127.0.0.1 \
    --dst-create-table \
    --with-create-database \
    \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=60 \
    --mempool-max-events-num=10000 \
    \
    --binlog-position-file=qwe.txt \
    --pump-data \
    --migrate-table \
    --src-wait \
    --src-resume

#    --dst-schema="${DST_SCHEMA}" \
#    --dst-table="${DST_TABLE}" \
#    --dst-table="${DST_SCHEMA}.${DST_TABLE}" \
#    --dst-table-prefix="pr1_" \
#    --log-file=ontime.log \
#	--mempool
#   --mempool-max-events-num=3
#   --mempool-max-flush-interval=30
#	--dst-file=dst.csv
#	--dst-schema=db
#   --dst-table=datatypes
#	--csvpool-keep-files
#    --log-level=info \
