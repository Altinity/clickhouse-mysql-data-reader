#!/bin/bash

sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse"

python3 main.py \
    --src-resume \
    --src-wait \
    --src-host=127.0.0.1 \
    --src-user=root \
    --dst-host=127.0.0.1 \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=300 \
    --mempool-max-events-num=100000

#	--mempool
#   --mempool-max-events-num=3
#   --mempool-max-flush-interval=30
#	--dst-file=dst.csv
#	--dst-schema=db
#   --dst-table=datatypes
#	--csvpool-keep-files
