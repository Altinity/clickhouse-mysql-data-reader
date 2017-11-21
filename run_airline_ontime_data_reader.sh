#!/bin/bash
# read airline.ontime test dataset from MySQL and write it to CH

# ugly stub to suppress unsufficient sockets
sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse"

# run data reader with specified Python version

PYTHON=python3.6
PYTHON=/home/user/pypy3.5-5.9-beta-linux_x86_64-portable/bin/pypy

$PYTHON main.py ${*:1} \
    --src-resume \
    --src-wait \
    --nice-pause=1 \
    --log-level=info \
    --log-file=ontime.log \
    --src-host=127.0.0.1 \
    --src-user=root \
    --dst-host=127.0.0.1 \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=60 \
    --mempool-max-events-num=1000

#	--mempool
#   --mempool-max-events-num=3
#   --mempool-max-flush-interval=30
#	--dst-file=dst.csv
#	--dst-schema=db
#   --dst-table=datatypes
#	--csvpool-keep-files
#    --log-level=info \
