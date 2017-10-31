#!/bin/bash

python3 main.py \
	--src-resume --src-wait \
	--src-host=127.0.0.1 --src-user=reader --src-password=qwerty \
	--dst-host=192.168.74.251 \
	--dst-db=db --dst-table=datatypes \
	--mempool --mempool-max-events-num=3 --mempool-max-flush-interval=30 \
	--csvpool --csvpool-file-path-prefix=qwe \
	--csv-column-default-value date_1=2000-01-01 datetime_1=2000-01-01\ 01:02:03 time_1=2001-01-01\ 01:02:03 timestamp_1=2002-01-01\ 01:02:03

#	--dst-file=dst.csv 
#	--csvpool-keep-files
