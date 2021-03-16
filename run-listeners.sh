#!/bin/bash

LOG_LEVEL=debug

SOURCE_HOST=127.0.0.1
SOURCE_PORT=3307
DESTINATION_HOST=127.0.0.1
SOURCE_USER=tinybird
SOURCE_PASSWD=goo7eu9AeS3i

PID_LOG_FILE=/tmp/listeners-pid.log

source tb_tables.config

############################################################
# Run a process to synchronize MySQL table using binlog.
#
# $1 --> Source schema
# $2 --> Source table
# $3 --> Destination schema
# $4 --> Destination table
# $5 --> Server id
# $6 --> Log file
# $7 --> Binlog position file
#
#############################################################
run_listener() {
   
   (clickhouse-mysql --src-server-id=$5 --src-wait --src-resume --binlog-position-file $7 --nice-pause=1 --src-host=$SOURCE_HOST --src-port=$SOURCE_PORT --src-user=$SOURCE_USER --src-password=$SOURCE_PASSWD --src-schemas=$1 --src-tables=$2 --dst-host=$DESTINATION_HOST --dst-schema=$3 --dst-table=$4 --log-level=$LOG_LEVEL --pump-data 2>> $6)&

}

run_listener "movida_preproduction" "schedulings" "$TB_DATABASE" "$SCHEDULINGS_TABLE" "91" "out-schedulings.log" "bl-pos-schedulings"
echo $! > $PID_LOG_FILE

run_listener "movida_preproduction" "platforms" "$TB_DATABASE" "$PLATFORMS_TABLE" "92" "out-platforms.log" "bl-pos-platforms"
echo $! >> $PID_LOG_FILE

run_listener "movida_preproduction" "titles" "$TB_DATABASE" "$TITLES_TABLE" "93" "out-titles.log" "bl-pos-titles"
echo $! >> $PID_LOG_FILE

run_listener "movida_preproduction" "assets" "$TB_DATABASE" "$ASSETS_TABLE" "94" "out-assets.log" "bl-pos-assets"
echo $! >> $PID_LOG_FILE

run_listener "movida_preproduction" "features" "$TB_DATABASE" "$FEATURES_TABLE" "95" "out-features.log" "bl-pos-features"
echo $! >> $PID_LOG_FILE

run_listener "movida_preproduction" "collection_entries" "$TB_DATABASE" "$COLLECTIONS_TABLE" "96" "out-collections.log" "bl-pos-collections"
echo $! >> $PID_LOG_FILE

echo "PID processes in $PID_LOG_FILE"