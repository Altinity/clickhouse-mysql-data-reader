#!/bin/bash

TO_DEL="build dist clickhouse_mysql.egg-info"

echo "########################################"
echo "### Clear all build and release data ###"
echo "########################################"

echo "Deleting:"
for DEL in $TO_DEL; do
    echo "    $DEL"
done

echo "rm -rf $TO_DEL"
rm -rf $TO_DEL
