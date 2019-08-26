#!/bin/bash

# List of items (files and folders) to be deleted.
# These items are package-related
ITEMS_TO_DEL="
build
dist
clickhouse_mysql.egg-info
deb_dist
"

echo "########################################"
echo "### Clear all build and release data ###"
echo "########################################"

echo "About to delete:"
DEL=""
for ITEM in ${ITEMS_TO_DEL}; do
    echo "    ${ITEM}"
    DEL="${DEL} ${ITEM}"
done

if [[ -z "${DEL}" ]]; then
    echo "No items to delete"
else
    echo "rm -rf ${DEL}"
    rm -rf ${DEL}
fi
