#!/bin/bash
# download airline.ontime test dataset

ZIP_FILES_DIR=$(pwd)"/zip"
CSV_FILES_DIR=$(pwd)"/csv"

FROM_YEAR=1987
TO_YEAR=2017

FROM_MONTH=1
TO_MONTH=12

echo "Check required commands availability"
if command -v wget && command -v unzip; then
	echo "Looks like all required commands are available"
else
	echo "Please ensure availability of: wget && unzip"
	exit 1
fi

echo "Download dataset"

echo "Create dir $ZIP_FILES_DIR for downloading zip files"
mkdir -p "$ZIP_FILES_DIR"

if [ ! -d "$ZIP_FILES_DIR" ]; then
	"Can' use dir: $ZIP_FILES_DIR - not available"
	exit 1
fi

echo "Download files into $ZIP_FILES_DIR"
for year in `seq $FROM_YEAR $TO_YEAR`; do
	for month in `seq $FROM_MONTH $TO_MONTH`; do
		FILE_NAME="On_Time_On_Time_Performance_${year}_${month}.zip"
		wget -O "$ZIP_FILES_DIR/$FILE_NAME" "http://transtats.bts.gov/PREZIP/$FILE_NAME"
	done
done

echo "Unzip dataset"

echo "Create dir $CSV_FILES_DIR for unzipped CSV files"
mkdir -p "$CSV_FILES_DIR"

if [ ! -d "$CSV_FILES_DIR" ]; then
	"Can' use dir: $CSV_FILES_DIR - not available"
	exit 1
fi

for ZIP_FILENAME in `ls "$ZIP_FILES_DIR"/*.zip`; do
	echo "Unzipping $ZIP_FILENAME to $CSV_FILES_DIR/"
	unzip -o "$ZIP_FILENAME" -d "$CSV_FILES_DIR/"
done
