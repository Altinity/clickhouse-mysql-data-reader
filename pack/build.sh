#!/bin/bash

set -x

SOURCE_ROOT_DIR="$( cd "$( dirname $( dirname "${BASH_SOURCE[0]}" ) )" && pwd )"

RPMBUILD_DIR="$SOURCE_ROOT_DIR/build/bdist.linux-x86_64/rpm/"

TMP_DIR="$RPMBUILD_DIR/TMP"

RPMMACROS=$(echo '%_topdir '"$RPMBUILD_DIR"'
%_tmppath '"$TMP_DIR")
echo "$RPMMACROS" > ~/.rpmmacros

SPEC_FILE="$SOURCE_ROOT_DIR/pack/clickhouse-mysql.spec"

VERSION=$(cat "$SOURCE_ROOT_DIR/setup.py" | grep 'version=' | grep -o "'.*'" | grep -o "[^']" | tr -d '\n')
echo "VERSION=$VERSION"

function mkdirs()
{
	mkdir -p "$RPMBUILD_DIR"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
	mkdir -p "$TMP_DIR"
}

mkdirs

rpmbuild -ba $SPEC_FILE

