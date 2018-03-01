#!/bin/bash

./package_clear_old.sh

python3 setup.py bdist_rpm --packager="Vladislav Klimenko <sunsingerus@gmail.com>"
# --spec-only

# ls -l ./build/bdist.linux-x86_64/rpm/SPECS/
# ls -l ./dist/
# build RPMs with
# rpmbuild -ba ./build/bdist.linux-x86_64/rpm/SPECS/clickhouse-mysql.spec

# https://docs.python.org/2.0/dist/creating-rpms.html
# for deb-based distro
# sudo apt install rpm

# 1. install python3
# ensure python3 is available or run sudo ln -s python3.6 /usr/bin/python3
# 2. install sudo yum install rpm-build