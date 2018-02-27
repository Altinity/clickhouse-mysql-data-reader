#!/bin/bash

./package_clear_old.sh

echo "##########################"
echo "### Build deb packages ###"
echo "##########################"

python3 setup.py --command-packages=stdeb.command bdist_deb

# pypi stdeb
# apt install python3-all python3-stdeb
