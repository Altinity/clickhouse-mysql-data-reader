#!/bin/bash

./package_clear_old.sh

echo "##########################"
echo "### Build deb packages ###"
echo "##########################"

python3 setup.py --command-packages=stdeb.command bdist_deb

echo ""
echo ""
echo ""
echo "############################"
echo "### Results - .deb files ###"
echo "############################"
ls -la ./deb_dist/*.deb

# pypi stdeb
# apt install python3-all python3-stdeb
