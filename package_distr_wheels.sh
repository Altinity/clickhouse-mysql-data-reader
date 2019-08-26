#!/bin/bash

./package_clear_old.sh

python3 setup.py bdist_wheel
