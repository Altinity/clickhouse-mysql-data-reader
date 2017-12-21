#!/bin/bash

echo "###########################"
echo "### Publish from dist/* ###"
echo "###########################"

echo "Going to publish:"
for FILE in $(ls dist/*); do
    echo "    $FILE"
done

twine upload dist/*
