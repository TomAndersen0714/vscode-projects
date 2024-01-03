#!/usr/bin/env bash

set -e
#set -x

# find all properties file in specified directory
find ./conf -name "*.properties" | while read -r file; do
    # read all keys in properties file
    cat "$file" | grep -v "^#" | grep -v "^$" | cut -d= -f1 | while read -r key; do
        echo "$file:$key:"

        # check if key is in files in subdirectories
        grep -rn "\${${key}}" bin/* | grep -v "$file" | head -n 3

        echo
    done
done
