#!/bin/bash
for file in ./*; do
    if test -d $file; then
        echo $file
        cd $file && git pull origin master && cd ..
        echo
    fi
done