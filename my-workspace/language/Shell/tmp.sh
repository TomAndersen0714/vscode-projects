#!/bin/bash

# get parent directory of script
work_dir=$(dirname "$PWD")

# using parent directory as working directory
cd "$work_dir" || exit

# run script
python3 -m xdt