#!/bin/bash

# read input file
input_file=$1
find_path=$2

# get timestamp and output file
timestamp=$(date +%s)
output_file="output_${timestamp}.txt"

# find every file full path in the input file
while read -r line
do
    # get the full path of the file
    full_path=$(find "$find_path" -name $line)
    echo "input file: $line, full path: $full_path" >> "$output_file"
done < "$input_file"
