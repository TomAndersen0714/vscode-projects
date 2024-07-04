#!/bin/bash

# read input file
input_file=$1

# read every line in the input file
while read -r line
do
    # split the line by empty space, and get the first element
    # the first element is the file path
    read -r replace_file replace_flag <<< "$line"
    
    # if the replace_flag is 'Large', replace the content of the file
    if [ "$replace_flag" == "Large" ]; then
        # replace the content of the file
        sed -i "s/old_content/new_content/g" "$replace_file"
    fi

    # replace the content of the file
    sed -i "s/old_content/new_content/g" "$line"
done < "$input_file"