#!/bin/bash

# exit when fails
set -e
# set line number log and open debug mode
export PS4='+${LINENO}: '
set -x


# NOTE: grep will return non-zero exit code when no match found
echo "hello world!" | grep "hello"
echo $?

echo "hello world!" | grep "la"
echo $?


echo "hello world!" | grep "la" | sort
echo $?