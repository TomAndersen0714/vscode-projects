#!/usr/bin/env bash

# exit when fails
set -e
# set line number log and open debug mode
export PS4='+${LINENO}: '
set -x

# parameter
FUNC=$1

# test command
echo "test"

test_func() {
    echo "test_func"
    
    # 1. shell function can access global variable in the same shell script
    echo "$FUNC"
    echo "$TEST_ARG"

    # 2. shell function can access function parameter using $1, $2, $3, ...
    echo "$1"
}

# global variable
TEST_ARG="test_arg"


# case 
case "${FUNC}" in
    1)
        test_func 1
        test_func
        ;;
    2)
        echo "2"
        ;;
    *)
        echo "default"
        ;;
esac