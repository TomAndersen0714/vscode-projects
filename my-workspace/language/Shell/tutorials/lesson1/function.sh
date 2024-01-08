#!/usr/bin/env bash

# exit when fails
set -e
# set line number log and open debug mode
export PS4='+${LINENO}: '
set -x

# parameter
FUNC=$1
export ENV_VAR="test_env_var"

# test command
echo "test"

test_func() {
    echo "test_func"
    
    # 1. shell function can access global variable in the same shell script
    echo "$FUNC"
    echo "$TEST_ARG"

    # 2. shell function can access function parameter using $1, $2, $3, ...
    echo "function paramter: ${1}"


    # 3. shell function can access evnironment variable in the same shell script
    echo "ENV_VAR: ${ENV_VAR}"
    
    # 4. shell function can modify global variable in the same shell script
    ENV_VAR="test_env_var_modified"
    echo "ENV_VAR: ${ENV_VAR}"
}

# global variable
TEST_ARG="test_arg"


# case 
case "${FUNC}" in
    1)
        test_func 2
        test_func 3
        ;;
    2)
        echo "2"
        ;;
    *)
        echo "default"
        ;;
esac