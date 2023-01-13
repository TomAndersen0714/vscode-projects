#!/bin/bash
CURRENT_PATH=$(realpath "$(dirname "$0")")
PROJECT_ROOT_PATH=$(realpath "$CURRENT_PATH"/..)
MODULE_ROOT_PATH=$PROJECT_ROOT_PATH/xdt
MODULE_NAME=xdt.consumer
MODULE_CONF_PATH=$MODULE_ROOT_PATH/consumer/conf/local_test.json

echo CURRENT_PATH:"$CURRENT_PATH"
echo PROJECT_ROOT_PATH:"$PROJECT_ROOT_PATH"
echo MODULE_ROOT_PATH:"$MODULE_ROOT_PATH"
echo MODULE_CONF_PATH:"$MODULE_CONF_PATH"
echo MODULE_NAME:$MODULE_NAME

# enter the script path
echo
cmd="cd $PROJECT_ROOT_PATH || exit"
echo "$cmd"
eval "$cmd"

# kill the running process
PID=$(cat "${MODULE_NAME}.pid")
if [ "$PID" != "" ]; then
    cmd="kill $PID"
    echo "$cmd"
    eval "$cmd"

    if [ "$?" == "0" ]; then
        # sleep and wait for closing
        echo "sleep 10 seconds waiting for close..."
        sleep 10s
    fi
fi

# start a new process
nohup python3 -m "$MODULE_NAME" "$MODULE_CONF_PATH" >/dev/null 2>&1 &
echo $! >${MODULE_NAME}.pid