#!/bin/bash
#Auth: fenghao
#Comment: create clickhouse instance
#History:
#2020-06-30 create first version
set -xeuo pipefail

BASE_DIR=/data0

function env_check {
    if [ -d $BASE_DIR/clickhouse/data/ ];then
        echo "ERROR: $BASE_DIR/clickhouse/data/ is exists"
        exit 1
    fi

}


function env_prepare {
    mkdir -p $BASE_DIR/clickhouse/conf/
    mkdir -p $BASE_DIR/clickhouse/data/
    cp clickhouse_config.xml  $BASE_DIR/clickhouse/conf/config.xml
}

function run_docker {
docker run -d --name clickhouse-server20.4.3.16 \
    --ulimit nofile=262144:262144 \
    -p 9000:9000 \
    -p 8123:8123 \
    -v $BASE_DIR/clickhouse/conf/:/etc/clickhouse-server/ \
    -v $BASE_DIR/clickhouse/data:/var/lib/clickhouse \
    -v $BASE_DIR/clickhouse/logs/:/var/log/clickhouse-server/ \
    --privileged=true --user=root \
    yandex/clickhouse-server:20.4.3.16 
}

function main {
    # env_check
    # env_prepare
    run_docker
}

main
