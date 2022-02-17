#!/bin/bash
docker run -d \
    --name=ch_single_node \
    --hostname=bigdata001_ch_single_node \
    --ulimit nofile=262144:262144 \
    --volume=/data/clickhouse/ch_single_node/data/:/var/lib/clickhouse/ \
    --volume=/data/clickhouse/ch_single_node/storage/:/var/lib/clickhouse/data0/ \
    --volume=/data/clickhouse/ch_single_node/log/:/var/log/clickhouse-server/ \
    --volume=/data/clickhouse/ch_single_node/conf/:/etc/clickhouse-server/ \
    --restart=always \
    --add-host=bigdata001:192.168.126.101 \
    -p 8123:8123 \
    -p 9000:9000 \
    -p 9009:9009 \
    -p 9004:9004 \
    yandex/clickhouse-server:20.4