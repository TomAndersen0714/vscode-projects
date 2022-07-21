#!/bin/bash
docker run -d --name clickhouse-server20.4.3.16 \
    --ulimit nofile=262144:262144 \
    -p 9000:9000 \
    -p 8123:8123 \
    -v /data0/clickhouse/conf/:/etc/clickhouse-server/ \
    -v /data0/clickhouse/data/:/var/lib/clickhouse/ \
    -v /data0/clickhouse/logs/:/var/log/clickhouse-server/ \
    --privileged=true \
    --user=root \
    yandex/clickhouse-server:20.4.3.16
