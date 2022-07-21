#!/bin/bash
docker run -d \
    --name=ch-s1-r1 \
    --hostname=ks-023111066-bigdata-clickhouse-ch-s1-r1 \
    --ulimit nofile=262144:262144 \
    -v /data0/clickhouse/conf/:/etc/clickhouse-server/ \
    -v /data0/clickhouse/data/:/var/lib/clickhouse/ \
    -v /data0/clickhouse/logs/:/var/log/clickhouse-server/ \
    -v /data2/clickhouse/data/:/var/lib/clickhouse/data2/ \
    --network=host \
    --restart=always \
    yandex/clickhouse-server:20.4.3.16
