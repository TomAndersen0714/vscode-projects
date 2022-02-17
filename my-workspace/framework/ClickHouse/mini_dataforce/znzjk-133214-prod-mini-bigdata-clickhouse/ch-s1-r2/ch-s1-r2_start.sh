#!/bin/bash
docker run -d \
--name ch-s1-r2 \
--hostname znzjk-133214-prod-mini-bigdata-clickhouse \
--ulimit nofile=262144:262144 \
--volume=/data/clickhouse/ch-s1-r2/data/:/var/data/clickhouse-server/data/ \
--volume=/data/clickhouse/ch-s1-r2/storage/:/var/data/clickhouse-server/storage/data0/ \
--volume=/data/clickhouse/ch-s1-r2/log/:/var/log/clickhouse-server/ \
--volume=/data/clickhouse/ch-s1-r2/conf/:/etc/clickhouse-server/ \
--network=host \
--restart=always \
yandex/clickhouse-server:21.8.3
