#!/bin/bash
docker run -d \
--name=ch-s1-r1 \
--hostname=hadoop101 \
--ulimit nofile=262144:262144 \
--volume=/data/clickhouse/ch-s1-r1/data/:/var/data/clickhouse-server/data/ \
--volume=/data/clickhouse/ch-s1-r1/storage/:/var/data/clickhouse-server/storage/data0/ \
--volume=/data/clickhouse/ch-s1-r1/log/:/var/log/clickhouse-server/ \
--volume=/data/clickhouse/ch-s1-r1/conf/:/etc/clickhouse-server/ \
--network=host \
--restart=on-failure:3 \
yandex/clickhouse-server:21.8.3