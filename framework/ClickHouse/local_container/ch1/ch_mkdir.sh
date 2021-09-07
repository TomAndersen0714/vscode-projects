#!/bin/bash
mkdir -p /data/clickhouse/ch-s1-r1/data/ /data/clickhouse/ch-s1-r1/storage/ \
/data/clickhouse/ch-s1-r1/log/ /data/clickhouse/ch-s1-r1/conf/ /etc/clickhouse/

chown 101:101 /data/clickhouse/ch-s1-r1/*

mkdir -p /data/clickhouse/ch-s2-r2/data/ /data/clickhouse/ch-s2-r2/storage/ \
/data/clickhouse/ch-s2-r2/log/ /data/clickhouse/ch-s2-r2/conf/ /etc/clickhouse/

chown 101:101 /data/clickhouse/ch-s2-r2/*