#!/bin/bash
docker run -d \
--name=zookeeper_1 \
--hostname=pdd-bigdata-clickhouse01_zookeeper_1 \
--env=ZOO_MY_ID=1 \
--volume=/data0/zookeeper/data/:/data/ \
--volume=/data0/zookeeper/datalog/:/datalog/ \
--volume=/data0/zookeeper/logs/:/logs/ \
--volume=/data0/zookeeper/conf/:/conf/ \
--env=JVMFLAGS="-Xms1024m -Xmx2048m" \
--env=ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
--add-host=pdd-bigdata-clickhouse01:172.16.226.27 \
--restart=on-failure:3 \
-p 2181:2181 \
-p 2888:2888 \
-p 3888:3888 \
-p 8081:8081 \
zookeeper:3.7.0


docker run -d \
--name=ch-s1-r1 \
--hostname=znzjk-133213-prod-mini-bigdata-clickhouse \
--ulimit nofile=262144:262144 \
--volume=/data0/clickhouse/logs/:/var/log/clickhouse-server/ \
--volume=/data0/clickhouse/:/var/lib/clickhouse/ \
--volume=/data0/clickhouse/conf/:/etc/clickhouse-server/ \
--network=host \
--restart=always \
registry.cn-zhangjiakou.aliyuncs.com/xiaoduoai/yandex/clickhouse-server:20.4