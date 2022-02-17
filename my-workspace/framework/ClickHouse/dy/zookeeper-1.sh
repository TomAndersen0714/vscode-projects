#!/bin/bash
docker run -d \
    --name=zookeeper-1 \
    --hostname=dy-bigdata-clickhouse-zookeeper-1 \
    --env=ZOO_MY_ID=1 \
    --volume=/data0/zookeeper/data/:/data/ \
    --volume=/data0/zookeeper/datalog/:/datalog/ \
    --volume=/data0/zookeeper/logs/:/logs/ \
    --volume=/data0/zookeeper/conf/:/conf/ \
    --env=JVMFLAGS="-Xms1024m -Xmx2048m" \
    --env=ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
    --restart=always \
    --network=host \
    zookeeper:3.7.0