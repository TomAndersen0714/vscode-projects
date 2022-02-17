#!/bin/bash
docker run -d \
    --name=zk_single_node \
    --hostname=bigdata001_zk_single_node \
    --env=ZOO_MY_ID=1 \
    --volume=/data/zookeeper/data/:/data/ \
    --volume=/data/zookeeper/datalog/:/datalog/ \
    --volume=/data/zookeeper/logs/:/logs/ \
    --volume=/data/zookeeper/conf/:/conf/ \
    --env=JVMFLAGS="-Xms1024m -Xmx2048m" \
    --env=ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
    --restart=always \
    --add-host=bigdata001:192.168.126.101 \
    -p 2181:2181 \
    -p 2888:2888 \
    -p 3888:3888 \
    -p 8081:8081 \
    zookeeper:3.7.0