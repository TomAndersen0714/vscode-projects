#!/bin/bash
docker run -d \
--name=zookeeper_2 \
--hostname=bigdata001_zookeeper_1 \
--env=ZOO_MY_ID=1 \
--volume=/data/zookeeper/data/:/data/ \
--volume=/data/zookeeper/datalog/:/datalog/ \
--volume=/data/zookeeper/logs/:/logs/ \
--volume=/data/zookeeper/conf/:/conf/ \
--env=JVMFLAGS="-Xms1024m -Xmx2048m" \
--env=ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
--add-host=bigdata001:192.168.126.101 \
--restart=always \
-p 2181:2181 \
-p 2888:2888 \
-p 3888:3888 \
-p 8081:8081 \
zookeeper:3.7.0