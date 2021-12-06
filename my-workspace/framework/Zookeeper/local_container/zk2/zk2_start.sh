#!/bin/bash
docker run -d \
--name=zookeeper_2 \
--hostname=hadoop102 \
--env=ZOO_MY_ID=2 \
--volume=/data/zookeeper/data/:/data/ \
--volume=/data/zookeeper/datalog/:/datalog/ \
--volume=/data/zookeeper/logs/:/logs/ \
--volume=/data/zookeeper/conf/:/conf/ \
--add-host=hadoop101:192.168.126.101 \
--add-host=hadoop102:192.168.126.102 \
--add-host=hadoop103:192.168.126.103 \
--env=JVMFLAGS="-Xms1024m -Xmx2048m" \
--env=ZOO_LOG4J_PROP="INFO,ROLLINGFILE" \
--restart=on-failure:3 \
-p 2182:2181 \
-p 2888:2888 \
-p 3888:3888 \
-p 8081:8081 \
zookeeper:3.7.0