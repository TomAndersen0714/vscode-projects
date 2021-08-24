#!/bin/bash
docker run -d \
--name=zookeeper_3 \
--hostname=hadoop103 \
--env=ZOO_MY_ID=3 \
--volume=/opt/module/zookeeper/data/:/data/ \
--volume=/opt/module/zookeeper/log/:/log/ \
--volume=/opt/module/zookeeper/conf/:/conf/ \
--add-host=hadoop101:192.168.126.101 \
--add-host=hadoop102:192.168.126.102 \
--add-host=hadoop103:192.168.126.103 \
--env=JVMFLAGS="-Xms1024m -Xmx2048m" \
--restart=on-failure:3 \
-p 2171:2181 \
-p 2888:2888 \
-p 3888:3888 \
-p 8081:8081 \
zookeeper:3.7.0