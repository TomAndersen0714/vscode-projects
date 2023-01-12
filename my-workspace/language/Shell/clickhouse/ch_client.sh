#!/bin/bash
echo $(docker ps | grep clickhouse | sed -n '1p')
container_id=$(docker ps | grep clickhouse | awk '{print $1}' | sed -n '1p')
docker exec -it ${container_id} clickhouse-client --port=19000 -m