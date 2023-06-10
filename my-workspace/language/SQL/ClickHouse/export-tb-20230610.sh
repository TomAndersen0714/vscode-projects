#!/bin/bash
docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230301
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230301.avro