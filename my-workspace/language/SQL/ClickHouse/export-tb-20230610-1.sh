#!/bin/bash
docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.order_event_all
    WHERE day BETWEEN 20230301 AND 20230331
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.order_event_all-20230301-20230331.avro