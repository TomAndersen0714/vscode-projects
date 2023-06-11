#!/bin/bash
docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230301
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230301.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230302
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230302.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230303
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230303.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230304
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230304.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230305
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230305.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230306
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230306.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230307
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230307.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230308
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230308.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230309
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230309.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230310
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230310.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230311
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230311.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230312
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230312.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230313
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230313.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230314
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230314.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230315
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230315.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230316
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230316.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230317
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230317.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230318
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230318.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230319
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230319.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230320
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230320.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230321
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230321.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230322
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230322.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230323
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230323.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230324
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230324.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230325
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230325.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230326
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230326.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230327
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230327.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230328
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230328.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230329
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230329.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230330
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230330.avro

docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230331
    AND shop_id = '5a48c5c489bc46387361988d'
    FORMAT Avro
" > ./tmp/ods.xdrs_logs_all-20230331.avro


