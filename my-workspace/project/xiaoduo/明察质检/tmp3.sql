
docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day = 20230724
    AND shop_id = '6302edf3fbfcd3001765e2e1'
    AND snick IN ['cnjd华凌空调京东自营官方旗舰店:华凌售后专线-小盛', 'cnjd华凌空调京东自营官方旗舰店:华凌售后专线-雨琦']
    FORMAT Avro
" > /tmp/ods.xdrs_logs_all_20230724.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO ods.xdrs_logs_all FORMAT Avro
" < /tmp/ods.xdrs_logs_all_20230724.avro