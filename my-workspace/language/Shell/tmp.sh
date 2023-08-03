# export
docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day BETWEEN 20230725 AND 20230802
    AND shop_id IN ['61616faa112fa5000dcc7fba', '6447a2067aa2e643ec2dc244']
    FORMAT Avro
" > /tmp/ods.xdrs_logs_all.avro


# Import
docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO ods.xdrs_logs_all
    FORMAT Avro
" < ods.xdrs_logs_all_20230725-20230802.avro