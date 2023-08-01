# export
docker exec -i 7a6838cfcc45 clickhouse-client --port=19000 --query="
    SELECT *
    FROM ods.xdrs_logs_all
    WHERE day BETWEEN 20230726 AND 20230731
    AND shop_id = '5f8ff0c0a3967d00188dca48'
    FORMAT Avro
" > /tmp/ods.xdrs_logs_all.avro


# Import
docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO ods.tb_xdrs_logs_all
    FORMAT Avro
" < ods.xdrs_logs_all.avro