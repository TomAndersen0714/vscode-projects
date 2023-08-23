# Import
docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_0.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_1.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_2.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_3.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_4.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_5.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_6.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230815_7.avro