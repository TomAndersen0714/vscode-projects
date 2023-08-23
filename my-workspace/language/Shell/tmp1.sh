docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_0.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_1.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_2.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_3.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_4.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_5.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_6.avro

docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < dwd.xdqc_dialog_all_20230813_7.avro
