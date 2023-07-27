# export
docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT * FROM dim.category_subcategory_all FORMAT Avro
" > /tmp/dim.category_subcategory_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT * FROM dim.kaleidoscope_category_domain_all FORMAT Avro
" > /tmp/dim.kaleidoscope_category_domain_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT * FROM dim.question_b_all FORMAT Avro
" > /tmp/dim.question_b_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT * FROM ods.xdrs_logs_all
    WHERE day = 20230606
    AND shop_id = '61616faa112fa5000dcc7fba' FORMAT Avro
" > /tmp/ods.xdrs_logs_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM dim.goods_center_all
    WHERE shop_id = '61616faa112fa5000dcc7fba' FORMAT Avro
" > /tmp/dim.goods_center_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM dim.xdre_shop_all
    FORMAT Avro
" > /tmp/dim.xdre_shop_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM dim.voc_question_b_all FORMAT Avro
" > /tmp/dim.voc_question_b_all.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query="
    SELECT *
    FROM dim.voc_question_b_group_all FORMAT Avro
" > /tmp/dim.voc_question_b_group_all.avro


# Import

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO ods.xdrs_logs_all FORMAT Avro
" < ods.xdrs_logs_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.category_subcategory_all FORMAT Avro
" < dim.category_subcategory_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.kaleidoscope_category_domain_all FORMAT Avro
" < dim.kaleidoscope_category_domain_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.question_b_all FORMAT Avro
" < dim.question_b_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.goods_center_all FORMAT Avro
" < dim.goods_center_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.xdre_shop_all FORMAT Avro
" < dim.xdre_shop_all.avro


docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.voc_question_b_all FORMAT Avro
" < dim.voc_question_b_all.avro

docker exec -i f513ece2eba7 clickhouse-client --port=19000 --query="
    INSERT INTO dim.voc_question_b_group_all FORMAT Avro
" < dim.voc_question_b_group_all.avro