-- CDH2数据导出
docker exec -i 98c20caf2a35 clickhouse-client --host=10.248.32.3 --port=19000 -m --query="\
SELECT * FROM sxx_ods.outbound_workorder_all LIMIT 10000 FORMAT Avro" \
> sxx_ods.outbound_workorder_all.avro

docker exec -i 98c20caf2a35 clickhouse-client --host=10.248.32.3 --port=19000 -m --query="\
SELECT * FROM sxx_ods.compensate_workorder_all LIMIT 10000 FORMAT Avro" \
> sxx_ods.compensate_workorder_all.avro


-- Test数据导入
docker exec -i 5eb03ec8bacb clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.outbound_workorder_all FORMAT Avro" < sxx_ods.outbound_workorder_all.avro


docker exec -i 5eb03ec8bacb clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.compensate_workorder_all FORMAT Avro" < sxx_ods.compensate_workorder_all.avro



-- 映射表导入
docker exec -i 497fcc1132c7 clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.compensate_filter_condition_all FORMAT CSV" < sxx_ods.compensate_filter_condition_all.csv

docker exec -i 497fcc1132c7 clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.compensate_way_map_all FORMAT CSV" < sxx_ods.compensate_way_map_all.csv

docker exec -i 497fcc1132c7 clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.responsible_party_map_all FORMAT CSV" < sxx_ods.responsible_party_map_all.csv