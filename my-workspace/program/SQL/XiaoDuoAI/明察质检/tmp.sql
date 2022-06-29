sxx_ods.compensate_filter_condition_local
sxx_ods.compensate_way_map_local
sxx_ods.jd_warehouse_map_local
sxx_ods.logistics_local
sxx_ods.outbound_goods_map_local
sxx_ods.plat_goods_map_local
sxx_ods.responsible_party_map_local
sxx_ods.warehouse_local
sxx_ods.workorder_filter_condition_local

ALTER TABLE sxx_ods.compensate_filter_condition_local ON CLUSTER cluster_3s_2r UPDATE day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.compensate_way_map_local ON CLUSTER cluster_3s_2r UPDATE day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.jd_warehouse_map_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.logistics_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.outbound_goods_map_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.plat_goods_map_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.responsible_party_map_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.warehouse_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1
ALTER TABLE sxx_ods.workorder_filter_condition_local ON CLUSTER cluster_3s_2r UPDATE  day = 20220628 WHERE 1=1


ALTER TABLE sxx_ods.compensate_filter_condition_local ON CLUSTER cluster_3s_2r DELETE WHERE day = 0
ALTER TABLE sxx_ods.compensate_way_map_local ON CLUSTER cluster_3s_2r DELETE WHERE day = 0
ALTER TABLE sxx_ods.jd_warehouse_map_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.logistics_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.outbound_goods_map_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.plat_goods_map_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.responsible_party_map_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.warehouse_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0
ALTER TABLE sxx_ods.workorder_filter_condition_local ON CLUSTER cluster_3s_2r DELETE WHERE  day = 0

truncate table sxx_ods.compensate_way_map_local on cluster cluster_3s_2r
truncate table sxx_ods.plat_goods_map_local on cluster cluster_3s_2r
truncate table sxx_ods.outbound_goods_map_local on cluster cluster_3s_2r
truncate table sxx_ods.compensate_filter_condition_local on cluster cluster_3s_2r
truncate table sxx_ods.responsible_party_map_local on cluster cluster_3s_2r


docker exec -i 497fcc1132c7 clickhouse-client --port=19000 -m --query="\
INSERT INTO sxx_ods.compensate_way_map_all FORMAT CSV" < sxx_ods.compensate_way_map_all.csv