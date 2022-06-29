CREATE DATABASE sxx_dwd ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dwd.voc_qc_compensate_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dwd.voc_qc_compensate_local ON CLUSTER cluster_3s_2r
(
    `dialog_id` String,
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `order_id` String,
    `focus_goods_id` String,
    `qc_label_id` String,
    `qc_label` String,
    `responsible_party` String,
    `goods_name` String,
    `compensate_workerorder_id` String,
    `compensate_day` Int32,
    `refund_way` String,
    `warehouse_type` String,
    `warehouse` String,
    `logistics_company` String,
    `logistics_company_abbr` String,
    `receiving_area` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (refund_way, warehouse_type)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dwd.voc_qc_compensate_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dwd.voc_qc_compensate_all ON CLUSTER cluster_3s_2r
AS sxx_dwd.voc_qc_compensate_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dwd', 'voc_qc_compensate_local', rand())

-- DROP TABLE buffer.sxx_dwd_voc_qc_compensate_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_dwd_voc_qc_compensate_buffer ON CLUSTER cluster_3s_2r
AS sxx_dwd.voc_qc_compensate_all
ENGINE = Buffer('sxx_dwd', 'voc_qc_compensate_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)