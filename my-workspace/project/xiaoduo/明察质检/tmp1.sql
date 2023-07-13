CREATE TABLE xqc_ods.dialog_all ON CLUSTER cluster_3s_2r
(
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
) ENGINE = Distributed(
    'cluster_3s_2r',
    'xqc_ods',
    'dialog_local',
    rand()
)

CREATE TABLE xqc_ods.dialog_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/dialog_local',
    '{replica}'
) PARTITION BY (day, platform)
ORDER BY snick SETTINGS index_granularity = 8192,
    storage_policy = 'rr'


CREATE TABLE buffer.xqc_ods_dialog_buffer ON CLUSTER cluster_3s_2r
(
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
) ENGINE = Buffer(
    'xqc_ods',
    'dialog_all',
    16,
    5,
    10,
    81920,
    409600,
    16777216,
    67108864
)
