-- 本地表
CREATE TABLE xqc_ods.alert_remind_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `source` Int64,
    `round` Int64,
    `platform` String,
    `shop_id` String,
    `shop_external_id` String,
    `resp_code` Int64,
    `resp_msg` String,
    `time` String,
    `day` Int64,
    `notify_type` Int64,
    `alert_id` String,
    `level` Int64,
    `begin_time` String,
    `end_time` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_ods/tables/{layer}_{shard}/alert_remind_local', '{replica}')
PARTITION BY (day, platform)
ORDER BY alert_id
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 分布式表
CREATE TABLE xqc_ods.alert_remind_all
AS xqc_ods.alert_remind_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'alert_remind_local', rand())

-- Buffer表
CREATE TABLE buffer.xqc_ods_alert_remind_buffer
AS xqc_ods.alert_remind_all
ENGINE = Buffer('xqc_ods', 'alert_remind_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)
