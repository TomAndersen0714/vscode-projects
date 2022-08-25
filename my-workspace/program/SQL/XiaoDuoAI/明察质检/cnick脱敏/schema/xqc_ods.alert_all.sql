CREATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `platform` String,
    `level` Int64,
    `warning_type` String,
    `dialog_id` String,
    `message_id` String,
    `time` String,
    `day` Int64,
    `is_finished` String,
    `finish_time` String,
    `shop_id` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `update_time` DateTime
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/alert_local',
    '{replica}',
    update_time
)
PARTITION BY day PRIMARY KEY (level, warning_type)
ORDER BY (level, warning_type, id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


CREATE TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
AS xqc_ods.alert_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'alert_local', xxHash64(level, warning_type, id))


CREATE TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.alert_all 
ENGINE = Buffer('xqc_ods', 'alert_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)

-- pulsar://pulsar-cluster01-slb:6650
-- persistent://qc/event/warning
-- bigdata_xqc_tb

ALTER TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
ADD COLUMN `buyer_one_id` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `buyer_one_id`

ALTER TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
ADD COLUMN `buyer_one_id` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `buyer_one_id`

DROP TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.alert_all
ENGINE = Buffer('xqc_ods', 'alert_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)