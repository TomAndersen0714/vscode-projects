-- 创建本地副本表
CREATE TABLE xqc_ods.qc_dialog_cnt_local ON CLUSTER cluster_3s_2r(
    `day` Int64,
    `platform` String,
    `seller_nick` String,
    `shop_id` String,
    `qc_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_ods/tables/{layer}_{shard}/qc_dialog_cnt_local', '{replica}') 
PARTITION BY (day, platform) 
ORDER BY snick 
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 创建分布式表
CREATE TABLE xqc_ods.qc_dialog_cnt_all ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_dialog_cnt_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'qc_dialog_cnt_local', rand())

