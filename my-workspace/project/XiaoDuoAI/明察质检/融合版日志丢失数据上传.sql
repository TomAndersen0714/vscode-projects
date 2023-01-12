-- 融合版日志备份表
CREATE TABLE ods.xdrs_logs_bak_1_local ON CLUSTER cluster_3s_2r
AS ods.xdrs_logs_all
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE ods.xdrs_logs_bak_1_all ON CLUSTER cluster_3s_2r
AS ods.xdrs_logs_bak_1_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xdrs_logs_bak_1_local', rand())

CREATE TABLE buffer.ods_xdrs_logs_bak_1_buffer ON CLUSTER cluster_3s_2r
AS ods.xdrs_logs_bak_1_all
ENGINE = Buffer('ods', 'xdrs_logs_bak_1_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- 融合版日志临时表
CREATE TABLE tmp.xdrs_logs_bak_1_local ON CLUSTER cluster_3s_2r
AS ods.xdrs_logs_all
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE tmp.xdrs_logs_bak_1_all ON CLUSTER cluster_3s_2r
AS tmp.xdrs_logs_bak_1_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xdrs_logs_bak_1_local', rand())

CREATE TABLE buffer.tmp_xdrs_logs_bak_1_buffer ON CLUSTER cluster_3s_2r
AS tmp.xdrs_logs_bak_1_all
ENGINE = Buffer('tmp', 'xdrs_logs_bak_1_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)