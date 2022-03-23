CREATE TABLE tmp.hxcpoc_local ON CLUSTER cluster_3s_2r
(
    `snick` String,
    `cnick` String,
    `order_id` String,
    `goods_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`snick`,`cnick`)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE tmp.hxcpoc_all ON CLUSTER cluster_3s_2r
AS tmp.hxcpoc_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'hxcpoc_local', rand())