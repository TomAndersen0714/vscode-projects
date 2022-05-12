-- xqc_dim.snick_full_local
-- DROP TABLE xqc_dim.snick_full_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_local ON CLUSTER cluster_3s_2r
(

)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, _id)
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- xqc_dim.snick_full_all
-- DROP TABLE xqc_dim.snick_full_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_full_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_full_local', rand())


-- ELT
