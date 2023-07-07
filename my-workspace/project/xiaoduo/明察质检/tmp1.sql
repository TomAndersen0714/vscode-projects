-- DROP TABLE test.datetype_test_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS test.datetype_test_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `dt` DateTime
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`id`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE test.datetype_test_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS test.datetype_test_all ON CLUSTER cluster_3s_2r
AS test.datetype_test_local
ENGINE = Distributed('cluster_3s_2r', 'test', 'datetype_test_local', rand());