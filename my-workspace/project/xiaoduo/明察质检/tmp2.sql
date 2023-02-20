CREATE DATABASE IF NOT EXISTS test ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE test.clickhouse_driver_test_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS test.clickhouse_driver_test_local ON CLUSTER cluster_3s_2r
(
    `num_a` Float64,
    `num_b` Float64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (num_a)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE test.clickhouse_driver_test_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS test.clickhouse_driver_test_all ON CLUSTER cluster_3s_2r
AS test.clickhouse_driver_test_local
ENGINE = Distributed('cluster_3s_2r', 'test', 'clickhouse_driver_test_local', rand());


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE buffer.test_clickhouse_driver_test_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.test_clickhouse_driver_test_buffer ON CLUSTER cluster_3s_2r
AS test.clickhouse_driver_test_all
ENGINE = Buffer('test', 'clickhouse_driver_test_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);