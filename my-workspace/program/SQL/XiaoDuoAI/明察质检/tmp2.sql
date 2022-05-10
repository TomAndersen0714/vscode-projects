DROP TABLE test.t2_local ON CLUSTER cluster_3s_2r
CREATE TABLE test.t2_local ON CLUSTER cluster_3s_2r
(
    `id` UInt32,
    `type` String,
    `name` String
)
ENGINE = MergeTree()
PARTITION BY id
ORDER BY id SETTINGS index_granularity = 8192

CREATE TABLE test.t2_all ON CLUSTER cluster_3s_2r
AS test.t2_local
ENGINE = Distributed(cluster_3s_2r, test, t2_local, rand())