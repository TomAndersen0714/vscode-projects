CREATE TABLE IF NOT EXISTS tmp.truncate_test_local ON CLUSTER cluster_3s_2r (
    `a` Int8,
    `b` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/truncate_test_local',
    '{replica}'
)
ORDER BY `a` SETTINGS index_granularity=8192,storage_policy='default_policy'

-- Create distributed table
CREATE TABLE IF NOT EXISTS tmp.truncate_test_all ON CLUSTER cluster_3s_2r 
AS tmp.truncate_test_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'truncate_test_local', rand())

-- Insert into table
INSERT INTO tmp.truncate_test_all VALUES(0,'Tom'),(1,'Alise')

-- Truncate table
TRUNCATE TABLE tmp.truncate_test_local ON CLUSTER cluster_3s_2r

/* ClickHouse 数据类型测试 */
DROP TABLE IF EXISTS test.type_test
CREATE TABLE test.type_test
(
    `arr` Array(Int64),
    `tup` Tuple(Int64,String),
    `enu` Enum8('status_1'=1,'status_2'=2),
    `nest` Nested(
        `id` UInt8,
        `name` String
    ),
    Decimal32
)
ENGINE = Memory
INSERT INTO test.type_test VALUES('[1,2]',(1,2),'status_1',[1],['Tom']);
INSERT INTO test.type_test VALUES ([1, 2], (1, '2'), 'status_1', [1], ['Tom']);
INSERT INTO test.type_test VALUES ([1, 2], [1, '2'], 'status_1', [1], ['Tom'])


TRUNCATE TABLE tmp.truncate_test_local ON CLUSTER cluster_3s_2r
INSERT INTO tmp.truncate_test_all VALUES (0, '0'), (1, '1'), (3, 'Alise'), (6, 'Tom'), (7, 'Tom'), (6, 'A'), (7, 'Tom'), (3, 'Tom')



DROP TABLE IF EXISTS tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r (
    `a` Int8,
    `b` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/truncate_test_local_1',
    '{replica}'
)
ORDER BY `a` SETTINGS index_granularity=8192,storage_policy='default_policy'

DROP TABLE IF EXISTS tmp.truncate_test_all_1 ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS tmp.truncate_test_all_1 ON CLUSTER cluster_3s_2r 
AS tmp.truncate_test_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'truncate_test_local_1', rand())