
-- DROP TABLE IF EXISTS dim.shop_question_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS dim.shop_question_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `shop_id` String,
    `update_time` String,
    `answers` String,
    `create_time` String,
    `is_enabled` String,
    `is_keyword` String,
    `question` String,
    `question_status` String,
    `questions` String,
    `replies` String,
    `source` String,
    `version` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/dim/tables/{layer}_{shard}/shop_question_local','{replica}'
)
ORDER BY (`_id`,`shop_id`,`update_time`) SETTINGS index_granularity = 8192


-- DROP TABLE IF EXISTS dim.shop_question_all ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS dim.shop_question_all ON CLUSTER cluster_3s_2r 
AS dim.shop_question_local 
ENGINE = Distributed('cluster_3s_2r', 'dim', 'shop_question_local', rand())


CREATE TABLE IF NOT EXISTS tmp.shop_question_all ON CLUSTER cluster_3s_2r 
AS tmp.shop_question_local 
ENGINE = Distributed('cluster_3s_2r', 'dim', 'shop_question_local', rand())

--
CREATE TABLE IF NOT EXISTS tmp.test_tbl_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `version` Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tmp/tables/{layer}_{shard}/test_tbl_local','{replica}')
PARTITION BY `version`
ORDER BY (`_id`,`version`) SETTINGS index_granularity = 8192

CREATE TABLE IF NOT EXISTS tmp.test_tbl_all ON CLUSTER cluster_3s_2r
AS tmp.test_tbl_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'test_tbl_local', rand())

-- Drop partition on cluster
ALTER TABLE tmp.test_tbl_local ON CLUSTER cluster_3s_2r DROP PARTITION 1


/* ALTER TABLE tmp.test_tbl_local DROP IF EXISTS PARTITION 1,2 */
-- PS: ClickHouse不支持同时删除多个分区,但是支持同时在Cluster中执行
ALTER TABLE tmp.test_tbl_local DROP PARTITION 1
ALTER TABLE tmp.test_tbl_local ON CLUSTER cluster_3s_2r DROP PARTITION 1


-- Kudu
