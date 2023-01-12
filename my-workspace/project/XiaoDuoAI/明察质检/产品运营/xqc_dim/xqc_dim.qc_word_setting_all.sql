-- JSON Example
{
  _id: ObjectId("62bfe3d735da128cda19f25d"),
  create_time: ISODate("2022-07-02T06:21:11.378Z"),
  update_time: ISODate("2022-07-02T06:21:11.378Z"),
  company_id: ObjectId("6273ad06d54236d7388cd070"),
  platform: 'tb',
  qc_norm_id: ObjectId("6273bb7bd54236d7388cfabb"),
  word: '发票',
  check_custom: true,
  check_service: true,
  group_id: ObjectId("62baa2a1b1557bd62683ff76"),
  check_labor_only: false,
  rule_category: 0
}

CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.qc_word_setting_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_word_setting_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `qc_norm_id` String,
    `word` String,
    `check_custom` String,
    `check_service` String,
    `group_id` String,
    `check_labor_only` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.qc_word_setting_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_word_setting_all ON CLUSTER cluster_3s_2r
AS tmp.qc_word_setting_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'qc_word_setting_local', rand())

-- DROP TABLE buffer.tmp_qc_word_setting_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_qc_word_setting_buffer ON CLUSTER cluster_3s_2r
AS tmp.qc_word_setting_all
ENGINE = Buffer('tmp', 'qc_word_setting_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_dim.qc_word_setting_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_word_setting_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `qc_norm_id` String,
    `word` String,
    `check_custom` String,
    `check_service` String,
    `group_id` String,
    `check_labor_only` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dim.qc_word_setting_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_word_setting_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_word_setting_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_word_setting_local', rand())

-- DROP TABLE buffer.xqc_dim_qc_word_setting_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dim_qc_word_setting_buffer ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_word_setting_all
ENGINE = Buffer('xqc_dim', 'qc_word_setting_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)