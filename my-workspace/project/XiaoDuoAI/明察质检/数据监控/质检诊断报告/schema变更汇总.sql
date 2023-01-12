/*
xqc_dim.qc_norm_group_full_all
*/
ALTER TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids Array(String) AFTER `parent_id`

ALTER TABLE xqc_dim.qc_norm_group_full_all ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids Array(String) AFTER `parent_id`


/*
xqc_dws.tag_group_stat_all
*/
CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `qc_norm_id` String,
    `tag_group_id` String,
    `tag_group_level` Int64,
    `add_score_dialog_cnt` Int64,
    `subtract_score_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.tag_group_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'tag_group_stat_local', rand())

/*
xqc_dim.snick_full_info_all
*/
CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `platform` String,
    `shop_id` String,
    `department_id` String,
    `department_name` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `superior_id` String,
    `superior_name` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, platform)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_full_info_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_full_info_local', rand())


/*
xqc_dws.snick_stat_all
*/
ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN subtract_score_dialog_cnt Int64 AFTER `dialog_cnt`,
ADD COLUMN add_score_dialog_cnt Int64 AFTER `subtract_score_dialog_cnt`

ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN subtract_score_dialog_cnt Int64 AFTER `dialog_cnt`,
ADD COLUMN add_score_dialog_cnt Int64 AFTER `subtract_score_dialog_cnt`
