/*
tmp.xdqc_tag
tmp.xinghuan_department
tmp.xinghuan_employee_snick
tmp.xinghuan_employee
tmp.xinghuan_account
tmp.xinghuan_qc_norm
tmp.xdqc_tag_sub_category
tmp.xdqc_abnormal_check_setting
tmp.xdqc_excellent_check_setting
tmp.xdqc_emotion_check_item
tmp.xinghuan_qc_norm_relate
tmp.xinghuan_company
*/
RENAME TABLE tmp.xdqc_tag TO tmp.xdqc_tag_all
RENAME TABLE tmp.xinghuan_department TO tmp.xinghuan_department_all
RENAME TABLE tmp.xinghuan_employee_snick TO tmp.xinghuan_employee_snick_all
RENAME TABLE tmp.xinghuan_employee TO tmp.xinghuan_employee_all
RENAME TABLE tmp.xinghuan_account TO tmp.xinghuan_account_all
RENAME TABLE tmp.xinghuan_qc_norm TO tmp.xinghuan_qc_norm_all
RENAME TABLE tmp.xdqc_tag_sub_category TO tmp.xdqc_tag_sub_category_all
RENAME TABLE tmp.xdqc_abnormal_check_setting TO tmp.xdqc_abnormal_check_setting_all
RENAME TABLE tmp.xdqc_excellent_check_setting TO tmp.xdqc_excellent_check_setting_all
RENAME TABLE tmp.xdqc_emotion_check_item TO tmp.xdqc_emotion_check_item_all
RENAME TABLE tmp.xinghuan_qc_norm_relate TO tmp.xinghuan_qc_norm_relate_all
RENAME TABLE tmp.xinghuan_company TO tmp.xinghuan_company_all


/* 
tmp.xinghuan_customize_rule_all
ods.xinghuan_customize_rule_all
 */
 -- PS: 非Replicated表, 无法使用ON CLUSTER cluster_3s_2r
CREATE TABLE tmp.xinghuan_customize_rule_all (
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `name` String,
    `platform` String,
    `qc_norm_id` String,
    `channel` String,
    `seller_nick` String,
    `group` String,
    `cal_op` String,
    `score` Int32,
    `check_step` String,
    `check_sale_status` String,
    `check_rs` String,
    `seller_reply_content` String,
    `buyer_content` String,
    `buyer_type` String,
    `status` String,
    `qid` Int32
)
ENGINE = MergeTree()
ORDER BY (platform, _id) SETTINGS index_granularity = 8192


CREATE TABLE ods.xinghuan_customize_rule_all (
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `name` String,
    `platform` String,
    `qc_norm_id` String,
    `channel` String,
    `seller_nick` String,
    `group` String,
    `cal_op` String,
    `score` Int32,
    `check_step` String,
    `check_sale_status` String,
    `check_rs` String,
    `seller_reply_content` String,
    `buyer_content` String,
    `buyer_type` String,
    `status` String,
    `qid` Int32,
    `day` Int32
) ENGINE = MergeTree() PARTITION BY day
ORDER BY (platform, _id) SETTINGS index_granularity = 8192

/*
tmp.xqc_shop_local
tmp.xqc_shop_all
xqc_dim.xqc_shop_local
xqc_dim.xqc_shop_all
*/
CREATE TABLE tmp.xqc_shop_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192

CREATE TABLE tmp.xqc_shop_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_shop_local', rand())

CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r ENGINE = Ordinary

CREATE TABLE xqc_dim.xqc_shop_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY day
ORDER BY company_id
SETTINGS storage_policy = 'rr',
index_granularity = 8192

CREATE TABLE xqc_dim.xqc_shop_all ON CLUSTER cluster_3s_2r
AS xqc_dim.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'xqc_shop_local', rand())

/*
ods.xinghuan_dialog_tag_score_all
*/
ALTER TABLE ods.xinghuan_dialog_tag_score_all ADD COLUMN platform String AFTER `seller_nick`
ALTER TABLE ods.xinghuan_dialog_tag_score_all UPDATE platform='jd' WHERE 1=1

/*
ods.qc_statistical_all
*/
ALTER TABLE ods.qc_statistical_all 
ADD COLUMN rule_score_stats_count Int32 AFTER `tag_score_add_stats_count`,
ADD COLUMN rule_score_add_stats_count Int32 AFTER `rule_score_stats_count`


/*
ods.qc_statistical_employee_all
*/
ALTER TABLE ods.qc_statistical_employee_all ADD COLUMN platform String AFTER `company_id`
ALTER TABLE ods.qc_statistical_employee_all UPDATE platform='jd' WHERE 1=1

/* 
ods.qc_statistical_department_all
*/
ALTER TABLE ods.qc_statistical_department_all ADD COLUMN platform String AFTER `company_id`
ALTER TABLE ods.qc_statistical_department_all UPDATE platform='jd' WHERE 1=1

/*
buffer.xdqc_dialog_buffer
ods.xdqc_dialog_all
buffer.xdqc_dialog_update_buffer
ods.xdqc_dialog_update_all
dwd.xdqc_dialog_all
*/

-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer
CREATE TABLE buffer.xdqc_dialog_buffer
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer
CREATE TABLE buffer.xdqc_dialog_update_buffer
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`