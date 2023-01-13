/* 
tmp.xinghuan_customize_rule_all
ods.xinghuan_customize_rule_all
 */
 -- PS: 非Replicated表, 无法使用ON CLUSTER cluster_3s_2r

ALTER TABLE tmp.xinghuan_customize_rule_all
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

ALTER TABLE ods.xinghuan_customize_rule_all
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

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
ALTER TABLE ods.xinghuan_dialog_tag_score_all UPDATE platform='ks' WHERE 1=1


/*
ods.qc_statistical_employee_all
*/
ALTER TABLE ods.qc_statistical_employee_all ADD COLUMN platform String AFTER `company_id`
ALTER TABLE ods.qc_statistical_employee_all UPDATE platform='ks' WHERE 1=1

/* 
ods.qc_statistical_department_all
*/
ALTER TABLE ods.qc_statistical_department_all ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_all UPDATE platform='ks' WHERE 1=1

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