-- dwd.xdqc_dialog_all 质检会话表增加人工质检对应字段
tag_score_stats_count Array(UInt32)
tag_score_stats_md Array(UInt8)
tag_score_stats_mm Array(UInt8)
tag_score_add_stats_count Array(UInt32)
tag_score_add_stats_md Array(UInt8)
tag_score_add_stats_mm Array(UInt8)

"buffer.xdqc_dialog_update_buffer",
"ods.xdqc_dialog_update_all",
"ods.xdqc_dialog_update_local",

"buffer.xdqc_dialog_buffer",
"ods.xdqc_dialog_all",
"ods.xdqc_dialog_local",

"dwd.xdqc_dialog_all",
"dwd.xdqc_dialog_local",

-- ods.xdqc_dialog_local
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_local
ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- dwd.xdqc_dialog_local
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`
-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`


-- ks平台ClickHouse还未升级到多盘和zk分布式形式, 无法直接对齐
PS: 已经上zk, 并且升级到了多盘，可以开始正常作业


-- tb/dy/pdd/mini/ks 平台自定义质检表是旧版, 需要切换到新版, 并增加对应字段
ALTER TABLE tmp.xinghuan_customize_rule_local ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`, ADD COLUMN qc_norm_id String AFTER `platform`
ALTER TABLE tmp.xinghuan_customize_rule_all ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`, ADD COLUMN qc_norm_id String AFTER `platform`
ALTER TABLE ods.xinghuan_customize_rule_local ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`, ADD COLUMN qc_norm_id String AFTER `platform`
ALTER TABLE ods.xinghuan_customize_rule_all ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`, ADD COLUMN qc_norm_id String AFTER `platform`

-- mini/jd dialig_transfer 缺少自定义质检的规则表
tmp.xinghuan_customize_rule_local
tmp.xinghuan_customize_rule_all
ods.xinghuan_customize_rule_local
ods.xinghuan_customize_rule_all

-- tb/mini/jd/ks 缺少质检已订阅店铺表
CREATE TABLE IF NOT EXISTS tmp.xqc_shop_local ON CLUSTER cluster_3s_2r
(
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


CREATE TABLE IF NOT EXISTS tmp.xqc_shop_all ON CLUSTER cluster_3s_2r
(
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
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_shop_local', rand())


CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r ENGINE = Ordinary


CREATE TABLE IF NOT EXISTS xqc_dim.xqc_shop_local ON CLUSTER cluster_3s_2r
(
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
SETTINGS storage_policy = 'rr', index_granularity = 8192


CREATE TABLE IF NOT EXISTS xqc_dim.xqc_shop_all ON CLUSTER cluster_3s_2r
(
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
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'xqc_shop_local', rand())