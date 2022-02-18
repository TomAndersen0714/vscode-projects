-- dwd.xdqc_dialog_all
tag_score_stats_count   Array(UInt32)
tag_score_stats_MD  UInt8
tag_score_stats_MM  UInt8
tag_score_add_stats_count   Array(UInt32)
tag_score_add_stats_MD  UInt8
tag_score_add_stats_MM  UInt8


-- mini/jd dialig_transfer 缺少自定义质检的规则表


-- tb/mini/jd/ks
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