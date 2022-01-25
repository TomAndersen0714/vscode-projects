xqc.shop -> xqc_dim.xqc_shop_all
xqc.company -> ods.xinghuan_company_all


-- xqc.shop -> ods.xinghuan_company_all
ALTER TABLE tmp.xinghuan_company_local ON CLUSTER cluster_3s_2r
ADD COLUMN `default_platform` String, ADD COLUMN `platforms` Array(String), ADD COLUMN `pri_center_id` String

ALTER TABLE tmp.xinghuan_company_all ON CLUSTER cluster_3s_2r
ADD COLUMN `default_platform` String, ADD COLUMN `platforms` Array(String), ADD COLUMN `pri_center_id` String

ALTER TABLE ods.xinghuan_company_local ON CLUSTER cluster_3s_2r
ADD COLUMN `default_platform` String, ADD COLUMN `platforms` Array(String), ADD COLUMN `pri_center_id` String

ALTER TABLE ods.xinghuan_company_all ON CLUSTER cluster_3s_2r
ADD COLUMN `default_platform` String, ADD COLUMN `platforms` Array(String), ADD COLUMN `pri_center_id` String

-- xqc.shop -> xqc_dim.xqc_shop_all
CREATE TABLE tmp.xqc_shop_local ON CLUSTER cluster_3s_2r
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
ENGINE = ReplicatedMergeTree('/clickhouse/tmp/tables/{layer}_{shard}/xqc_shop_local', '{replica}') 
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192

CREATE TABLE tmp.xqc_shop_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_shop_local', rand())


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
    `day` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_dim/tables/{layer}_{shard}/xqc_shop_local', '{replica}') 
PARTITION BY day
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192


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
    `day` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_dim/tables/{layer}_{shard}/xqc_shop_local', '{replica}') 
PARTITION BY (day, platform)
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192

CREATE TABLE xqc_dim.xqc_shop_all ON CLUSTER cluster_3s_2r
AS xqc_dim.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'xqc_shop_local', rand())