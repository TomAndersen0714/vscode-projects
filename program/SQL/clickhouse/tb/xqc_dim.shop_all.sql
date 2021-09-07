CREATE TABLE xqc_dim.shop_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `nick` String,
    `is_create_qc_engine_shop` String,
    `is_close` String,
    `groups` String,
    `nlu_code` String,
    `shop_id` String,
    `is_start_use` String,
    `start_use_time` DateTime64(3),
    `expire_time` DateTime64(3),
    `version` UInt32,
    `whitelist` Array(String),
    `is_phone_verified` String,
    `phone` String,
    `name` String,
    `source` String -- 数据源所在平台:tb
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/shop_local',
    '{replica}'
)
ORDER BY (`shop_id`, `nick`,`_id`) 
SETTINGS storage_policy = 'rr',index_granularity = 8192


CREATE TABLE xqc_dim.shop_all ON CLUSTER cluster_3s_2r
AS xqc_dim.shop_local
ENGINE = Distributed(
    'cluster_3s_2r','xqc_dim','shop_local',rand()
)


CREATE TABLE xqc_dim.xinghuan_mc_company_shop_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `mp_shop_id` String,
    `platform` String,
    `source` String -- 数据源所在平台:tb
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/xinghuan_mc_company_shop_local',
    '{replica}'
)
PARTITION BY (`platform`)
ORDER BY (`company_id`, `mp_shop_id`,`_id`) 
SETTINGS storage_policy = 'rr',index_granularity = 8192


CREATE TABLE xqc_dim.xinghuan_mc_company_shop_all
AS xqc_dim.xinghuan_mc_company_shop_local
ENGINE = Distributed(
    'cluster_3s_2r','xqc_dim','xinghuan_mc_company_shop_local',rand()
)


CREATE TABLE xqc_dim.xinghuan_mc_company_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `name` String,
    `shot_name` String,
    `logo` String,
    `url` String,
    `source` String -- 数据源所在平台:tb
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/xinghuan_mc_company_local',
    '{replica}'
)
ORDER BY (`_id`, `name`,`shot_name`) 
SETTINGS storage_policy = 'rr',index_granularity = 8192

CREATE TABLE xqc_dim.xinghuan_mc_company_all
AS xqc_dim.xinghuan_mc_company_local
ENGINE = Distributed(
    'cluster_3s_2r','xqc_dim','xinghuan_mc_company_local',rand()
)


CREATE TABLE xqc_dim.xinghuan_mc_xdmp_shop_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `platform` String,
    `plat_shop_id` String,
    `plat_shop_name` String,
    `shot_name` String,
    `plat_user_id` String,
    `source` String -- 数据源所在平台:tb
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/xinghuan_mc_xdmp_shop_local',
    '{replica}'
)
ORDER BY (`_id`, `platform`, `plat_shop_id`,) 
SETTINGS storage_policy = 'rr',index_granularity = 8192


CREATE TABLE xqc_dim.xinghuan_mc_xdmp_shop_all ON CLUSTER cluster_3s_2r
AS xqc_dim.xinghuan_mc_xdmp_shop_local
ENGINE = Distributed(
    'cluster_3s_2r','xqc_dim','xinghuan_mc_xdmp_shop_all',rand()
)

