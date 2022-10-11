CREATE DATABASE IF NOT EXISTS trino ON CLUSTER cluster_3s_2r

CREATE TABLE trino.xqc_shop_local on cluster cluster_3s_2r
(
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}'
)
PARTITION BY `day` 
ORDER BY (shop_id, cnick_id)
SETTINGS index_granularity=8192, storage_policy='rr'


CREATE TABLE tmp.xqc_shop_all on cluster cluster_3s_2r
as tmp.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_shop_local', rand())


CREATE TABLE buffer.xqc_tmp_shop_buffer ON CLUSTER cluster_3s_2r
AS tmp.xqc_shop_all
ENGINE = Buffer('tmp', 'xqc_shop_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)