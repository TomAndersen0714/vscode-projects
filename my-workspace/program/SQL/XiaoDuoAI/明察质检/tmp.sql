CREATE DATABASE IF NOT EXISTS trino ON CLUSTER cluster_3s_2r ENGINE=Ordinary

CREATE TABLE trino.xqc_shop_local ON CLUSTER cluster_3s_2r
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
ORDER BY (company_id, shop_id)
SETTINGS index_granularity=8192, storage_policy='rr'


CREATE TABLE trino.xqc_shop_all ON CLUSTER cluster_3s_2r
AS trino.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'trino', 'xqc_shop_local', rand())


CREATE TABLE buffer.trino_xqc_shop_buffer ON CLUSTER cluster_3s_2r
AS trino.xqc_shop_all
ENGINE = Buffer('trino', 'xqc_shop_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


INSERT INTO clickhouse.trino.xqc_shop_all
SELECT
    CAST(create_time AS varbinary)
FROM mongodb.xqc.shop;