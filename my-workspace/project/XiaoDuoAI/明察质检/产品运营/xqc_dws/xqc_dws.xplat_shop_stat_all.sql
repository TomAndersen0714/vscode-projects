-- DROP TABLE xqc_dws.xplat_shop_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.xplat_shop_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `company_id` String,
    `company_name` String,
    `company_short_name` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `dialog_cnt` Int64,
    `cnick_uv` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY seller_nick
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.xplat_shop_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.xplat_shop_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_shop_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'xplat_shop_stat_local', rand())


-- DROP TABLE buffer.xqc_dws_xplat_shop_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dws_xplat_shop_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_shop_stat_all
ENGINE = Buffer('xqc_dws', 'xplat_shop_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)