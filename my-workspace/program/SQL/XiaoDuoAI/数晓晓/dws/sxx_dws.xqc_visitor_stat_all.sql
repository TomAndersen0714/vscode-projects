CREATE DATABASE sxx_dws ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dws.xqc_visitor_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dws.xqc_visitor_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `cnick_uv` Int64,
    `voc_cnick_uv` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dws.xqc_visitor_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dws.xqc_visitor_stat_all ON CLUSTER cluster_3s_2r
AS sxx_dws.xqc_visitor_stat_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dws', 'xqc_visitor_stat_local', rand())

-- DROP TABLE buffer.sxx_dws_xqc_visitor_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_dws_xqc_visitor_stat_buffer ON CLUSTER cluster_3s_2r
AS sxx_dws.xqc_visitor_stat_all
ENGINE = Buffer('sxx_dws', 'xqc_visitor_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)