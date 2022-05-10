-- 如果查询性能依旧无法满足需求, 则直接生成最终统计结果, 用于查询, 包括所有需要关联的维度数据

-- xqc_ads.xqc_snick_report_local
-- DROP TABLE xqc_ads.xqc_snick_report_local ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_ads.xqc_snick_report_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,

)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY `day`
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- xqc_ads.xqc_snick_report_all
-- DROP TABLE xqc_ads.xqc_snick_report_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ads.xqc_snick_report_all ON CLUSTER cluster_3s_2r
AS xqc_ads.xqc_snick_report_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ads', 'xqc_snick_report_local', rand())



-- ELT
