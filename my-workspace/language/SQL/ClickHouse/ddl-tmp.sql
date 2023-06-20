CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE xqc_dim.company_version_local ON CLUSTER cluster_3s_2r NO DELAY;
CREATE TABLE IF NOT EXISTS xqc_dim.company_version_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `company_short_name` String,
    `tenant_id` String,
    `version_code` String,
    `version_name` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY company_id
SETTINGS index_granularity = 8192;


-- DROP TABLE xqc_dim.company_version_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.company_version_all ON CLUSTER cluster_3s_2r
AS xqc_dim.company_version_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'company_version_local', rand());


-- 分割线!!!
-- PS: 上面新版上线后先双写观察一段时间数据, 没问题后再下线以下的旧版, 通过执行DDL实现切换


-- DROP TABLE IF EXISTS xqc_dim.version_all ON CLUSTER cluster_3s_2r;
-- CREATE TABLE xqc_dim.version_all ON CLUSTER cluster_3s_2r
-- (
--     `id` String,
--     `shot_name` String,
--     `version` String,
--     `day` Int32
-- )
-- ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'version_local', rand());

CREATE VIEW IF NOT EXISTS xqc_dim.version_all ON CLUSTER cluster_3s_2r
AS
SELECT
    `company_id` AS id,
    `company_short_name` AS shot_name,
    `tenant_id` AS tenant_id,
    `version_code` AS version,
    `version_name` AS version_name,
    day
FROM xqc_dim.company_version_all;