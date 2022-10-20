xqc_dws.xplat_shop_stat_all
xqc_dws.company_shop_monthly_stat_all


1. 创建临时表, 不用分区
-- DROP TABLE tmp.xqc_dws_xplat_shop_stat_local ON CLUSTER cluster_3s_2r
-- TRUNCATE TABLE tmp.xqc_dws_xplat_shop_stat_local ON CLUSTER cluster_3s_2r
CREATE TABLE tmp.xqc_dws_xplat_shop_stat_local ON CLUSTER cluster_3s_2r
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
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/xqc_dws_xplat_shop_stat_local',
    '{replica}'
)
ORDER BY seller_nick
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.xqc_dws_xplat_shop_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xqc_dws_xplat_shop_stat_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_xplat_shop_stat_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_dws_xplat_shop_stat_local', rand())

-- DROP TABLE buffer.tmp_xqc_dws_xplat_shop_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_xqc_dws_xplat_shop_stat_buffer ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_xplat_shop_stat_all
ENGINE = Buffer('tmp', 'xqc_dws_xplat_shop_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

2. 自 20220101 20221018, 输出至临时表
INSERT INTO buffer.tmp_xqc_dws_xplat_shop_stat_buffer
SELECT
    day,
    platform,
    company_id,
    company_name,
    company_short_name,
    shop_id,
    shop_name,
    seller_nick,
    dialog_cnt,
    cnick_uv
FROM (
    -- 指标统计
    SELECT
        toYYYYMMDD(begin_time) AS day,
        platform,
        seller_nick,
        COUNT(DISTINCT _id) AS dialog_cnt,
        COUNT(DISTINCT cnick) AS cnick_uv
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = {{{{ds_nodash}}}}
    AND platform IN {PLATFORMS}
    GROUP BY day, platform, seller_nick
) AS stat_info
GLOBAL LEFT JOIN (
    -- 关联企业和店铺信息
    SELECT
        company_id,
        company_name,
        company_short_name,
        platform,
        shop_id,
        shop_name,
        seller_nick
    FROM (
        SELECT
            _id AS company_id,
            name AS company_name,
            shot_name AS company_short_name
        FROM ods.xinghuan_company_all
        WHERE day = {SNAPSHOT_DS_NODASH}
    ) AS company_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            platform,
            company_id,
            shop_id,
            plat_shop_name AS shop_name,
            seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day = {SNAPSHOT_DS_NODASH}
    ) AS shop_info
    USING(company_id)
) AS dim_info
USING(platform, seller_nick)


3. 导出临时表数据, 存储到oss盘
docker exec -i 42198f0fe342 clickhouse-client --host=mini-bigdata-004 --port=19000 --query \
"SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all FORMAT Parquet" \
> tmp.xqc_dws_xplat_shop_stat_all.parq


4. 导入测试表, 测试
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query \
"INSERT INTO tmp.xqc_dws_xplat_shop_stat_all FORMAT Parquet" \
< tmp.xqc_dws_xplat_shop_stat_all.parq


docker exec -i 9043cb24167c clickhouse-client --port=19000 --query \
"INSERT INTO tmp.xqc_dws_xplat_shop_stat_all FORMAT Parquet" \
< tmp.xqc_dws_xplat_shop_stat_all.parq


4. 迁移数据写入到 xqc_dws.xplat_shop_stat_all
INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220101 AND 20220131

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220201 AND 20220231

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220301 AND 20220331

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220401 AND 20220431

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220501 AND 20220531

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220601 AND 20220631

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220701 AND 20220731

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220801 AND 20220831

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20220901 AND 20220931

INSERT INTO xqc_dws.xplat_shop_stat_all
SELECT * FROM tmp.xqc_dws_xplat_shop_stat_all
WHERE day BETWEEN 20221001 AND 20221017

5. 统计每个月份的数据 xqc_dws.company_shop_monthly_stat_all
