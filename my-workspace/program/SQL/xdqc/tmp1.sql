-- 创建本地副本表
CREATE TABLE xqc_ods.qc_dialog_cnt_local ON CLUSTER cluster_3s_2r(
    `day` Int64,
    `platform` String,
    `seller_nick` String,
    `shop_id` String,
    `qc_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_ods/tables/{layer}_{shard}/qc_dialog_cnt_local', '{replica}') 
PARTITION BY (day, platform) 
ORDER BY snick 
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 创建分布式表
CREATE TABLE xqc_ods.qc_dialog_cnt_all ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_dialog_cnt_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'qc_dialog_cnt_local', rand())

-- 计算并写入对应的历史数据
-- 使用脚本按月执行
INSERT INTO xqc_ods.qc_dialog_cnt_all
SELECT
    day,
    'tb' AS platform,
    seller_nick,
    shop_id,
    COUNT(1) AS qc_dialog_cnt
FROM (
    SELECT
        toInt32(toYYYYMMDD(begin_time)) AS day,
        seller_nick,
        snick,
        _id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN {{ ds_nodash }}
    AND platform = 'tb'
    -- 过滤关联了质检标准的店铺
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = {{ ds_nodash }}
        AND platform = 'tb'
    )
    -- 过滤关联了质检标注的子账号
    AND snick GLOBAL IN (
        -- 查询所有关联了质检标准的子账号分组下的子账号
        SELECT DISTINCT snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = {{ ds_nodash }}
        AND platform = 'tb'
        AND department_id GLOBAL IN (
            -- 查询关联了质检标准的子账号分组ID
            SELECT DISTINCT department_id
            FROM ods.xinghuan_qc_norm_relate_all
            WHERE day = {{ ds_nodash }}
            AND platform = 'tb'
        )
    )
) AS dialog_info
GLOBAL LEFT JOIN (
    -- 查询所有关联了质检标准的子账号分组下的子账号
    SELECT DISTINCT
        snick,
        mp_shop_id AS shop_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = {{ ds_nodash }}
    AND platform = 'tb'
    AND department_id GLOBAL IN (
        -- 查询关联了质检标准的子账号分组ID
        SELECT DISTINCT department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = {{ ds_nodash }}
        AND platform = 'tb'
    )
) AS snick_shop_id
USING snick
GROUP BY day, seller_nick, shop_id