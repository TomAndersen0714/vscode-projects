-- xqc_dim.snick_department_full_local
-- DROP TABLE xqc_dim.snick_department_full_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_department_full_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `name` String,
    `full_name` String,
    `parent_id` String,
    `is_edit` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, _id)
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- xqc_dim.snick_department_full_all
-- DROP TABLE xqc_dim.snick_department_full_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_department_full_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_department_full_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_department_full_local', rand())


-- ELT
-- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
INSERT INTO xqc_dim.snick_department_full_all
SELECT
    -- parent_group_id全为空, 即当前树型结构层次遍历完毕
    level_2_3_4._id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    level_2_3_4.short_name,
    if(
        level_1._id!='', 
        concat(level_1.short_name,'/',level_2_3_4.full_name),
        level_2_3_4.full_name
    ) AS full_name,
    level_2_3_4.parent_id,
    level_2_3_4.is_edit,
    toYYYYMMDD(yesterday()) AS day
FROM (
    SELECT
        level_3_4._id,
        level_3_4.create_time,
        level_3_4.update_time,
        level_3_4.company_id,
        level_3_4.short_name,
        if(
            level_2._id!='', 
            concat(level_2.short_name,'/',level_3_4.full_name),
            level_3_4.full_name
        ) AS full_name, 
        level_3_4.parent_id,
        level_3_4.is_edit,
        level_2.parent_id AS top_parent_id
    FROM (
        SELECT
            level_4._id,
            level_4.create_time,
            level_4.update_time,
            level_4.company_id,
            level_4.short_name,
            if(
                level_3._id!='', 
                concat(level_3.short_name,'/',level_4.full_name),
                level_4.full_name
            ) AS full_name,
            level_4.parent_id,
            level_4.is_edit,
            level_3.parent_id AS top_parent_id
        FROM (
            SELECT 
                *,
                name AS short_name,
                name AS full_name,
                parent_id AS top_parent_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name AS short_name,
                parent_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name AS short_name,
            parent_id
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name AS short_name,
        parent_id
    FROM ods.xinghuan_department_all
    WHERE day = toYYYYMMDD(yesterday())
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id