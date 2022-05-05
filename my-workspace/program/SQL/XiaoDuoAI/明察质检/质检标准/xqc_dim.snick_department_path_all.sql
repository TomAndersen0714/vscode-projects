-- Local Table
CREATE TABLE xqc_dim.snick_department_path_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `name` String,
    `parent_id` String,
    `is_edit` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, parent_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- Distributed Table
CREATE TABLE xqc_dim.snick_department_path_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_department_path_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_department_path_local', rand())


-- Transform
-- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
INSERT INTO xqc_dim.snick_department_path_all
SELECT
    -- parent_group_id全为空, 即当前树型结构层次遍历完毕
    level_2_3_4._id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    if(
        level_1._id!='', 
        concat(level_1.name,'/',level_2_3_4.name),
        level_2_3_4.name
    ) AS name,
    level_2_3_4.parent_id,
    level_2_3_4.is_edit,
    toYYYYMMDD(yesterday()) AS day
FROM (
    SELECT
        level_3_4._id,
        level_3_4.create_time,
        level_3_4.update_time,
        level_3_4.company_id,
        if(
            level_2._id!='', 
            concat(level_2.name,'/',level_3_4.name),
            level_3_4.name
        ) AS name, 
        level_3_4.parent_id,
        level_3_4.is_edit,
        level_2.parent_id AS top_parent_id
    FROM (
        SELECT
            level_4._id,
            level_4.create_time,
            level_4.update_time,
            level_4.company_id,
            if(
                level_3._id!='', 
                concat(level_3.name,'/',level_4.name),
                level_4.name
            ) AS name,
            level_4.parent_id,
            level_4.is_edit,
            level_3.parent_id AS top_parent_id
        FROM (
            SELECT 
                *,
                parent_id AS top_parent_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name,
                parent_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name,
            parent_id
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name,
        parent_id
    FROM ods.xinghuan_department_all
    WHERE day = toYYYYMMDD(yesterday())
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id