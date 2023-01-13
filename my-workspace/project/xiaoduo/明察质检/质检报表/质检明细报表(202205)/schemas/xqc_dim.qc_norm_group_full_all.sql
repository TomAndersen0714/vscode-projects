-- xqc_dim.qc_norm_group_full_local
-- DROP TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform`  String,
    `qc_norm_id` String,
    `name` String,
    `full_name` String,
    `level` Int32,
    `parent_id` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, platform)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- xqc_dim.qc_norm_group_full_all
-- DROP TABLE xqc_dim.qc_norm_group_full_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_norm_group_full_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_norm_group_full_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_norm_group_full_local', rand())


-- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
INSERT INTO xqc_dim.qc_norm_group_full_all
SELECT
    -- parent_group_id全为空, 即当前树型结构层次遍历完毕
    level_2_3_4._id AS _id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    level_2_3_4.platform,
    level_2_3_4.qc_norm_id,
    level_2_3_4.short_name,
    if(
        level_1._id!='', 
        concat(level_1.short_name,'/',level_2_3_4.full_name),
        level_2_3_4.full_name
    ) AS full_name,
    level_2_3_4.level AS level,
    level_2_3_4.parent_id AS parent_id,
    {ds_nodash} AS day
FROM (
    SELECT
        level_3_4.create_time,
        level_3_4.update_time,
        level_3_4.company_id,
        level_3_4.platform,
        level_3_4.qc_norm_id,
        level_3_4.level,
        level_3_4.parent_id AS parent_id,
        level_2.parent_id AS top_parent_id,
        level_3_4._id AS _id,
        level_3_4.short_name AS short_name,
        if(
            level_2._id!='', 
            concat(level_2.short_name,'/',level_3_4.full_name),
            level_3_4.full_name
        ) AS full_name
    FROM (
        SELECT
            level_4.create_time,
            level_4.update_time,
            level_4.company_id,
            level_4.platform,
            level_4.qc_norm_id,
            level_4.level,
            level_4.parent_id AS parent_id,
            level_3.parent_id AS top_parent_id,
            level_4._id AS _id,
            level_4.short_name AS short_name,
            if(
                level_3._id!='', 
                concat(level_3.short_name,'/',level_4.full_name),
                level_4.full_name
            ) AS full_name
        FROM (
            SELECT 
                *,
                name AS short_name,
                name AS full_name,
                parent_id AS top_parent_id
            FROM xqc_dim.qc_norm_group_all
            WHERE day = {ds_nodash}
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name AS short_name,
                parent_id
            FROM xqc_dim.qc_norm_group_all
            WHERE day = {ds_nodash}
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name AS short_name,
            parent_id
        FROM xqc_dim.qc_norm_group_all
        WHERE day = {ds_nodash}
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name AS short_name,
        parent_id
    FROM xqc_dim.qc_norm_group_all
    WHERE day = {ds_nodash}
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id