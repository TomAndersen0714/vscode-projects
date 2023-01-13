-- xqc_dim.qc_norm_group_full_all
ALTER TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids Array(String) AFTER `parent_id`

ALTER TABLE xqc_dim.qc_norm_group_full_all ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids Array(String) AFTER `parent_id`

-- INSERT INTO
-- PS: 此处需要JOIN 3次来获取分组的完整路径, 因为分组树高为4
INSERT INTO {sink_tbl}
SELECT
    -- parent_group_id 全为空, 即当前树型结构层次遍历完毕
    level_2_3_4._id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    level_2_3_4.platform,
    level_2_3_4.qc_norm_id,
    level_2_3_4.short_name AS name,
    if(
        level_1._id!='', 
        concat(level_1.short_name,'/',level_2_3_4.full_name),
        level_2_3_4.full_name
    ) AS full_name,
    (length(super_group_ids) + 1) AS level,
    level_2_3_4.parent_id AS parent_id,
    if(
        level_1._id!='', 
        arrayPushFront(level_2_3_4.super_group_ids, level_1._id),
        level_2_3_4.super_group_ids
    ) AS super_group_ids,
    {snapshot_ds_nodash} AS day
FROM (
    SELECT
        level_3_4.create_time,
        level_3_4.update_time,
        level_3_4.company_id,
        level_3_4.platform,
        level_3_4.qc_norm_id,
        level_3_4.parent_id AS parent_id,
        level_2.parent_id AS top_parent_id,
        level_3_4._id AS _id,
        level_3_4.short_name AS short_name,
        if(
            level_2._id!='', 
            concat(level_2.short_name,'/',level_3_4.full_name),
            level_3_4.full_name
        ) AS full_name,
        if(
            level_2._id!='', 
            arrayPushFront(level_3_4.super_group_ids, level_2._id),
            level_3_4.super_group_ids
        ) AS super_group_ids
    FROM (
        SELECT
            level_4.create_time,
            level_4.update_time,
            level_4.company_id,
            level_4.platform,
            level_4.qc_norm_id,
            level_4.parent_id AS parent_id,
            level_3.parent_id AS top_parent_id,
            level_4._id AS _id,
            level_4.short_name AS short_name,
            if(
                level_3._id!='', 
                concat(level_3.short_name,'/',level_4.full_name),
                level_4.full_name
            ) AS full_name,
            if(
                level_3._id!='', 
                arrayPushFront(level_4.super_group_ids, level_3._id),
                level_4.super_group_ids
            ) AS super_group_ids
        FROM (
            SELECT 
                *,
                name AS short_name,
                name AS full_name,
                parent_id AS top_parent_id,
                [] AS super_group_ids
            FROM {source_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name AS short_name,
                parent_id
            FROM {source_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name AS short_name,
            parent_id
        FROM {source_tbl}
        WHERE day = {snapshot_ds_nodash}
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name AS short_name,
        parent_id
    FROM {source_tbl}
    WHERE day = {snapshot_ds_nodash}
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id