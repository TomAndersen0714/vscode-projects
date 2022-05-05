INSERT INTO xqc_dim.qc_norm_group_path_all
SELECT
    level_2_3_4._id AS _id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    level_2_3_4.platform,
    level_2_3_4.qc_norm_id,
    if(
        level_1._id!='', 
        concat(level_1.name,'/',level_2_3_4.name),
        level_2_3_4.name
    ) AS name,
    level_2_3_4.level AS level,
    level_2_3_4.parent_id AS parent_id,
    20220425 AS day
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
        if(
            level_2._id!='', 
            concat(level_2.name,'/',level_3_4.name),
            level_3_4.name
        ) AS name
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
            if(
                level_3._id!='', 
                concat(level_3.name,'/',level_4.name),
                level_4.name
            ) AS name
        FROM (
            SELECT 
                *,
                parent_id AS top_parent_id
            FROM xqc_dim.qc_norm_group_all
            WHERE day = 20220425
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name,
                parent_id
            FROM xqc_dim.qc_norm_group_all
            WHERE day = 20220425
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name,
            parent_id
        FROM xqc_dim.qc_norm_group_all
        WHERE day = 20220425
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name,
        parent_id
    FROM xqc_dim.qc_norm_group_all
    WHERE day = 20220425
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id