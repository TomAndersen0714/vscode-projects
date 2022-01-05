-- 质检报表-平台-获取子账号分组
-- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
-- parent_department_id全为空,则代表树层次遍历完毕
SELECT
    level_1.parent_department_id AS parent_department_id,
    level_2_3_4.department_id AS department_id,
    if(
        level_1.department_id!='', 
        concat(level_1.department_name,'-',level_2_3_4.department_name),
        level_2_3_4.department_name
    ) AS department_name
FROM (
    SELECT
        level_2.parent_department_id AS parent_department_id,
        level_3_4.department_id AS department_id,
        if(
            level_2.department_id!='', 
            concat(level_2.department_name,'-',level_3_4.department_name),
            level_3_4.department_name
        ) AS department_name
    FROM (
        SELECT
            level_3.parent_department_id AS parent_department_id,
            level_4.department_id AS department_id,
            if(
                level_3.department_id!='', 
                concat(level_3.department_name,'-',level_4.department_name),
                level_4.department_name
            ) AS department_name
        FROM (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id }}'
            AND (
                parent_id GLOBAL IN (
                    SELECT DISTINCT
                        _id AS department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id }}'
                ) -- 清除子账号父分组被删除, 而子分组依旧存在的脏数据
                OR 
                parent_id = '' -- 保留顶级分组
            )
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id }}'
        ) AS level_3
        ON level_4.parent_department_id = level_3.department_id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id AS department_id,
            name AS department_name,
            parent_id AS parent_department_id
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id }}'
    ) AS level_2
    ON level_3_4.parent_department_id = level_2.department_id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id AS department_id,
        name AS department_name,
        parent_id AS parent_department_id
    FROM ods.xinghuan_department_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id }}'
) AS level_1
ON level_2_3_4.parent_department_id = level_1.department_id