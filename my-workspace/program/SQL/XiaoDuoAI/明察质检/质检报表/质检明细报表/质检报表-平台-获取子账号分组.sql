-- 质检报表-平台-获取子账号分组
SELECT DISTINCT
    concat(department_name,'//',department_id) AS department_name_id
FROM (
    SELECT DISTINCT
        snick
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = '{{ platform=tb }}'
    AND seller_nick GLOBAL IN (
        -- 查询对应企业-平台的店铺
        SELECT DISTINCT seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day=toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
    )
    AND snick GLOBAL IN (
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
        AND platform = '{{ platform=tb }}'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    )
) AS dialog_info
-- 获取最新版本的维度数据(T+1)
GLOBAL LEFT JOIN (
    SELECT
        snick, department_id, department_name
    FROM (
        -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
        SELECT snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = '{{ platform=tb }}'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS snick_info
    GLOBAL RIGHT JOIN (
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
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS level_1
        ON level_2_3_4.parent_department_id = level_1.department_id
    ) AS department_info
    USING (department_id)
) AS snick_department_map
USING(snick)
WHERE department_id!='' -- 清除匹配不上最新分组的子账号
ORDER BY department_name COLLATE 'zh'