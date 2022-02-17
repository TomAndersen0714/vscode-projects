-- 质检报表-店铺-分组-AI质检-加分行为触发次数
SELECT
    seller_nick AS `店铺`, 
    -- department_id,
    department_name AS `子账号分组`,
    sumIf(excellent_cnt, excellent_type=1) AS `需求挖掘`,
    sumIf(excellent_cnt, excellent_type=2) AS `商品细节解答`,
    sumIf(excellent_cnt, excellent_type=3) AS `卖点传达`,
    sumIf(excellent_cnt, excellent_type=4) AS `商品推荐`,
    sumIf(excellent_cnt, excellent_type=5) AS `退换货理由修改`,
    sumIf(excellent_cnt, excellent_type=6) AS `主动跟进`,
    sumIf(excellent_cnt, excellent_type=7) AS `无货挽回`,
    sumIf(excellent_cnt, excellent_type=8) AS `活动传达`,
    sumIf(excellent_cnt, excellent_type=9) AS `店铺保障`,
    sumIf(excellent_cnt, excellent_type=10) AS `催拍催付`,
    sumIf(excellent_cnt, excellent_type=11) AS `核对地址`,
    sumIf(excellent_cnt, excellent_type=12) AS `好评引导`,
    sumIf(excellent_cnt, excellent_type=13) AS `优秀结束语`
FROM (
    SELECT
        toInt32(toYYYYMMDD(begin_time)) AS day,
        seller_nick,
        snick,
        excellent_type,
        excellent_cnt
    FROM dwd.xdqc_dialog_all
    ARRAY JOIN
        excellents_type AS excellent_type, 
        excellents_count AS excellent_cnt
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND snick GLOBAL IN (
        -- 查询对应企业-平台的所有子账号, 不论其是否绑定员工
        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = '{{ platform }}'
        AND company_id = '{{ company_id }}'
    )
    AND excellent_cnt!=0
) AS ai_abnormal_info
GLOBAL LEFT JOIN (
    -- 获取最新版本的维度数据(T+1)
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
GROUP BY seller_nick, department_id, department_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY seller_nick, department_name