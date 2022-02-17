-- 质检报表-店铺
-- 统计维度: 子账号分组, 下钻维度: 会话
SELECT
    seller_nick AS `店铺`,
    -- department_id,
    department_name AS `子账号分组`,
    count(distinct snick) AS `客服人数`,
    sum(dialog_cnt) AS `总会话量`,
    round((`总会话量`*100 + sum(score_add)- sum(score))/`总会话量`,2) AS `平均分`,
    `总会话量` AS `AI质检量`,
    sum(abnormal_dialog_cnt) AS `AI异常会话量`,
    concat(
        toString(round((`AI异常会话量` * 100 / `总会话量`), 2)),
        '%'
    ) AS `AI扣分会话比例`,
    sum(excellents_dialog_cnt) AS `AI加分会话量`,
    round((0.9604 * `总会话量`) /(0.0025 * `总会话量` + 0.9604), 0) as `建议抽检量`,
    sum(mark_dialog_cnt) AS `人工抽检量`,
    concat(
        toString(round((`人工抽检量` * 100 / `总会话量`), 2)),
        '%'
    ) as `抽检比例`,
    sum(tag_score_dialog_cnt) `人工质检扣分会话量`,
    concat(
        toString(round((`人工质检扣分会话量` * 100 / `总会话量`), 2)),
        '%'
    ) AS `人工扣分会话比例`,
    sum(tag_score_add_dialog_cnt) `人工质检加分会话量`
FROM (
    SELECT
        seller_nick,
        snick,
        COUNT(1) AS dialog_cnt,
        sum(score_add) AS score_add,
        sum(score) AS score,
        sum(arraySum(abnormals_count)!=0) AS abnormal_dialog_cnt,
        sum(arraySum(excellents_count)!=0) AS excellents_dialog_cnt,
        sum(length(mark_ids)!=0) AS mark_dialog_cnt,
        sum(length(tag_score_stats_id)!=0) AS tag_score_dialog_cnt,
        sum(length(tag_score_add_stats_id)!=0) AS tag_score_add_dialog_cnt
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = '{{ platform=tb }}'
    AND snick GLOBAL IN (
        -- 获取最新版本的维度数据(T+1)
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = '{{ platform=tb }}'
        AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        -- 下拉框-子账号分组
        AND (
            '{{ depatment_ids= }}'=''
            OR
            department_id IN splitByChar(',','{{ depatment_ids= }}')
        )
    )
    GROUP BY seller_nick, snick
) AS dialog_info
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
        AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
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
                    AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
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
                    AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
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
                AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
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
            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        ) AS level_1
        ON level_2_3_4.parent_department_id = level_1.department_id
    ) AS department_info
    USING (department_id)
) AS snick_department_map
USING(snick)
GROUP BY seller_nick, department_id, department_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY seller_nick, department_name