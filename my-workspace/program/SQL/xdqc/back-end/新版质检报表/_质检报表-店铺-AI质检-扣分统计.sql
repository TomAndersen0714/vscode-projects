SELECT 
    seller_nick AS `店铺`, 
    -- department_id,
    department_name AS `子账号分组`,
    sumIf(abnormal_cnt, abnormal_type=1) AS `非客服结束会话`,
    sumIf(abnormal_cnt, abnormal_type=2) AS `漏跟进`,
    sumIf(abnormal_cnt, abnormal_type=3) AS `快捷短语重复`,
    sumIf(abnormal_cnt, abnormal_type=4) AS `生硬拒绝`,
    sumIf(abnormal_cnt, abnormal_type=5) AS `欠缺安抚`,
    sumIf(abnormal_cnt, abnormal_type=6) AS `答非所问`,
    sumIf(abnormal_cnt, abnormal_type=7) AS `单字回复`,
    sumIf(abnormal_cnt, abnormal_type=8) AS `单句响应慢`,
    sumIf(abnormal_cnt, abnormal_type=9) AS `产品不熟悉`,
    sumIf(abnormal_cnt, abnormal_type=10) AS `活动不熟悉`,
    sumIf(abnormal_cnt, abnormal_type=11) AS `内部回复慢`,
    sumIf(abnormal_cnt, abnormal_type=12) AS `回复严重超时`,
    sumIf(abnormal_cnt, abnormal_type=13) AS `撤回人工消息`,
    sumIf(abnormal_cnt, abnormal_type=14) AS `单表情回复`,
    sumIf(abnormal_cnt, abnormal_type=15) AS `异常撤回`,
    sumIf(abnormal_cnt, abnormal_type=16) AS `转接前未有效回复`,
    sumIf(abnormal_cnt, abnormal_type=17) AS `超时未回复`,
    sumIf(abnormal_cnt, abnormal_type=18) AS `顾客撤回`,
    sumIf(abnormal_cnt, abnormal_type=19) AS `前后回复矛盾`,
    sumIf(abnormal_cnt, abnormal_type=20) AS `撤回机器人消息`,
    sumIf(abnormal_cnt, abnormal_type=21) AS `第三方投诉或曝光`,
    sumIf(abnormal_cnt, abnormal_type=22) AS `顾客提及投诉或举报`,
    sumIf(abnormal_cnt, abnormal_type=23) AS `差评或要挟差评`,
    sumIf(abnormal_cnt, abnormal_type=24) AS `反问/质疑顾客`,
    sumIf(abnormal_cnt, abnormal_type=25) AS `违禁词`,
    sumIf(abnormal_cnt, abnormal_type=26) AS `客服冷漠讥讽`,
    sumIf(abnormal_cnt, abnormal_type=27) AS `顾客怀疑假货`,
    sumIf(abnormal_cnt, abnormal_type=28) AS `客服态度消极敷衍`,
    sumIf(abnormal_cnt, abnormal_type=29) AS `售后不满意`
FROM (
    SELECT 
        toInt32(toYYYYMMDD(begin_time)) AS day,
        seller_nick,
        snick,
        abnormal_type,
        abnormal_cnt
    FROM dwd.xdqc_dialog_all
    ARRAY JOIN
        abnormals_type AS abnormal_type, 
        abnormals_count AS abnormal_cnt
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND snick GLOBAL IN (
        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = '{{ platform }}'
        AND company_id = '{{ company_id }}'
    )
    AND abnormal_cnt!=0
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