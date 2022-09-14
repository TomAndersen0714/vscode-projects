-- 客户评价满意度-分析-客服满意率分布
SELECT
    CONCAT('{{ day.start=week_ago }}',' - ','{{ day.end=yesterday }}') AS `时间`,
    platform AS `平台`,
    seller_nick AS `店铺`,
    snick AS `子账号`,
    employee_name AS `客服`,
    dialog_cnt AS `总会话量`,
    eval_sum AS `总评价量`,
    satisfy_pct AS `满意率`,
    CONCAT(toString(satisfy_pct),'%') AS `满意率%`,
    eval_code_0_cnt AS `非常满意`,
    eval_code_1_cnt AS `满意`,
    eval_code_2_cnt AS `一般`,
    source_1_code_3_eval_cnt AS `不满意(主动评价)`,
    source_0_2_code_3_eval_cnt AS `不满意(邀评)`,
    source_1_code_4_eval_cnt AS `非常不满意(主动评价)`,
    source_0_2_code_4_eval_cnt AS `非常不满意(邀评)`
FROM (
    SELECT
        platform,
        seller_nick,
        snick,
        dialog_cnt,
        eval_sum,
        satisfy_pct,
        eval_code_0_cnt,
        eval_code_1_cnt,
        eval_code_2_cnt,
        source_1_code_3_eval_cnt,
        source_0_2_code_3_eval_cnt,
        source_1_code_4_eval_cnt,
        source_0_2_code_4_eval_cnt
    FROM (
        -- 客服维度-会话量统计
        SELECT
            platform,
            seller_nick,
            snick,
            COUNT(1) AS dialog_cnt
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        -- 筛选指定子账号
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
        AND snick GLOBAL IN (
            -- 获取最新版本的维度数据(T+1)
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        GROUP BY platform, seller_nick, snick
    ) AS snick_dialog_stat_info
    GLOBAL LEFT JOIN (
        -- 客服满意率统计
        SELECT
            seller_nick,
            snick,
            COUNT(1) AS eval_sum,
            SUM(eval_code=0) AS eval_code_0_cnt,
            SUM(eval_code=1) AS eval_code_1_cnt,
            SUM(eval_code=2) AS eval_code_2_cnt,
            SUM(source=1 AND eval_code=3) AS source_1_code_3_eval_cnt,
            SUM((source=0 OR source=2) AND eval_code=3) AS source_0_2_code_3_eval_cnt,
            SUM(source=1 AND eval_code=4) AS source_1_code_4_eval_cnt,
            SUM((source=0 OR source=2) AND eval_code=4) AS source_0_2_code_4_eval_cnt,
            if(eval_sum!=0, round((eval_code_0_cnt + eval_code_1_cnt)/eval_sum*100,2), 0.00) AS satisfy_pct
        FROM (
            SELECT
                replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
                replaceOne(user_nick, 'cntaobao', '') AS snick,
                eval_code,
                source
            FROM ods.kefu_eval_detail_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
            -- 过滤买家已评价记录
            AND eval_time != ''
            -- 下拉框-店铺
            AND (
                '{{ seller_nicks }}'=''
                OR
                seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
            )
            AND snick IN (
                -- 当前企业对应的子账号
                SELECT DISTINCT snick
                FROM (
                    SELECT distinct snick, username
                    FROM ods.xinghuan_employee_snick_all AS snick_info
                    GLOBAL LEFT JOIN (
                        SELECT distinct
                            _id AS employee_id, username
                        FROM ods.xinghuan_employee_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    ) AS employee_info
                    USING(employee_id)
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    -- 下拉框-子账号分组id
                    AND (
                        '{{ department_ids }}'=''
                        OR
                        department_id IN splitByChar(',','{{ department_ids }}')
                    )
                ) AS snick_employee_info
            )
        ) AS satisfy_info
        GROUP BY seller_nick, snick
    ) AS snick_satisfy_stat_info
    USING(seller_nick, snick)
) AS snick_stat_info
GLOBAL LEFT JOIN (
    SELECT snick, employee_name
    FROM (
        -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
        SELECT snick, employee_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id, username AS employee_name
        FROM ods.xinghuan_employee_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS employee_info
    USING(employee_id)
) AS employee_info
USING(snick)
ORDER BY satisfy_pct