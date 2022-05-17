-- 质检报表-客服-查看详情-人工质检明细
-- 统计维度: 平台/店铺/子账号/会话, 下钻维度路径: 日期/平台/店铺/子账号分组/子账号/会话
SELECT
    dialog_id,
    day AS dialog_day,
    day AS `日期`,
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    seller_nick AS `店铺`,
    department_name AS `子账号分组`,
    snick AS `客服子账号`,
    cnick AS `顾客名称`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,

    -- 人工质检结果
    arrayStringConcat(manual_tag_names,'$$') AS `人工质检标签`,
    arrayStringConcat(manual_tag_cnts,'$$') AS `人工质检触发次数`

FROM (
    -- 人工质检结果-会话维度质检项触发次数统计
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        cnick,
        dialog_id,
        arrayMap(x->toString(x), groupArray(tag_name)) AS manual_tag_names,
        arrayMap(x->toString(x), groupArray(tag_sum)) AS manual_tag_cnts
    FROM (
        -- 人工质检-标签触发次数统计
        SELECT
            day,
            platform,
            seller_nick,
            snick,
            cnick,
            dialog_id,
            tag_id,
            SUM(tag_cnt + if(tag_md>0, 1, 0)) AS tag_sum
        FROM (
            -- 人工质检项-扣分项
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                cnick,
                _id AS dialog_id,
                tag_score_stats_id AS tag_ids,
                tag_score_stats_count AS tag_cnts,
                -- 是否打标在会话上
                if(
                    tag_score_stats_md=[],
                    arrayResize([0], length(tag_score_stats_id)),
                    tag_score_stats_md
                ) AS tag_mds
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            -- 过滤空数据
            AND tag_score_stats_id != []
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 下拉框-子账号
            AND (
                    '{{ snicks }}'=''
                    OR
                    snick IN splitByChar(',','{{ snicks }}')
            )

            -- 人工质检项-加分项
            UNION ALL
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                cnick,
                _id AS dialog_id,
                tag_score_add_stats_id AS tag_ids,
                tag_score_add_stats_count AS tag_cnts,
                -- 是否打标在会话上
                if(
                    tag_score_add_stats_md=[],
                    arrayResize([0], length(tag_score_add_stats_id)),
                    tag_score_add_stats_md
                ) AS tag_mds
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            -- 过滤空数据
            AND tag_score_add_stats_id != []
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 下拉框-子账号
            AND (
                    '{{ snicks }}'=''
                    OR
                    snick IN splitByChar(',','{{ snicks }}')
            )
        ) AS ods_manual_tag
        ARRAY JOIN
            tag_ids AS tag_id,
            tag_cnts AS tag_cnt,
            tag_mds AS tag_md
        GROUP BY day, platform, seller_nick, snick, cnick, dialog_id, tag_id
    ) AS ods_manual_tag_stat
    GLOBAL LEFT JOIN (
        -- 关联人工质检项
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND rule_category = 2
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS dim_tag
    USING(tag_id)
    GROUP BY day, platform, seller_nick, snick, cnick, dialog_id
) AS stat_ai_check_info
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM (
        SELECT snick, employee_name, superior_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name, superior_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        SELECT
            _id AS department_id, full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
ORDER BY day ASC