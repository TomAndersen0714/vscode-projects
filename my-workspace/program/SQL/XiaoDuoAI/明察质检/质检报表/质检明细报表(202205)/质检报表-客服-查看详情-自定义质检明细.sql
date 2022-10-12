-- 质检报表-客服-查看详情-自定义质检明细
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
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
    if(real_buyer_nick!='', real_buyer_nick, cnick) AS `顾客名称`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,

    -- 自定义质检结果
    arrayStringConcat(custom_tag_names,'$$') AS `自定义质检标签`,
    arrayStringConcat(custom_tag_cnts,'$$') AS `自定义质检触发次数`
FROM (
    -- 自定义质检结果-会话维度质检项触发次数统计
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        cnick,
        real_buyer_nick,
        dialog_id,
        arrayMap(x->toString(x), groupArray(tag_name)) AS custom_tag_names,
        arrayMap(x->toString(x), groupArray(tag_sum)) AS custom_tag_cnts
    FROM (
        SELECT
            day,
            platform,
            seller_nick,
            snick,
            cnick,
            real_buyer_nick,
            dialog_id,
            tag_id,
            SUM(tag_cnt) AS tag_sum
        FROM (
            -- 旧版本自定义质检项-扣分项
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                cnick,
                real_buyer_nick,
                _id AS dialog_id,
                arrayConcat(
                    -- 旧版本自定义质检项-扣分项-质检类型
                    arrayResize(['custom_subtract'], length(rule_stats_id), 'custom_subtract'),
                    -- 旧版本自定义质检项-加分项-质检类型
                    arrayResize(['custom_add'], length(rule_add_stats_id), 'custom_add'),
                    -- 新版本自定义质检项-消息质检项
                    arrayResize(['custom_message'], length(xrule_stats_id), 'custom_message'),
                    -- 新版本自定义质检项-会话质检项
                    arrayResize(['custom_dialog'], length(top_xrules_id), 'custom_dialog')
                ) AS tag_types,
                arrayConcat(
                    -- 旧版本自定义质检项-扣分项-质检项ID
                    rule_stats_id,
                    -- 旧版本自定义质检项-加分项-质检项ID
                    rule_add_stats_id,
                    -- 新版本自定义质检项-消息质检项-质检项ID
                    xrule_stats_id,
                    -- 新版本自定义质检项-会话质检项-质检项ID
                    top_xrules_id
                ) AS tag_ids,
                arrayConcat(
                    -- 旧版本自定义质检项-扣分项-质检项触发次数
                    rule_stats_count,
                    -- 旧版本自定义质检项-加分项-质检项触发次数
                    rule_add_stats_count,
                    -- 新版本自定义质检项-消息质检项-质检项触发次数
                    xrule_stats_count,
                    -- 新版本自定义质检项-会话质检项-质检项触发次数
                    top_xrules_count
                ) AS tag_cnts
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start_=week_ago }}'))
                AND toYYYYMMDD(toDate('{{ day.end_=yesterday }}'))
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks_ }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks_ }}')
            )
        ) AS ods_custom_tag
        ARRAY JOIN
            tag_ids AS tag_id,
            tag_cnts AS tag_cnt
        WHERE snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
        -- 下拉框-子账号
        AND (
            '{{ snicks_ }}'=''
            OR
            snick IN splitByChar(',','{{ snicks_ }}')
        )
        -- 排除空数据
        AND tag_cnt!=0
        GROUP BY day, platform, seller_nick, snick, cnick, real_buyer_nick, dialog_id, tag_id 
    ) AS ods_custom_tag
    GLOBAL LEFT JOIN (
        -- 查询自定义质检项
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND rule_category = 3
    ) AS dim_tag
    USING(tag_id)
    GROUP BY day, platform, seller_nick, snick, cnick, real_buyer_nick, dialog_id
) AS ods_custom_tag_stat
GLOBAL LEFT JOIN (
    -- 获取子账号完整信息
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
) AS dim_snick_department
USING(snick)
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
ORDER BY day ASC