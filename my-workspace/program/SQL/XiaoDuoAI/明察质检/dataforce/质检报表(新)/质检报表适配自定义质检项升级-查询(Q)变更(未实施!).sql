-- 质检报表所有质检总览部分进行修改
-- 质检结果总览-子账号维度
SELECT
    platform,
    seller_nick,
    snick,
    sum(score) AS score,
    sum(score_add) AS score_add,
    sum(ai_score) AS ai_score,
    sum(ai_score_add) AS ai_score_add,
    sum(mark_score) AS mark_score,
    sum(mark_score_add) AS mark_score_add,
    sum(rule_score) AS rule_score,
    sum(rule_score_add) AS rule_score_add,

    count(1) AS dialog_cnt,
    sum(mark_id_cnt > 0) AS mark_dialog_cnt,
    sum(ai_score > 0) AS abnormal_dialog_cnt,
    sum(ai_score_add > 0) AS excellents_dialog_cnt,
    sum(mark_score>0) AS tag_score_dialog_cnt,
    sum(mark_score_add>0) AS tag_score_add_dialog_cnt,
    sum(rule_score > 0) AS rule_dialog_cnt,
    sum(rule_score_add > 0) AS rule_add_dialog_cnt
FROM (
    SELECT
        _id,
        platform,
        seller_nick,
        snick,
        score,
        score_add,
        mark_score,
        mark_score_add,
        arraySum(rule_stats_score) + negate(arraySum(arrayFilter(x->x<0, xrule_stats_score)) + arraySum(arrayFilter(x->x<0, top_xrules))) AS rule_score,
        arraySum(rule_add_stats_score) + arraySum(arrayFilter(x->x>0, xrule_stats_score)) + arraySum(arrayFilter(x->x>0, top_xrules)) AS rule_score_add,
        score - mark_score - rule_score AS ai_score,
        score_add - mark_score_add - rule_score_add AS ai_score_add,
        length(mark_ids) AS mark_id_cnt
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = 'tb'
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
    -- 下拉框-子账号
    AND (
        '{{ snicks }}'=''
        OR
        snick IN splitByChar(',','{{ snicks }}')
    )
) AS dialog_info
GROUP BY platform, seller_nick, snick

-- 自定义质检-子账号维度自定义质检项消息触发次数
UNION ALL

SELECT
    platform,
    seller_nick,
    snick,
    xrule_stats_tag_id AS tag_id,
    sum(xrule_stats_tag_count) AS tag_cnt
FROM dwd.xdqc_dialog_all
ARRAY JOIN
    xrule_stats_id AS xrule_stats_tag_id,
    xrule_stats_count AS xrule_stats_tag_count
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND snick GLOBAL IN (
    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
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
-- 清除没有打标的数据, 减小计算量
AND xrule_stats_id!=[]
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
GROUP BY platform, seller_nick, snick, xrule_stats_tag_id

-- 自定义质检-子账号维度自定义质检项会话触发次数
UNION ALL

SELECT
    platform,
    seller_nick,
    snick,
    top_xrules_tag_id AS tag_id,
    sum(top_xrules_tag_count) AS tag_cnt
FROM dwd.xdqc_dialog_all
ARRAY JOIN
    top_xrules_id AS top_xrules_tag_id,
    top_xrules_count AS top_xrules_tag_count
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND snick GLOBAL IN (
    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
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
-- 清除没有打标的数据, 减小计算量
AND top_xrules_id!=[]
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
GROUP BY platform, seller_nick, snick, top_xrules_tag_id