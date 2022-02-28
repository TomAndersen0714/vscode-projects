-- 自定义质检-平台维度扣分质检项触发次数统计
SELECT
    platform,
    rule_add_stats_tag_id AS tag_id,
    sum(rule_add_stats_tag_count) AS tag_cnt
FROM dwd.xdqc_dialog_all
ARRAY JOIN
    rule_add_stats_id AS rule_add_stats_tag_id,
    rule_add_stats_count AS rule_add_stats_tag_count
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND snick GLOBAL IN (
    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
    SELECT distinct snick
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    -- 下拉框-子账号分组
    AND (
        '{{ department_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ department_ids }}')
    )
)
-- 清除没有打标的数据, 减小计算量
AND rule_add_stats_id!=[]
GROUP BY platform, rule_add_stats_tag_id