-- 质检报表-客服-查看详情-人工质检结果
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
SELECT
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='wx' THEN '企微'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    seller_nick AS `店铺`,
    department_name AS `子账号分组`,
    snick AS `客服子账号`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,

    -- 人工质检结果
    arrayStringConcat(arrayMap(x->toString(x), manual_tag_names),'$$') AS `人工质检标签`,
    arrayStringConcat(arrayMap(x->toString(x), manual_tag_cnts),'$$') AS `人工质检触发次数`

FROM (
    --质检结果明细-子账号维度
    -- PS: 此处应该先进行预聚合, 减小中间结果的数组长度
    SELECT
        platform, seller_nick, snick,
        -- 人工质检
        groupArrayIf(
            tag_name, tag_type IN ['manual_subtract', 'manual_add']
        ) AS manual_tag_names,
        groupArrayIf(
            tag_cnt_sum, tag_type IN ['manual_subtract', 'manual_add']
        ) AS manual_tag_cnts
    FROM (
        SELECT
            platform, seller_nick, snick, tag_type, tag_id, tag_name,
            sum(tag_cnt_sum) AS tag_cnt_sum
        FROM xqc_dws.tag_stat_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start_=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end_=yesterday }}'))
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
            -- 获取最新版本的维度数据(T+1)
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids_ }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids_ }}')
            )
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks_ }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks_ }}')
        )
        -- 下拉框-子账号
        AND (
            '{{ snicks_ }}'=''
            OR
            snick IN splitByChar(',','{{ snicks_ }}')
        )
        GROUP BY platform, seller_nick, snick, tag_type, tag_id, tag_name
    ) AS dws_tag_stat
    GROUP BY platform, seller_nick, snick
) AS manual_check_info
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
AND department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY platform, seller_nick, department_name, snick, employee_name