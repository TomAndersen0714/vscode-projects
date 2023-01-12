-- 质检报表-下拉框-获取子账号分组
SELECT DISTINCT
    concat(department_name,'//',department_id) AS department_name_id
FROM (
    SELECT DISTINCT
        snick
    FROM xqc_dws.snick_stat_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
        ) AS snick_employee_info
        -- 下拉框-客服姓名
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
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
GLOBAL LEFT JOIN (
    -- 获取最新版本的维度数据(T+1)
    SELECT
        snick, department_id, department_name
    FROM (
        -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
        SELECT snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        SELECT
            _id AS department_id, full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS department_info
    USING (department_id)
) AS snick_department_map
USING(snick)
WHERE department_id!='' -- 清除匹配不上最新分组的子账号
ORDER BY department_name COLLATE 'zh'