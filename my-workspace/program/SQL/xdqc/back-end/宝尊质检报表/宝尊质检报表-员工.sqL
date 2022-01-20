-- 宝尊质检报表-员工

-- 基础表
dwd.xdqc_dialog_all
xqc_ods.dialog_all
xqc_ods.alert_all

-- 维度数据

-- 获取组织架构维度数据, 天/BG/BU/店铺, 仅展示有店铺的维度数据
SELECT
    day,
    bg_name, bu_name, shop_id, shop_name
FROM (
    SELECT
        bg_name, bu_name, shop_id, shop_name
    FROM (
        SELECT DISTINCT
            bg_id, bu_id, bu_name, shop_id, shop_name
        FROM (
            SELECT DISTINCT
                parent_department_path[1] AS bg_id,
                parent_department_path[2] AS bu_id,
                department_id AS shop_id,
                department_name AS shop_name
            FROM xqc_dim.group_all
            WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
            AND is_shop = 'True'
        ) AS shop_info
        GLOBAL LEFT JOIN (
            SELECT DISTINCT
                parent_department_path[1] AS bg_id,
                department_id AS bu_id,
                department_name AS bu_name
            FROM xqc_dim.group_all
            WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
            AND level = 2
            AND is_shop = 'False'
        ) AS bu_info
        USING(bg_id, bu_id)
        WHERE 
        -- 下拉框-BG
        (
            '{{ bg_ids }}'='' 
            OR
            bg_id IN splitByChar(',','{{ bg_ids }}')
        )
        -- 下拉框-BU
        AND (
            '{{ bu_ids }}'='' 
            OR 
            bu_id IN splitByChar(',','{{ bu_ids }}')
        )
        -- 下拉框-店铺
        AND (
            '{{ shop_ids }}'='' 
            OR 
            shop_id IN splitByChar(',','{{ shop_ids }}')
        )
    ) AS bg_bu_shop_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            department_id AS bg_id,
            department_name AS bg_name
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND level = 1
        AND is_shop = 'False'
    ) AS bg_info
    USING(bg_id)
) AS dim_info
GLOBAL CROSS JOIN (
    SELECT
    arrayJoin(
        arrayMap(x->toYYYYMMDD(toDate(x)),
        range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=yesterday }}') + 1), 1))
    ) AS day
) AS day
ORDER BY day,bg_name,bu_name,shop_name

-- 获取子账号-员工-主管映射信息
SELECT
    shop_id, employee_name, superior_name, snick
FROM (
    SELECT
        mp_shop_id AS shop_id,
        snick,
        employee_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day=toYYYYMMDD(yesterday())
    AND company_id = '{{company_id=6131e6554524490001fc6825}}'
) AS snick_info
GLOBAL RIGHT JOIN (
    SELECT
        _id AS employee_id,
        username AS employee_name,
        superior_name
    FROM ods.xinghuan_employee_all
    WHERE day=toYYYYMMDD(yesterday())
    AND company_id = '{{company_id=6131e6554524490001fc6825}}'
) AS employee_info
USING(employee_id)

-- 统计子账号基础指标
SELECT
    snick, 
    COUNT(1) AS dialog_cnt, -- 会话量
    SUM(score > 0) AS dialog_abnormal_cnt, -- 扣分会话量
    