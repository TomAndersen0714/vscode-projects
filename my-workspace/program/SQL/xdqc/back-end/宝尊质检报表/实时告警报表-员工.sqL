-- 实时告警报表-员工

-- 基础表
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
GLOBAL LEFT JOIN (
    SELECT
        _id AS employee_id,
        username AS employee_name,
        superior_name
    FROM ods.xinghuan_employee_all
    WHERE day=toYYYYMMDD(yesterday())
    AND company_id = '{{company_id=6131e6554524490001fc6825}}'
) AS employee_info
USING(employee_id)

-- 统计每日子账号基础指标
SELECT
    day,
    snick, 
    COUNT(1) AS dialog_cnt -- 会话量
FROM xqc_ods.dialog_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND shop_id IN (
    SELECT DISTINCT tenant_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
)
GROUP BY day, snick

SELECT
    day,
    snick,
    COUNT(1) AS alert_cnt, --告警总量
    COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
    SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
    SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
    SUM(level=1) AS level_1_alert_cnt -- 初级告警量
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start}}')) AND toYYYYMMDD(toDate('{{day.end}}'))
AND shop_id IN (
    SELECT DISTINCT tenant_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{company_id=6131e6554524490001fc6825}}'
)
GROUP BY day, snick

-- 基础指标上卷
SELECT
    snick,
    dialog_cnt, -- 会话量
    dialog_abnormal_cnt, -- 扣分会话量
    alert_cnt, --告警总量
    alert_dialog_cnt, -- 告警会话量
    level_3_alert_cnt, -- 高级告警量
    level_2_alert_cnt, -- 中级告警量
    level_1_alert_cnt, -- 初级告警量
    CONCAT(if(alert_cnt!=0, toString(round(level_3_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_3_alert_pct, -- 高级告警比例
    CONCAT(if(alert_cnt!=0, toString(round(level_2_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_2_alert_pct, -- 中级告警比例
    CONCAT(if(alert_cnt!=0, toString(round(level_1_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_1_alert_pct -- 初级告警比例
FROM (
    SELECT
        day,
        snick, 
        COUNT(1) AS dialog_cnt -- 会话量
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND shop_id IN (
        SELECT DISTINCT tenant_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    )
    GROUP BY day, snick
) AS dialog_info
GLOBAL FULL OUTER JOIN (
    SELECT
        day,
        snick,
        COUNT(1) AS alert_cnt, --告警总量
        COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
        SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
        SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
        SUM(level=1) AS level_1_alert_cnt -- 初级告警量
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start}}')) AND toYYYYMMDD(toDate('{{day.end}}'))
    AND shop_id IN (
        SELECT DISTINCT tenant_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{company_id=6131e6554524490001fc6825}}'
    )
    GROUP BY day, snick
) AS alert_info
USING(snick)

-- 链接子账号维度数据
SELECT *
FROM (
    -- 获取店铺-子账号-员工-主管映射信息
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
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id,
            username AS employee_name,
            superior_name
        FROM ods.xinghuan_employee_all
        WHERE day=toYYYYMMDD(yesterday())
        AND company_id = '{{company_id=6131e6554524490001fc6825}}'
    ) AS employee_info
    USING(employee_id)
) AS dim_info
GLOBAL FULL OUTER JOIN (
    SELECT
        snick,
        dialog_cnt, -- 会话量
        dialog_abnormal_cnt, -- 扣分会话量
        alert_cnt, --告警总量
        alert_dialog_cnt, -- 告警会话量
        level_3_alert_cnt, -- 高级告警量
        level_2_alert_cnt, -- 中级告警量
        level_1_alert_cnt, -- 初级告警量
        CONCAT(if(alert_cnt!=0, toString(round(level_3_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_3_alert_pct, -- 高级告警比例
        CONCAT(if(alert_cnt!=0, toString(round(level_2_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_2_alert_pct, -- 中级告警比例
        CONCAT(if(alert_cnt!=0, toString(round(level_1_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_1_alert_pct -- 初级告警比例
    FROM (
        SELECT
            day,
            snick, 
            COUNT(1) AS dialog_cnt -- 会话量
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND shop_id IN (
            SELECT DISTINCT tenant_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        )
        GROUP BY day, snick
    ) AS dialog_info
    GLOBAL FULL OUTER JOIN (
        SELECT
            day,
            snick,
            COUNT(1) AS alert_cnt, --告警总量
            COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
            SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
            SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
            SUM(level=1) AS level_1_alert_cnt -- 初级告警量
        FROM xqc_ods.alert_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start}}')) AND toYYYYMMDD(toDate('{{day.end}}'))
        AND shop_id IN (
            SELECT DISTINCT tenant_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{company_id=6131e6554524490001fc6825}}'
        )
        GROUP BY day, snick
    ) AS alert_info
    USING(snick)
) AS snick_stat
USING(snick)

-- 实时告警报表-员工
-- 链接组织架构维度数据, 天/BG/BU/店铺, 仅展示有店铺的维度数据
SELECT
    day AS `日期`, 
    bg_name AS `BG`, 
    bu_name AS `BU`, 
    -- shop_id,
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='open' THEN '开放平台'
        WHEN platform='~' THEN ''
        ELSE platform
    END AS `平台`,
    shop_name AS `店铺`,
    employee_name AS `客服`, 
    superior_name AS `主管`,
    SUM(dialog_cnt) AS `会话量`,
    SUM(alert_cnt) AS `告警总量`,
    SUM(alert_dialog_cnt) AS `告警会话量`,
    SUM(level_3_alert_cnt) AS `高级告警量`,
    SUM(level_2_alert_cnt) AS `中级告警量`,
    SUM(level_1_alert_cnt) AS `初级告警量`,
    CONCAT(if(`告警总量`!=0, toString(round(`高级告警量`/`告警总量`*100, 2)), '0'),'%') AS `高级告警比例`,
    CONCAT(if(`告警总量`!=0, toString(round(`中级告警量`/`告警总量`*100, 2)), '0'),'%') AS `中级告警比例`,
    CONCAT(if(`告警总量`!=0, toString(round(`初级告警量`/`告警总量`*100, 2)), '0'),'%') AS `初级告警比例`
FROM (
    SELECT
        day,
        bg_name, bu_name, platform, shop_id, shop_name
    FROM (
        SELECT
            bg_name, bu_name, platform, shop_id, shop_name
        FROM (
            SELECT DISTINCT
                bg_id, bu_id, bu_name, platform, shop_id, shop_name
            FROM (
                SELECT DISTINCT
                    parent_department_path[1] AS bg_id,
                    parent_department_path[2] AS bu_id,
                    platform,
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
    WHERE 
    -- 权限隔离-shop_id
    (
        '{{shop_id_list}}'=''
        OR
        shop_id IN splitByChar(',','{{shop_id_list}}')
    )
    ORDER BY day,bg_name,bu_name,shop_name
) AS bg_bu_info
GLOBAL LEFT JOIN (
    SELECT *
    FROM (
        -- 获取店铺-子账号-员工-主管映射信息
        SELECT
            shop_id, employee_name, superior_name, snick
        FROM (
            SELECT
                mp_shop_id AS shop_id,
                snick,
                employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day=toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id,
                username AS employee_name,
                superior_name
            FROM ods.xinghuan_employee_all
            WHERE day=toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        ) AS employee_info
        USING(employee_id)

    ) AS dim_info
    -- 仅展示发生了会话的子账号
    GLOBAL RIGHT JOIN (
        SELECT
            day,
            snick,
            dialog_cnt, -- 会话量
            alert_cnt, --告警总量
            alert_dialog_cnt, -- 告警会话量
            level_3_alert_cnt, -- 高级告警量
            level_2_alert_cnt, -- 中级告警量
            level_1_alert_cnt -- 初级告警量
        FROM (
            SELECT
                day,
                snick, 
                COUNT(1) AS dialog_cnt -- 会话量
            FROM xqc_ods.dialog_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND shop_id IN (
                SELECT DISTINCT tenant_id
                FROM xqc_dim.company_tenant
                WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
            )
            GROUP BY day, snick
        ) AS dialog_info
        GLOBAL FULL OUTER JOIN (
            SELECT
                day,
                snick,
                COUNT(1) AS alert_cnt, --告警总量
                COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
                SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
                SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
                SUM(level=1) AS level_1_alert_cnt -- 初级告警量
            FROM xqc_ods.alert_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND shop_id IN (
                SELECT DISTINCT tenant_id
                FROM xqc_dim.company_tenant
                WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
            )
            GROUP BY day, snick
        ) AS alert_info
        USING(day, snick)
        WHERE 
        -- 权限隔离-snick
        (
            '{{snick_list}}'=''
            OR
            snick IN splitByChar(',','{{snick_list}}')
        )
    ) AS snick_stat
    USING(snick)
) AS shop_snick_stat
USING(day, shop_id)
GROUP BY day, bg_name, bu_name, platform, shop_id, shop_name, employee_name, superior_name
ORDER BY day, bg_name, bu_name, platform, shop_name, employee_name, superior_name