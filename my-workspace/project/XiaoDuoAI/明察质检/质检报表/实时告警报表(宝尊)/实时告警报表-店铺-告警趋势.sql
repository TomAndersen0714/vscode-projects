-- 实时告警报表-店铺-告警趋势
SELECT
    day, 
    CASE
        WHEN type=1 THEN '告警总量'
        WHEN type=2 THEN '告警提醒总量'
        WHEN type=3 THEN '告警完结总量'
        ELSE ''
    END AS `cnt_type`,
    SUM(cnt) AS cnt
FROM (
    SELECT DISTINCT
        day, shop_id, type
    FROM (
        -- 维度数据
        -- 获取组织架构维度数据, 天/BG/BU/店铺, 仅展示有店铺的维度数据
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
                -- 权限隔离-shop_id
                (
                    '{{ shop_id_list }}'=''
                    OR
                    shop_id IN splitByChar(',','{{ shop_id_list }}')
                )
                -- 下拉框-BG
                AND (
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
    ) AS dim_info
    GLOBAL CROSS JOIN (
        SELECT arrayJoin([1,2,3]) AS type
    ) AS type_info

) AS dim_info
GLOBAL LEFT JOIN (
    -- 告警量
    SELECT day, shop_id, 1 AS `type`, COUNT(DISTINCT id) AS cnt
    FROM xqc_ods.alert_all FIANL
    WHERE shop_id IN (
        -- 已订阅店铺
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    )
    AND day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    GROUP BY day, shop_id

    UNION ALL

    -- 告警自动发送提醒量
    SELECT day, shop_id, 2 AS `type`, COUNT(DISTINCT id)  AS cnt
    FROM xqc_ods.alert_remind_all
    WHERE shop_id IN (
        -- 已订阅店铺
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    )
    AND day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND source = 1 -- 自动触发
    AND notify_type = 1 -- 实时触发
    GROUP BY day, shop_id

    UNION ALL

    -- 告警完结量
    SELECT day, shop_id, 3 AS `type`, SUM(is_finished = 'True') AS cnt
    FROM xqc_ods.alert_all FIANL
    WHERE shop_id IN (
        -- 已订阅店铺
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    )
    AND day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    GROUP BY day, shop_id
) AS stat_info
USING(day, shop_id, type)
GROUP BY day, type
HAVING cnt_type!=''