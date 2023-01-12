-- 新实时告警-店铺告警-实时告警项
WITH (
    -- 告警总量
    SELECT COUNT(DISTINCT id)
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) AND toYYYYMMDD(toDate('{{ day.end=today }}'))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = '{{ platform=tb }}'
        )
        -- 权限隔离
        AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5,5e7dbfa6e4f3320016e9b7d1 }}')
            OR
            snick IN splitByChar(',', '{{ snick_list=null }}')
        )
        -- 下拉框-平台
        AND platform = '{{ platform=tb }}'
        -- 下拉框-店铺
        AND (
            '{{ shop_ids }}' = ''
            OR
            shop_id IN splitByChar(',','{{ shop_ids }}')
        )
        -- 下拉框-告警等级
        AND (
            '{{ levels }}' = ''
            OR
            toString(level) IN splitByChar(',','{{ levels }}')
        )
        -- 下拉框-告警项
        AND (
            '{{ warning_types }}' = ''
            OR
            warning_type IN splitByChar(',','{{ warning_types }}')
        )
) AS all_alert_sum
SELECT
    `level`,
    warning_type as `告警项`,
    sum(1) AS level_type_alert_cnt,
    level_type_alert_cnt AS `告警量`,
    CONCAT(
        toString(
            if(
                all_alert_sum != 0,
                round(level_type_alert_cnt / all_alert_sum * 100, 2),
                0.00
            )
        ),
        '%'
    ) AS `告警占比`,
    sum(is_finished = 'False') AS not_finished_level_type_alert_cnt,
    not_finished_level_type_alert_cnt AS `未处理量`,
    CONCAT(
        toString(
            if(
                level_type_alert_cnt != 0,
                round((level_type_alert_cnt - not_finished_level_type_alert_cnt) / level_type_alert_cnt * 100,2),
                0.00
            )
        ),
        '%'
    ) AS `完结率`
FROM (
    SELECT *
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) AND toYYYYMMDD(toDate('{{ day.end=today }}'))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                AND platform = '{{ platform=tb }}'
        )
        -- 权限隔离
        AND (
            shop_id IN splitByChar(
                ',',
                '{{ shop_id_list=5bfe7a6a89bc4612f16586a5,5e7dbfa6e4f3320016e9b7d1 }}'
            )
            OR snick IN splitByChar(',', '{{ snick_list=null }}')
        )
        -- 下拉框-平台
        AND platform = '{{ platform=tb }}'
        -- 下拉框-店铺
        AND (
            '{{ shop_ids }}' = ''
            OR
            shop_id IN splitByChar(',','{{ shop_ids }}')
        )
        -- 下拉框-告警等级
        AND (
            '{{ levels }}' = ''
            OR
            toString(level) IN splitByChar(',','{{ levels }}')
        )
        -- 下拉框-告警项
        AND (
            '{{ warning_types }}' = ''
            OR
            warning_type IN splitByChar(',','{{ warning_types }}')
        )
    ORDER BY update_time DESC
    LIMIT 1 BY id
) AS alert_info
GROUP BY `level`,
    warning_type
ORDER BY `level` DESC,
    warning_type ASC