-- 新实时告警-店铺告警-实时告警项
WITH (
    -- 告警总量
    SELECT
        COUNT(1)
    FROM xqc_ods.alert_all FINAL
    PREWHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) 
    AND toYYYYMMDD(toDate('{{ day.end }}'))
    AND seller_nick = '华硕京东自营官方旗舰店'
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
    level_type_alert_cnt AS `告警总量`,
    COUNT(DISTINCT dialog_id) AS `告警会话量`,
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
    ) AS `告警完结率`
FROM (
    SELECT *
    FROM xqc_ods.alert_all FINAL
    PREWHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) 
    AND toYYYYMMDD(toDate('{{ day.end }}'))
    AND seller_nick = '华硕京东自营官方旗舰店'
    -- 下拉框-告警项
    AND (
        '{{ warning_types }}' = ''
        OR
        warning_type IN splitByChar(',','{{ warning_types }}')
    )
) AS alert_info
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type ASC