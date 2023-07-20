SELECT
    day AS `日期`,
    seller_nick AS `店铺主账号`,
    snick AS `子账号`,
    alert_cnt AS `告警总量`,
    level_1_cnt AS `初级告警总量`,
    level_2_cnt AS `中级告警总量`,
    level_3_cnt AS `高级告警总量`,
    level_1_finished_alert_cnt AS `初级未完结告警总量`,
    level_2_finished_alert_cnt AS `中级未完结告警总量`,
    level_3_finished_alert_cnt AS `高级未完结告警总量`,
    finished_alert_cnt AS `已处理告警总量`,
    unfinished_alert_cnt AS `未处理告警总量`,
    alert_dialog_cnt AS `告警会话总量`,
    level_1_alert_dialog_cnt AS ``,
    level_2_alert_dialog_cnt AS ``,
    level_3_alert_dialog_cnt AS ``
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        COUNT(1) AS alert_cnt,
        SUM(level=1) AS level_1_cnt,
        SUM(level=2) AS level_2_cnt,
        SUM(level=3) AS level_3_cnt,
        SUM(level=1 AND is_finished = 'True') AS level_1_finished_alert_cnt,
        SUM(level=2 AND is_finished = 'True') AS level_2_finished_alert_cnt,
        SUM(level=3 AND is_finished = 'True') AS level_3_finished_alert_cnt,

        level_1_finished_alert_cnt + level_2_finished_alert_cnt + level_3_finished_alert_cnt AS finished_alert_cnt,
        alert_cnt - finished_alert_cnt AS unfinished_alert_cnt,

        uniqExact(dialog_id) AS alert_dialog_cnt,
        uniqExactIf(dialog_id, level=1) AS level_1_alert_dialog_cnt,
        uniqExactIf(dialog_id, level=2) AS level_2_alert_dialog_cnt,
        uniqExactIf(dialog_id, level=3) AS level_3_alert_dialog_cnt
    FROM xqc_ods.alert_all FINAL
    PREWHERE day = toYYYYMMDD(toDate('{{day}}'))
    GROUP BY 
        day, platform, seller_nick, snick
)
GLOBAL LEFT JOIN (
    
)
ORDER BY day, platform, seller_nick, snick COLLATE 'zh'