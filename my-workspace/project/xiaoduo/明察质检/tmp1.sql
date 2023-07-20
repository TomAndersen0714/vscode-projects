SELECT
    day AS `日期`,
    company_id AS `企业ID`,
    company_name AS `企业名称`,
    company_short_name AS `企业简称`,
    shop_id AS `店铺ID`,
    shop_name AS `店铺名`,
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
    unfinished_alert_dialog_cnt AS `未处理完成会话总量`,
    finished_alert_dialog_cnt AS `已处理完成会话总量`,
    level_1_alert_dialog_cnt AS `初级告警会话总量`,
    level_2_alert_dialog_cnt AS `中级告警会话总量`,
    level_3_alert_dialog_cnt AS `高级告警会话总量`
FROM (
    SELECT
        day,
        platform,
        shop_id,
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
        uniqExactIf(dialog_id, is_finished = 'False') AS unfinished_alert_dialog_cnt,
        alert_dialog_cnt - unfinished_alert_dialog_cnt AS finished_alert_dialog_cnt,
        uniqExactIf(dialog_id, level=1) AS level_1_alert_dialog_cnt,
        uniqExactIf(dialog_id, level=2) AS level_2_alert_dialog_cnt,
        uniqExactIf(dialog_id, level=3) AS level_3_alert_dialog_cnt
    FROM xqc_ods.alert_all FINAL
    PREWHERE day = toYYYYMMDD(toDate('{{day}}'))
    GROUP BY 
        day, platform, shop_id, seller_nick, snick
)
GLOBAL LEFT JOIN (
    SELECT
        company_id, company_name, company_short_name,
        platform, shop_id, shop_name, seller_nick
    FROM (
        SELECT
            _id AS company_id,
            name AS company_name,
            shot_name AS company_short_name
        FROM xqc_dim.company_latest_all
    ) AS company_info
    GLOBAL LEFT JOIN (
        SELECT
            company_id, platform,
            shop_id, plat_shop_name AS shop_name, seller_nick
        FROM xqc_dim.shop_latest_all
    ) AS shop_info
    USING(company_id)
    WHERE shop_id != ''
) AS company_shop_info
USING(platform, shop_id)
ORDER BY day, platform, seller_nick, snick COLLATE 'zh' 