SELECT
    day,
    ifNull(company_id, '') AS company_id,
    ifNull(platform, '') AS platform,
    ifNull(shop_id, '') AS shop_id,
    ifNull(seller_nick, '') AS seller_nick,
    ifNull(snick, '') AS snick,
    ifNull(employee_name, '') AS employee_name,
    ifNull(department_id, '') AS department_id,
    ifNull(department_name, '') AS department_name,
    subtract_score_sum,
    add_score_sum,
    ai_subtract_score_sum,
    ai_add_score_sum,
    custom_subtract_score_sum,
    custom_add_score_sum,
    manual_subtract_score_sum,
    manual_add_score_sum,
    dialog_cnt
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        employee_name,
        department_id,
        department_name,
        subtract_score_sum,
        add_score_sum,
        ai_subtract_score_sum,
        ai_add_score_sum,
        custom_subtract_score_sum,
        custom_add_score_sum,
        manual_subtract_score_sum,
        manual_add_score_sum,
        dialog_cnt
    FROM xqc_dws.snick_stat_all
    WHERE day BETWEEN 20230721 AND 20230727
    AND platform = '{{platform}}'
    AND seller_nick = '{{seller_nick}}'
) AS stat_info
LEFT JOIN (
    SELECT
        company_id,
        platform,
        shop_id,
        seller_nick
    FROM xqc_dim.shop_latest_all
    WHERE platform = '{{platform}}'
    AND seller_nick = '{{seller_nick}}'
) AS shop_info
USING(platform, seller_nick)