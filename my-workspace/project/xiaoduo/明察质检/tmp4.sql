SELECT
    day,
    snick_dim.company_id AS company_id, snick_dim.shop_id AS shop_id,
    platform, seller_nick, snick,
    snick_dim.employee_id,
    snick_dim.employee_name,
    snick_dim.department_id,
    snick_dim.department_name,
    snick_dim.qc_norm_id,
    snick_dim.qc_norm_name,
    snick_dim.qc_norm_tag_cnt,
    snick_dim.qc_norm_ai_tag_cnt,
    snick_dim.qc_norm_custom_tag_cnt,
    snick_dim.qc_norm_manual_tag_cnt,
    snick_dim.qc_norm_alert_tag_cnt,
    dialog_cnt
FROM (
    SELECT
        toYYYYMMDD(begin_time) AS day,
        platform,
        seller_nick,
        snick,
        -- 会话量统计-总计
        uniqExact(_id) AS dialog_cnt
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20231006
    AND platform = 'jd'
    AND seller_nick = '九牧官方旗舰店'
    GROUP BY day, platform, seller_nick, snick
) AS dws_snick_stat
GLOBAL LEFT JOIN (
    -- 获取维度数据快照
    SELECT
        company_id, shop_id, platform, snick,
        employee_id, employee_name, department_id, department_name,
        qc_norm_id, qc_norm_name,
        qc_norm_tag_cnt, qc_norm_ai_tag_cnt, qc_norm_custom_tag_cnt, qc_norm_manual_tag_cnt, qc_norm_alert_tag_cnt
    FROM (
        SELECT
            company_id, shop_id, platform, snick,
            employee_id, employee_name, department_id, department_name,
            qc_norm_id, qc_norm_name
        FROM xqc_dim.snick_full_info_all
        WHERE day = 20231006
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            qc_norm_id,
            COUNT(1) AS qc_norm_tag_cnt,
            sum(rule_category = 1) AS qc_norm_ai_tag_cnt,
            sum(rule_category = 3) AS qc_norm_custom_tag_cnt,
            sum(rule_category = 2) AS qc_norm_manual_tag_cnt,
            sum(alert_level != 0) AS qc_norm_alert_tag_cnt
        FROM xqc_dim.qc_rule_all
        WHERE day = 20231006
        -- 仅开启的质检项
        AND status = 1
        -- 仅有效的质检项
        AND qc_norm_id != ''
        GROUP BY qc_norm_id
    ) AS qc_norm_cnt
    USING(qc_norm_id)
) AS snick_dim
USING(platform, snick)