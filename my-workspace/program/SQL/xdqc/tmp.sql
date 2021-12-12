INSERT INTO xqc_ods.qc_dialog_cnt_all
SELECT
    day,
    'tb' AS platform,
    shop_id,
    seller_nick,
    COUNT(1) AS qc_dialog_cnt
FROM (
    SELECT
        toInt32(toYYYYMMDD(begin_time)) AS day,
        seller_nick,
        snick,
        _id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20210901
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = 20210901
        AND platform = 'tb'
    )
    AND snick GLOBAL IN (
        SELECT DISTINCT snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = 20210901
        AND platform = 'tb'
        AND department_id GLOBAL IN (
            SELECT DISTINCT department_id
            FROM ods.xinghuan_qc_norm_relate_all
            WHERE day = 20210901
            AND platform = 'tb'
        )
    )
) AS dialog_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        snick,
        mp_shop_id AS shop_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = 20210901
    AND platform = 'tb'
    AND department_id GLOBAL IN (
        SELECT DISTINCT department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = 20210901
        AND platform = 'tb'
    )
) AS snick_shop_id
USING snick
GROUP BY day, seller_nick, shop_id