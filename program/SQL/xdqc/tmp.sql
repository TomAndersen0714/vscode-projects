SELECT
    day,
    '{{ platform }}' AS platform,
    seller_nick,
    shop_id,
    COUNT(1) AS qc_dialog_cnt
FROM (
    SELECT
        toInt32(toYYYYMMDD(begin_time)) AS day,
        seller_nick,
        snick,
        _id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN {{start_day}} AND {{end_day}}
    AND platform = '{{ platform }}'
    -- 过滤关联了质检标准的店铺
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND platform = '{{ platform }}'
    )
    -- 过滤关联了质检标注的子账号
    AND snick GLOBAL IN (
        -- 查询所有关联了质检标准的子账号分组下的子账号
        SELECT DISTINCT snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND platform = '{{ platform }}'
        AND department_id GLOBAL IN (
            -- 查询关联了质检标准的子账号分组ID
            SELECT DISTINCT department_id
            FROM ods.xinghuan_qc_norm_relate_all
            WHERE day BETWEEN {{start_day}} AND {{end_day}}
            AND platform = '{{ platform }}'
        )
    )
) AS dialog_info
GLOBAL LEFT JOIN (
    -- 查询所有关联了质检标准的子账号分组下的子账号
    SELECT DISTINCT
        snick,
        mp_shop_id AS shop_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day BETWEEN {{start_day}} AND {{end_day}}
    AND platform = '{{ platform }}'
    AND department_id GLOBAL IN (
        -- 查询关联了质检标准的子账号分组ID
        SELECT DISTINCT department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND platform = '{{ platform }}'
    )
) AS snick_shop_id
USING snick
GROUP BY day, seller_nick, shop_id
ORDER BY day, seller_nick