SELECT
    day,
    platform,
    seller_nick,
    snick_info.qc_norm_id,
    snick_info.department_id,
    snick,
    dialog_id,
    score,
    score_add
FROM (
    SELECT
        20220813 AS day,
        platform,
        seller_nick,
        snick,
        _id AS dialog_id,
        score,
        score_add
    FROM dwd.xdqc_dialog_all
    WHERE day = 20220813
    AND seller_nick = '方太官方旗舰店'
    LIMIT 1000
) AS dialog_info
GLOBAL INNER JOIN (
    SELECT
        qc_norm_id,
        department_id,
        platform,
        snick
    FROM (
        SELECT
            qc_norm_id,
            department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = 20220813
    ) AS qc_norm_binding_info
    GLOBAL INNER JOIN (
        SELECT DISTINCT
            department_id,
            platform,
            snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = 20220813
        AND company_id = '5f747ba42c90fd0001254404'
    ) AS snick_info
    USING(department_id)
) AS snick_info
ON dialog_info.platform = snick_info.platform
AND dialog_info.snick = snick_info.snick