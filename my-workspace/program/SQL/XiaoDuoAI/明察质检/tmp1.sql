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
        {ds_nodash} AS day,
        platform,
        seller_nick,
        snick,
        _id AS dialog_id,
        score,
        score_add
    FROM dwd.xdqc_dialog_all
    WHERE day = {ds_nodash}
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
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_binding_info
    GLOBAL INNER JOIN (
        SELECT
            department_id,
            platform,
            snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = {snapshot_ds_nodash}
    ) AS snick_info
    USING(department_id)
) AS snick_info
ON dialog_info.platform = snick_info.platform
AND dialog_info.snick = snick_info.snick