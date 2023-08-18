SELECT
    _id,
    create_time, update_time,
    qc_norm_group_info.company_id, qc_norm_group_info.platform,
    create_account, update_account,
    qc_norm_id , qc_norm_name, qc_norm_group_id, qc_norm_group_name, qc_norm_group_full_name,
    template_id, name, seller_nick, rule_category, rule_type, settings, check, check_target,
    alert_level, notify_way, notify_target, score, threshold, special_settings, status
FROM (
    SELECT
        _id,
        create_time, update_time,
        company_id, platform,
        create_account, update_account,
        qc_norm_id, qc_norm_group_id, 
        template_id, name, seller_nick, rule_category, rule_type, settings, check, check_target,
        alert_level, notify_way, notify_target, score, threshold, special_settings, status
    FROM xqc_dim.qc_rule_all
    WHERE day = {snapshot_ds_nodash}
) AS qc_rule_info
GLOBAL LEFT JOIN (
    SELECT
        company_id,
        platform,
        qc_norm_id,
        qc_norm_name,
        qc_norm_group_id,
        qc_norm_group_name,
        qc_norm_group_full_name
    FROM (
        SELECT
            qc_norm_id,
            _id AS qc_norm_group_id,
            name AS qc_norm_group_name,
            full_name AS qc_norm_group_full_name
        FROM xqc_dim.qc_norm_group_full_all
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_group_info
    GLOBAL LEFT JOIN (
        SELECT
            company_id,
            platform,
            _id AS qc_norm_id,
            name AS qc_norm_name
        FROM ods.xinghuan_qc_norm_all
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_info
    USING(qc_norm_id)
) AS qc_norm_group_info
USING(qc_norm_id, qc_norm_group_id)