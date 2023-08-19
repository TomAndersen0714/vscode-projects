SELECT
    day,
    company_id, shop_id, platform, seller_nick, snick,
    employee_id, employee_name, department_id, department_name,
    qc_norm_id, qc_norm_name, 
    qc_norm_tag_cnt, qc_norm_ai_tag_cnt, qc_norm_custom_tag_cnt, qc_norm_manual_tag_cnt, qc_norm_alert_tag_cnt,
    dialog_tag_cnt, dialog_ai_tag_cnt, dialog_custom_tag_cnt, dialog_manual_tag_cnt,
    subtract_score_sum, add_score_sum, ai_subtract_score_sum, ai_add_score_sum, 
    custom_subtract_score_sum, custom_add_score_sum, manual_subtract_score_sum, manual_add_score_sum,
    dialog_score_avg, dialog_cnt, excellent_dialog_cnt,
    tagged_dialog_cnt, ai_tagged_dialog_cnt, custom_tagged_dialog_cnt, manual_tagged_dialog_cnt,
    subtract_score_dialog_cnt, add_score_dialog_cnt, manual_marked_dialog_cnt,
    ai_subtract_score_dialog_cnt, ai_add_score_dialog_cnt, ai_zero_score_tagged_dialog_cnt,
    custom_subtract_score_dialog_cnt, custom_add_score_dialog_cnt, custom_zero_score_tagged_dialog_cnt,
    manual_subtract_score_dialog_cnt, manual_add_score_dialog_cnt, manual_zero_score_tagged_dialog_cnt,
    eval_dialog_cnt, eval_level_1_dialog_cnt, eval_level_2_dialog_cnt, eval_level_3_dialog_cnt, eval_level_4_dialog_cnt, eval_level_5_dialog_cnt
FROM (
    SELECT
        day,
        company_id, shop_id, platform, seller_nick, snick,
        employee_id, employee_name, department_id, department_name,
        qc_norm_id, qc_norm_name, 
        qc_norm_tag_cnt, qc_norm_ai_tag_cnt, qc_norm_custom_tag_cnt, qc_norm_manual_tag_cnt, qc_norm_alert_tag_cnt,
        dialog_tag_cnt, dialog_ai_tag_cnt, dialog_custom_tag_cnt, dialog_manual_tag_cnt,
        subtract_score_sum, add_score_sum, ai_subtract_score_sum, ai_add_score_sum, 
        custom_subtract_score_sum, custom_add_score_sum, manual_subtract_score_sum, manual_add_score_sum,
        dialog_score_avg, dialog_cnt, excellent_dialog_cnt,
        tagged_dialog_cnt, ai_tagged_dialog_cnt, custom_tagged_dialog_cnt, manual_tagged_dialog_cnt,
        subtract_score_dialog_cnt, add_score_dialog_cnt, manual_marked_dialog_cnt,
        ai_subtract_score_dialog_cnt, ai_add_score_dialog_cnt, ai_zero_score_tagged_dialog_cnt,
        custom_subtract_score_dialog_cnt, custom_add_score_dialog_cnt, custom_zero_score_tagged_dialog_cnt,
        manual_subtract_score_dialog_cnt, manual_add_score_dialog_cnt, manual_zero_score_tagged_dialog_cnt
    FROM xqc_dws.snick_stat_all
    WHERE day = {ds_nodash}
    AND platform = '{platform}'
)
GLOBAL LEFT JOIN (
    SELECT
        day,
        platform, seller_nick, snick,
        COUNT(DISTINCT dialog_id) AS eval_dialog_cnt,
        uniqExactIf(dialog_id, eval_code = 0) AS eval_level_1_dialog_cnt,
        uniqExactIf(dialog_id, eval_code = 1) AS eval_level_2_dialog_cnt,
        uniqExactIf(dialog_id, eval_code = 2) AS eval_level_3_dialog_cnt,
        uniqExactIf(dialog_id, eval_code = 3) AS eval_level_4_dialog_cnt,
        uniqExactIf(dialog_id, eval_code = 4) AS eval_level_5_dialog_cnt
    FROM xqc_ods.dialog_eval_all
    WHERE day = {ds_nodash}
    AND platform = '{platform}'
    GROUP BY day, platform, seller_nick, snick
) AS dialog_eval_info
USING (day, platform, seller_nick, snick)