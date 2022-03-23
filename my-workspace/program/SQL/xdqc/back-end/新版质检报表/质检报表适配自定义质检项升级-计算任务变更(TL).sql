-- ods.qc_session_count_all
-- 涉及功能模块: 质检总览-客服服务质量排行TOP10-自定义扣分
INSERT INTO ods.qc_session_count_all
SELECT toDate('{ds}') AS `date` ,
    a.platform, 
    a.company_id, 
    a.company_name, 
    a.department_id, 
    a.department_name, 
    a.employee_id, 
    a.employee_name,
    a.`group`,
    a.shop_name,
    a.snick,
    a.session_count,
    a.add_score_count,
    a.subtract_score_count,
    a.manual_qc_count,
    a.ai_abnormal_count,
    a.manual_abnormal_count,
    a.ai_add_score,
    a.manual_add_score,
    a.ai_subtract_score,
    a.manual_subtract_score,
    a.ai_add_score_count,
    a.manual_add_score_count,
    a.ai_subtract_score_count,
    a.manual_subtract_score_count,
    a.rule_score_count,
    a.rule_score,
    a.rule_add_score_count,
    a.rule_add_score,
    length(b.dialog_array),
    b.dialog_array
FROM (
    SELECT 
        session_info.`date`,
        session_info.platform AS platform,
        dim_info.company_id AS  company_id,
        '' AS company_name,
        dim_info.department_id AS department_id,
        dim_info.department_name AS department_name,
        dim_info.employee_id AS employee_id,
        dim_info.employee_name AS employee_name,
        session_info.group AS group,
        session_info.shop_name AS shop_name,
        session_info.snick AS snick,
        session_info.session_count AS session_count,
        session_info.add_score_count AS add_score_count,
        session_info.subtract_score_count AS subtract_score_count,
        session_info.manual_qc_count AS manual_qc_count,
        session_info.ai_abnormal_count AS ai_abnormal_count,
        session_info.manual_abnormal_count AS manual_abnormal_count,
        session_info.ai_add_score AS ai_add_score,
        session_info.manual_add_score AS manual_add_score,
        session_info.ai_subtract_score AS ai_subtract_score,
        session_info.manual_subtract_score AS manual_subtract_score,
        session_info.ai_add_score_count AS ai_add_score_count,
        session_info.manual_add_score_count AS manual_add_score_count,
        session_info.ai_subtract_score_count AS ai_subtract_score_count,
        session_info.manual_subtract_score_count AS manual_subtract_score_count,
        session_info.rule_score_count AS rule_score_count,
        session_info.rule_score AS rule_score,
        session_info.rule_add_score_count AS rule_add_score_count,
        session_info.rule_add_score AS rule_add_score
    FROM (
        SELECT `date`,
            platform,
            `group`,
            seller_nick AS shop_name ,
            snick,
            count(1) AS session_count,
            sum(if(score_add > 0 or mark_score_add > 0 or rule_add_score_info > 0,1,0)) AS add_score_count,
            sum(if(score > 0 or mark_score > 0 or rule_score_info > 0,1,0)) AS subtract_score_count,
            sum(if(length(mark_ids) != 0,1,0)) AS  manual_qc_count,
            sum(if(arraySum(abnormals_count)>0,1,0)) AS ai_abnormal_count,
            sum(if(length(tag_score_stats_id) > 0,1,0)) AS manual_abnormal_count,
            sum(score_add) AS ai_add_score,
            sum(mark_score_add) AS manual_add_score,
            sum(score) AS ai_subtract_score , 
            sum(mark_score) AS manual_subtract_score,
            sum(if(score_add > 0,1,0)) AS ai_add_score_count,
            sum(if(mark_score_add > 0,1,0)) AS manual_add_score_count,
            sum(if(score > 0,1,0)) AS ai_subtract_score_count , 
            sum(if(mark_score > 0,1,0)) AS manual_subtract_score_count,
            sum (if(rule_score_info>0,1,0 )) AS rule_score_count,
            sum (rule_score_info) AS rule_score,
            sum (if(rule_add_score_info>0,1,0 )) AS rule_add_score_count,
            sum (rule_add_score_info) AS rule_add_score
        FROM (
            SELECT `date`,
                platform,
                `group`,
                seller_nick,
                seller_nick AS shop_name ,
                snick,
                _id,
                score,
                score_add,
                mark_score,
                mark_score_add,
                mark_ids,
                abnormals_count,
                tag_score_stats_id,
                negate(arraySum(arrayFilter(x->x<0, xrule_stats_score))) AS xrule_score_info,
                arraySum(arrayFilter(x->x>0, xrule_stats_score)) AS xrule_add_score_info,
                arraySum(rule_stats_score) + xrule_score_info AS rule_score_info,
                arraySum(rule_add_stats_score) + xrule_add_score_info AS rule_add_score_info
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        ) AS rule_info 
        GROUP BY date, platform, seller_nick, group, snick
    ) AS session_info
    GLOBAL LEFT JOIN (
        SELECT a.company_id AS company_id,
            b.platform,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
            SELECT * 
            FROM ods.xinghuan_department_all
            WHERE day = {ds_nodash}
        ) AS a 
        GLOBAL RIGHT JOIN (
            SELECT a._id AS employee_id,
                b.platform,
                b.department_id AS department_id,
                a.username AS employee_name,
                b.snick AS snick
            FROM (
                SELECT * 
                FROM ods.xinghuan_employee_all
                WHERE  day = {ds_nodash} 
            ) AS a 
            GLOBAL RIGHT JOIN ( 
                SELECT * 
                FROM ods.xinghuan_employee_snick_all
                WHERE day = {ds_nodash} 
                AND platform ='tb'
            ) AS b 
            ON a._id = b.employee_id
        ) AS b 
        ON a._id = b.department_id
        ) AS dim_info
    ON session_info.platform = dim_info.platform
    AND session_info.snick = dim_info.snick 
) AS a 
GLOBAL LEFT JOIN (
    SELECT
        day,
        platform,
        shop_name,
        snick,
        groupArray(dialog_id) AS dialog_array
    FROM ods.xinghuan_qc_abnormal_all 
    WHERE row_number < 4 
    AND day = {ds_nodash}
    GROUP BY day, platform, shop_name, snick
) AS b 
ON a.date = b.day
AND a.platform = b.platform
AND a.shop_name =b.shop_name
AND a.snick = b.snick
