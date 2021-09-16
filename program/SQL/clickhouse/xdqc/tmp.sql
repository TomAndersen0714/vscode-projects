select a.*,
    b.json_list as json_list
from (
        SELECT platform,
            sum(session_count) as total_count,
            sum(subtract_score_count) as abnormal_count,
            sum(subtract_score_count) / sum(session_count) as abnormal_rate,
            sum(ai_subtract_score_count) as ai_abnormal_cnt,
            sum(manual_qc_count) as human_check_count,
            toInt32(
                round(
                    (0.9604 * sum(session_count)) /(0.0025 * sum(session_count) + 0.9604),
                    0
                )
            ) as suggestion_check_count,
            round(
                (
                    sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
                ) / sum(session_count),
                2
            ) AS avg_score,
            round((sum(manual_qc_count) / sum(session_count)), 4) as check_rate,
            sum(manual_subtract_score_count) as human_abnormal_count,
            length(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_count,
            toString(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_dialog_id
        FROM ods.qc_session_count_all
        WHERE date between 1630425600 and 1631030399
            and shop_name in ('方太官方旗舰店')
            and department_id != ''
        group by platform
    ) as a
    left join (
        select platform,
            toString(groupArray(json_info)) as json_list
        from (
                SELECT platform,
                    concat(
                        '{\"day\":',
                        toString(toInt64(date)),
                        ',\"subtract_score_proportion\":',
                        toString((sum(subtract_score_count) / sum(session_count))),
                        ',\"manual_subtract_score_proportion\":',
                        toString(
                            (
                                sum(manual_subtract_score_count) / sum(session_count)
                            )
                        ),
                        ',\"ai_subtract_score_proportion\":',
                        toString(
                            (
                                sum(ai_subtract_score_count) / sum(session_count)
                            )
                        ),
                        '}'
                    ) as json_info
                FROM ods.qc_session_count_all
                WHERE date between 1630425600 and 1631030399
                    and shop_name in ('方太官方旗舰店')
                group by date,
                    platform
                order by date
            ) json
        group by platform
    ) as b on a.platform = b.platform


select 'server' as type,
    employee_id,
    employee_name,
    sum(session_count) as total_count,
    0 as total_check,
    sum(ai_subtract_score) as abnormal_score,
    sum(subtract_score_count) / sum(session_count) as abnormal_rate,
    sum(ai_subtract_score) - sum(manual_subtract_score) - sum(rule_score) as ai_abnormal_score,
    sum(manual_subtract_score) as human_abnormal_score,
    0 as human_total_check,
    0 as average_check,
    sum(rule_score) AS user_rule_score,
    round(
        (
            sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
        ) / sum(session_count),
        2
    ) AS avg_score
from ods.qc_session_count_all
where date >= 1630425600
    and date < 1631030400
    and shop_name in ('方太官方旗舰店')
    and employee_name != ''
group by employee_id,
    employee_name
order by avg_score desc
limit 10
union all
select 'server_read_mark' as type,
    employee_id,
    employee_name,
    sum(session_count) as total_count,
    0 as total_check,
    0 as abnormal_score,
    0 as abnormal_rate,
    0 as ai_abnormal_score,
    0 as human_abnormal_score,
    sum(manual_qc_count) as human_total_check,
    sum(manual_qc_count) / if(
        dateDiff('day', toDate(1630425600), toDate(1631030400)) = 0,
        1,
        dateDiff('day', toDate(1630425600), toDate(1631030400))
    ) as average_check,
    0 as user_rule_score,
    0 as avg_score
from ods.qc_session_count_all
where date >= 1630425600
    and date < 1631030400
    and shop_name in ('方太官方旗舰店')
    and employee_name != ''
    and manual_qc_count != 0
group by employee_id,
    employee_name
order by human_total_check desc
limit 10
union all
select 'read_mark' as type,
    account_id as employee_id,
    username as employee_name,
    0 as total_count,
    count(1) as total_check,
    0 as abnormal_score,
    0 as abnormal_rate,
    0 as ai_abnormal_count,
    0 as human_abnormal_count,
    0 as human_total_check,
    count(1) / if(
        dateDiff('day', toDate(1630425600), toDate(1631030400)) = 0,
        1,
        dateDiff('day', toDate(1630425600), toDate(1631030400))
    ) as average_check,
    0 as user_rule_score,
    0 as avg_score
from ods.qc_read_mark_detail_all
where username != ''
    and date >= 1630425600
    and date < 1631030400
    and shop_name in ('方太官方旗舰店')
    and employee_name != ''
group by account_id,
    username
order by total_check desc
limit 10

select a.platform as platform,
    type,
    b.qc_id as qc_id,
    b.qc_name as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
            and `type` in ('ai', 's_emotion', 'c_emotion')
        group by platform
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and `type` in ('ai', 's_emotion', 'c_emotion')
            and shop_name in ('方太官方旗舰店')
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
    ) as b on a.platform = b.platform
UNION ALL
SELECT a.platform AS platform,
    's_emotion' AS `type`,
    b.qc_id AS qc_id,
    b.qc_name AS qc_name,
    round(b.count_info / a.count_all_info, 4) AS qc_proportion
FROM (
        SELECT platform,
            sum(qc_count) AS count_all_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
            AND `type` = 's_emotion'
        GROUP BY platform,
            `type`
    ) AS a
    LEFT JOIN (
        SELECT platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) AS count_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
            AND `type` = 's_emotion'
        GROUP BY platform,
            `type`,
            qc_id,
            qc_name
    ) AS b ON a.platform = b.platform
union all
select a.platform as platform,
    'manual' as `type`,
    b.qc_id as qc_id,
    b.qc_name_all as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
            and `type` = 'manual'
        group by platform,
            `type`
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            replaceAll(replaceAll(qc_name, '未设置一级标签/', ''), '未设置二级标签/', '') as qc_name_all,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and `type` = 'manual'
            and shop_name in ('方太官方旗舰店')
        group by platform,
            `type`,
            qc_id,
            qc_name
        limit 10
    ) as b on a.platform = b.platform
order by qc_proportion desc
union all
select b.platform as platform,
    'qc_word' as `type`,
    '' as qc_id,
    a.word as qc_name,
    round((a.words_count_info / b.words_count_all), 4) as qc_proportion
from (
        select platform,
            word,
            sum(words_count) as words_count_info
        from ods.qc_words_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
        group by platform,
            word
        order by words_count_info desc
    ) a
    left join (
        select platform,
            sum(words_count) as words_count_all
        from ods.qc_words_detail_all
        WHERE date >= 1630425600
            and date < 1631030399
            and shop_name in ('方太官方旗舰店')
        group by platform
        order by words_count_all
    ) b on a.platform = b.platform
limit 10

SELECT a.company_id AS company_id,
    a.name AS name,
    sum (b.label_count) AS label_count
FROM (
        with (
            select max(date)
            FROM ods.qc_case_label_detail_all
            WHERE company_id = '5f747ba42c90fd0001254404'
        ) as max_date
        SELECT DISTINCT `date`,
            company_id,
            concat(
                parent_label_name,
                if(
                    label_name = '',
                    label_name,
                    concat('/', label_name)
                )
            ) AS name
        FROM ods.qc_case_label_detail_all
        WHERE company_id = '5f747ba42c90fd0001254404'
            and date = max_date
    ) AS a
    LEFT JOIN (
        with (
            select max(date)
            FROM ods.qc_case_label_detail_all
            WHERE company_id = '5f747ba42c90fd0001254404'
        ) as max_date
        SELECT `date`,
            company_id,
            concat(
                parent_label_name,
                if(
                    label_name = '',
                    label_name,
                    concat('/', label_name)
                )
            ) AS name,
            sum(IF (dialog_id = '', 0, 1)) AS label_count
        FROM ods.qc_case_label_detail_all
        WHERE shop_name IN ('方太官方旗舰店')
            and date = max_date
        GROUP BY `date`,
            company_id,
            name
    ) AS b ON a.company_id = b.company_id
    AND a.name = b.name
    and a.date = b.date
GROUP by company_id,
    name