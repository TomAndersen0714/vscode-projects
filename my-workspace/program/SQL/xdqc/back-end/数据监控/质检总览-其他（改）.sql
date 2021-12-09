-- ods.qc_session_count_all
select
    a.*,
    b.json_list as json_list
from (
        SELECT platform,
            sum(session_count) as total_count, -- 会话总量
            sum(subtract_score_count) as abnormal_count,-- 扣分会话
            sum(subtract_score_count) / sum(session_count) as abnormal_rate, -- 扣分会话占比
            sum(ai_subtract_score_count) as ai_abnormal_cnt, -- AI质检异常会话量
            sum(manual_qc_count) as human_check_count, -- 人工抽检量
            toInt32(
                round(
                    (0.9604 * sum(session_count)) /(0.0025 * sum(session_count) + 0.9604),
                    0
                )
            ) as suggestion_check_count, -- 建议抽检量
            round(
                (
                    sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
                ) / sum(session_count),
                2
            ) AS avg_score, -- 质检平均分
            round((sum(manual_qc_count) / sum(session_count)), 4) as check_rate, -- 抽检比例
            sum(manual_subtract_score_count) as human_abnormal_count, -- 人工质检扣分量
            length(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_count, -- 高异常情绪案例数量
            toString(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_dialog_id -- 高异常情绪案例
        FROM ods.qc_session_count_all
        WHERE date between %d and %d -- startDate, endDate, shopStr
            and shop_name in %s
            -- and department_id != ''
        group by platform
    ) as a
    left join (
        select platform,
            toString(groupArray(json_info)) as json_list
        from ( -- 服务情况趋势
                SELECT platform,
                    concat(
                        '{"day":', -- 日期
                        toString(toInt64(date)),
                        ',"subtract_score_proportion":', -- 总扣分
                        toString((sum(subtract_score_count) / sum(session_count))),
                        ',"manual_subtract_score_proportion":', -- 人工质检扣分占比
                        toString(
                            (
                                sum(manual_subtract_score_count) / sum(session_count)
                            )
                        ),
                        ',"ai_subtract_score_proportion":', -- AI质检扣分占比
                        toString(
                            (
                                sum(ai_subtract_score_count) / sum(session_count)
                            )
                        ),
                        '}'
                    ) as json_info
                FROM ods.qc_session_count_all
                WHERE date between %d and %d -- startDate, endDate, shopStr
                    and shop_name in %s
                group by date,
                    platform
                order by date
            ) json
        group by platform
    ) as b 
    on a.platform = b.platform