-- 历史会话总量查询
select
    count(1) as count -- 会话总量
from dwd.xdqc_dialog_all
where seller_nick in %s -- param.SellerNicks
    and snick in %s -- param.SNick 
    and cnick = '%s'
    and score > 0 -- and score = 0
    and length(mark_ids) > 0 -- and length(mark_ids)=0
    and _id in %s
    and toYYYYMMDD(begin_time) between %d and %d -- (startDate, endDate)

-- 历史会话查询
select
    _id as id, -- 会话ID
    platform,
    channel,
    group,
    date,
    seller_nick, -- 店铺名
    cnick, -- 顾客名称
    snick, -- 客服子账号
    toString(begin_time) as begin_time,
    toString(end_time) as end_time,
    is_after_sale,
    is_inside,
    employee_name, -- 客服名称
    s_emotion_type,
    s_emotion_count,
    emotions,
    abnormals_type,
    abnormals_count,
    excellents_type,
    excellents_count,
    qc_word_source,
    qc_word_word,
    qc_word_count,
    qid,
    mark,
    mark_judge,
    mark_score,
    mark_score_add,
    mark_ids,
    last_mark_id,
    human_check,
    tag_score_stats_id,
    tag_score_stats_score,
    tag_score_add_stats_id,
    tag_score_add_stats_score,
    rule_stats_id,
    rule_stats_score,
    rule_stats_count,
    rule_add_stats_id,
    rule_add_stats_score,
    rule_add_stats_count,
    score,
    score_add,
    question_count,
    answer_count,
    toString(first_answer_time) as first_answer_time,
    qa_time_sum,
    qa_round_sum,
    focus_goods_id,
    is_remind,
    task_list_id,
    read_mark,
    last_msg_id,
    consulte_transfor_v2,
    order_info_id,
    order_info_status,
    order_info_payment,
    order_info_time,
    intel_score,
    remind_ntype,
    toString(first_follow_up_time) as first_follow_up_time,
    is_follow_up_remind,
    emotion_detect_mode,
    has_withdraw_robot_msg,
    is_order_matched,
    suspected_positive_emotion,
    suspected_problem,
    suspected_excellent,
    has_after,
    cnick_customize_rule,
    toString(update_time) as update_time,
    sign
from dwd.xdqc_dialog_all
where seller_nick in %s -- param.SellerNicks
and snick in %s -- param.SNick 
and cnick = '%s'
and score > 0 -- and score = 0
and length(mark_ids) > 0 -- and length(mark_ids)=0
and _id in %s
and toYYYYMMDD(begin_time) between %d and %d -- (startDate, endDate)