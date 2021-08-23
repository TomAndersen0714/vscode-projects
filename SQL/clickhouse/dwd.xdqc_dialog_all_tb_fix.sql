insert into dwd.xdqc_dialog_all
select 
    a._id ,platform ,channel ,`group` ,
    `date` ,seller_nick ,cnick ,snick ,begin_time ,
    end_time ,is_after_sale ,is_inside ,employee_name , s_emotion_type ,s_emotion_count ,c_emotion_type ,c_emotion_count ,emotions,
    abnormals_type ,abnormals_count ,excellents_type ,excellents_count ,    qc_word_source ,
    qc_word_word,qc_word_count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
    human_check ,   tag_score_stats_id,tag_score_stats_score,tag_score_add_stats_id,tag_score_add_stats_score,rule_stats_id,
    rule_stats_score,rule_stats_count ,rule_add_stats_id,rule_add_stats_score,rule_add_stats_count ,score ,
    score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
    qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark,   last_msg_id ,
    consulte_transfor_v2 ,order_info_id,order_info_status,
    order_info_payment,order_info_time,intel_score ,remind_ntype ,
    first_follow_up_time ,is_follow_up_remind ,emotion_detect_mode ,has_withdraw_robot_msg ,is_order_matched ,
    suspected_positive_emotion ,suspected_problem ,suspected_excellent ,has_after , cnick_customize_rule,
    update_time, -1
from (
        select *
        from dwd.xdqc_dialog_all
        where `begin_time` >= toDateTime64('2021-07-10 00:00:00', 3)
            and `begin_time` <= toDateTime64('2021-07-10 24:00:00', 3)
    ) a
    left join (
        select _id
        from ods.xdqc_dialog_update_all
        where ch_insert_day = 20210809
    ) as b on a._id = b._id
where b._id <> ''


insert into dwd.xdqc_dialog_all
select 
    a._id ,platform ,channel ,`group` ,
    `date` ,seller_nick ,cnick ,snick ,begin_time ,
    end_time ,is_after_sale ,is_inside ,employee_name , s_emotion_type ,s_emotion_count ,c_emotion_type ,c_emotion_count ,emotions,
    abnormals_type ,abnormals_count ,excellents_type ,excellents_count ,    qc_word_source ,
    qc_word_word,qc_word_count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
    human_check ,   tag_score_stats_id,tag_score_stats_score,tag_score_add_stats_id,tag_score_add_stats_score,rule_stats_id,
    rule_stats_score,rule_stats_count ,rule_add_stats_id,rule_add_stats_score,rule_add_stats_count ,score ,
    score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
    qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark,   last_msg_id ,
    consulte_transfor_v2 ,order_info_id,order_info_status,
    order_info_payment,order_info_time,intel_score ,remind_ntype ,
    first_follow_up_time ,is_follow_up_remind ,emotion_detect_mode ,has_withdraw_robot_msg ,is_order_matched ,
    suspected_positive_emotion ,suspected_problem ,suspected_excellent ,has_after , cnick_customize_rule,
    update_time, -1
from (
        select *
        from dwd.xdqc_dialog_all
        where `begin_time` >= toDateTime64('2021-07-11 00:00:00', 3)
            and `begin_time` <= toDateTime64('2021-07-11 24:00:00', 3)
    ) a
    left join (
        select _id
        from ods.xdqc_dialog_update_all
        where ch_insert_day = 20210810
    ) as b on a._id = b._id
where b._id <> ''


insert into dwd.xdqc_dialog_all
select 
    a._id ,platform ,channel ,`group` ,
    `date` ,seller_nick ,cnick ,snick ,begin_time ,
    end_time ,is_after_sale ,is_inside ,employee_name , s_emotion_type ,s_emotion_count ,c_emotion_type ,c_emotion_count ,emotions,
    abnormals_type ,abnormals_count ,excellents_type ,excellents_count ,    qc_word_source ,
    qc_word_word,qc_word_count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
    human_check ,   tag_score_stats_id,tag_score_stats_score,tag_score_add_stats_id,tag_score_add_stats_score,rule_stats_id,
    rule_stats_score,rule_stats_count ,rule_add_stats_id,rule_add_stats_score,rule_add_stats_count ,score ,
    score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
    qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark,   last_msg_id ,
    consulte_transfor_v2 ,order_info_id,order_info_status,
    order_info_payment,order_info_time,intel_score ,remind_ntype ,
    first_follow_up_time ,is_follow_up_remind ,emotion_detect_mode ,has_withdraw_robot_msg ,is_order_matched ,
    suspected_positive_emotion ,suspected_problem ,suspected_excellent ,has_after , cnick_customize_rule,
    update_time, -1
from (
        select *
        from dwd.xdqc_dialog_all
        where `begin_time` >= toDateTime64('2021-07-12 00:00:00', 3)
            and `begin_time` <= toDateTime64('2021-07-12 24:00:00', 3)
    ) a
    left join (
        select _id
        from ods.xdqc_dialog_update_all
        where ch_insert_day = 20210811
    ) as b on a._id = b._id
where b._id <> ''

optimize table dwd.xdqc_dialog_local ON cluster cluster_3s_2r partition 20210710 final
optimize table dwd.xdqc_dialog_local ON cluster cluster_3s_2r partition 20210711 final
optimize table dwd.xdqc_dialog_local ON cluster cluster_3s_2r partition 20210712 final