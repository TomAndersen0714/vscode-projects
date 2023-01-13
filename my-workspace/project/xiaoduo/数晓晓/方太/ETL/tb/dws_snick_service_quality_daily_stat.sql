INSERT INTO ft_dws.snick_day_stats_all
SELECT
    day,
    shop_id,
    platform,
    snick,
    'all' AS goods_id,
    stat_label,
    stat_value,
    now64(3, 'Asia/Shanghai') AS update_time
FROM (
    SELECT
        day,
        platform,
        shop_id,
        snick,
        toFloat64(uniqExactIf(cnick, session_recv_cnt>0)) AS recv_cnick_cnt,
        toFloat64(uniqExactIf(cnick, session_recv_cnt>0 AND session_send_cnt>0)) AS reply_cnick_cnt,
        toFloat64(uniqExactIf(cnick, session_recv_cnt>0 AND m_session_send_cnt>0)) AS m_reply_cnick_cnt,
        toFloat64(uniqExact(session_id)) AS session_cnt,
        toFloat64(uniqExactIf(session_id, m_session_send_cnt>0)) AS m_session_cnt,
        toFloat64(uniqExactIf(session_id, notEmpty(qa_reply_intervals_secs) AND qa_reply_intervals_secs[1]<30)) AS first_reply_within_thirty_secs_session_cnt,
        toFloat64(uniqExactIf(session_id, notEmpty(m_qa_reply_intervals_secs) AND m_qa_reply_intervals_secs[1]<30)) AS m_first_reply_within_thirty_secs_session_cnt,
        toFloat64(SUM(session_recv_cnt)) AS recv_msg_cnt,
        toFloat64(SUM(session_send_cnt)) AS send_msg_cnt,
        toFloat64(SUM(m_session_send_cnt)) AS m_send_msg_cnt,
        toFloat64(SUM(qa_cnt)) AS qa_sum,
        toFloat64(SUM(m_qa_cnt)) AS m_qa_sum,
        toFloat64(SUM(arraySum(qa_reply_intervals_secs))) AS reply_interval_secs_sum,
        toFloat64(SUM(arraySum(m_qa_reply_intervals_secs))) AS m_reply_interval_secs_sum,
        -- 平响
        IF(qa_sum!=0, reply_interval_secs_sum / qa_sum, 0.0) AS reply_interval_secs_avg,
        IF(qa_sum!=0, m_reply_interval_secs_sum / m_qa_sum, 0.0) AS m_reply_interval_secs_avg,

        -- 30秒响应率
        IF(session_cnt!=0, first_reply_within_thirty_secs_session_cnt / session_cnt, 0.0) AS first_reply_within_thirty_secs_session_pct,
        IF(m_session_cnt!=0, m_first_reply_within_thirty_secs_session_cnt / m_session_cnt, 0.0) AS m_first_reply_within_thirty_secs_session_pct,

        -- 问答比
        IF(recv_msg_cnt!=0, send_msg_cnt / recv_msg_cnt, 0.0) AS send_msg_pct,
        IF(recv_msg_cnt!=0, m_send_msg_cnt / recv_msg_cnt, 0.0) AS m_send_msg_pct,

        -- 回复率
        IF(recv_cnick_cnt!=0, reply_cnick_cnt / recv_cnick_cnt, 0.0) AS reply_cnick_pct,
        IF(recv_cnick_cnt!=0, m_reply_cnick_cnt / recv_cnick_cnt, 0.0) AS m_reply_cnick_pct
    FROM ft_dwd.session_detail_all
    WHERE day = {{ds_nodash}}
      AND platform = '{{platform}}'
      AND shop_id = '{{shop_id}}'
    GROUP BY day, platform, shop_id, snick
) AS stat_info
ARRAY JOIN
    [
        'recv_cnick_cnt','reply_cnick_cnt','m_reply_cnick_cnt','session_cnt','first_reply_within_thirty_secs_session_cnt',
        'm_first_reply_within_thirty_secs_session_cnt','recv_msg_cnt','send_msg_cnt','m_send_msg_cnt','qa_sum',
        'm_qa_sum','reply_interval_secs_sum','m_reply_interval_secs_sum', 
        'reply_interval_secs_avg', 'm_reply_interval_secs_avg', 'first_reply_within_thirty_secs_session_pct', 'm_first_reply_within_thirty_secs_session_pct',
        'send_msg_pct', 'm_send_msg_pct', 'reply_cnick_pct', 'm_reply_cnick_pct'
    ] AS stat_label,
    [
        `recv_cnick_cnt`,`reply_cnick_cnt`,`m_reply_cnick_cnt`,`session_cnt`,`first_reply_within_thirty_secs_session_cnt`,
        `m_first_reply_within_thirty_secs_session_cnt`,`recv_msg_cnt`,`send_msg_cnt`,`m_send_msg_cnt`,`qa_sum`,
        `m_qa_sum`,`reply_interval_secs_sum`,`m_reply_interval_secs_sum`,
        `reply_interval_secs_avg`, `m_reply_interval_secs_avg`, `first_reply_within_thirty_secs_session_pct`, `m_first_reply_within_thirty_secs_session_pct`,
        `send_msg_pct`, `m_send_msg_pct`, `reply_cnick_pct`, `m_reply_cnick_pct`
    ] AS stat_value