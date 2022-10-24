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
        toInt64(uniqExact(cnick)) AS recv_cnick_cnt,
        toInt64(uniqExactIf(cnick, session_send_cnt>0)) AS reply_cnick_cnt,
        toInt64(uniqExactIf(cnick, m_session_send_cnt>0)) AS m_reply_cnick_cnt,
        toInt64(uniqExact(session_id)) AS session_cnt,
        toInt64(uniqExactIf(session_id, qa_reply_intervals_secs[1]<30)) AS first_reply_within_thirty_secs_session_cnt,
        toInt64(uniqExactIf(session_id, m_qa_reply_intervals_secs[1]<30)) AS m_first_reply_within_thirty_secs_session_cnt,
        SUM(session_recv_cnt) AS recv_msg_cnt,
        SUM(session_send_cnt) AS send_msg_cnt,
        SUM(m_session_send_cnt) AS m_send_msg_cnt,
        SUM(qa_cnt) AS reply_cnt,
        SUM(m_qa_cnt) AS m_reply_cnt,
        SUM(arraySum(qa_reply_intervals_secs)) AS reply_interval_secs_sum,
        SUM(arraySum(m_qa_reply_intervals_secs)) AS m_reply_interval_secs_sum
    FROM ft_dwd.session_detail_all
    WHERE day = {ds_nodash}
    AND platform = '{platform}'
    AND shop_id = '{shop_id}'
    GROUP BY day, platform, shop_id, snick
) AS stat_info
ARRAY JOIN
    [
        'recv_cnick_cnt','reply_cnick_cnt','m_reply_cnick_cnt','session_cnt','first_reply_within_thirty_secs_session_cnt',
        'm_first_reply_within_thirty_secs_session_cnt','recv_msg_cnt','send_msg_cnt','m_send_msg_cnt','reply_cnt',
        'm_reply_cnt','reply_interval_secs_sum','m_reply_interval_secs_sum'
    ] AS stat_label,
    [
        `recv_cnick_cnt`,`reply_cnick_cnt`,`m_reply_cnick_cnt`,`session_cnt`,`first_reply_within_thirty_secs_session_cnt`,
        `m_first_reply_within_thirty_secs_session_cnt`,`recv_msg_cnt`,`send_msg_cnt`,`m_send_msg_cnt`,`reply_cnt`,
        `m_reply_cnt`,`reply_interval_secs_sum`,`m_reply_interval_secs_sum`
    ] AS stat_value