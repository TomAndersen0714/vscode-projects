with chat_measure as (
    SELECT snick,
        cnick,
        act,
        send_msg_from,
        msg,
        if(
            ms_msg_time != 0,
            ms_msg_time,
            cast(msg_time as int) * 1000
        ) as create_time,
        qa_id,
        row_number() OVER (
            PARTITION BY snick,
            cnick
            ORDER BY create_time
        ) AS rn
    FROM ods.jd_xdrs_logs
    WHERE day = 20221110
        AND act IN ('send_msg', 'recv_msg')
),
badcase_qa_id as (
    SELECT qa_id
    FROM ods.badcase_report
    WHERE day = 20221110
        AND create_time BETWEEN "2022-11-10 22" AND "2022-11-10 23"
),
report_chat as (
    SELECT c.qa_id,
        c.snick,
        c.cnick,
        c.rn
    FROM badcase_qa_id b
        JOIN [shuffle] chat_measure c USING (qa_id)
)
insert into dwd.badcase_context partition(day = 20221110)
SELECT r.qa_id as context_id,
    c.qa_id as qa_id,
    c.snick,
    c.cnick,
    c.act,
    c.send_msg_from,
    c.msg,
    c.create_time as msg_time,
    coalesce(r.qa_id = c.qa_id, false) is_reported,
    2022111022 as hour_nodash
FROM report_chat r
    LEFT JOIN [shuffle] chat_measure c ON c.rn BETWEEN r.rn - 5 AND r.rn + 5
    AND c.snick = r.snick
    AND c.cnick = r.cnick;