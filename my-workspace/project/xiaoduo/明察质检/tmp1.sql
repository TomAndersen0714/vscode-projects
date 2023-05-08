select if(
        b.question_id = '',
        concat(question, '【历史数据】'),
        question
    ) AS `问题`,
    question_id,
    `咨询量`,
    `自动回复`,
    `机器人未回复`,
    `应答率`
from (
        select question,
            question_id,
            if(
                sum(no_reply_count) > sum(recv_count),
                sum(no_reply_count),
                sum(recv_count)
            ) as `咨询量`,
            sum(no_reply_count) as `机器人未回复`,
            `咨询量` - `机器人未回复` as `自动回复`,
            if(
                length(
                    splitByChar('.', toString(round(`自动回复` / `咨询量` * 100, 2)))
                ) != 2,
                concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '.00%'),
                if(
                    length(
                        splitByChar('.', toString(round(`自动回复` / `咨询量` * 100, 2))) [2]
                    ) = 1,
                    concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '0%'),
                    concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '%')
                )
            ) as `应答率`
        from (
                select distinct shop_id,
                    snick,
                    question_id,
                    question,
                    question_type,
                    recv_count,
                    reply_count,
                    no_reply_count,
                    day
                from dws.shop_snick_question_stat_all
                where day between toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.start=week_ago }}')
                    ) and toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.end=yesterday }}')
                    )
                    AND shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
                    AND question_type = 'question_b'
            )
        where '{{ snk_group_name }}' = ''
        group by question,
            question_id
        union all
        select question,
            question_id,
            if(
                sum(no_reply_count) > sum(recv_count),
                sum(no_reply_count),
                sum(recv_count)
            ) as `咨询量`,
            sum(no_reply_count) as `机器人未回复`,
            `咨询量` - `机器人未回复` as `自动回复`,
            if(
                length(
                    splitByChar('.', toString(round(`自动回复` / `咨询量` * 100, 2)))
                ) != 2,
                concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '.00%'),
                if(
                    length(
                        splitByChar('.', toString(round(`自动回复` / `咨询量` * 100, 2))) [2]
                    ) = 1,
                    concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '0%'),
                    concat(toString(round(`自动回复` / `咨询量` * 100, 2)), '%')
                )
            ) as `应答率`
        from (
                select distinct shop_id,
                    snick,
                    question_id,
                    question,
                    question_type,
                    recv_count,
                    reply_count,
                    no_reply_count,
                    day
                from dws.shop_snick_question_stat_all
                where day between toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.start=week_ago }}')
                    ) and toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.end=yesterday }}')
                    )
                    AND shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
                    AND question_type = 'question_b'
                    AND user_group in splitByChar(',', '{{ snk_group_name }}')
            )
        group by question,
            question_id
    )
    left join (
        select distinct question_id
        from dim.shop_category_question_all
        where shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
    ) as b using(question_id)
WHERE (`问题` != '机器人无法识别的问题')
order by `咨询量` desc