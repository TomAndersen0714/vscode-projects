select * from (
    select question AS `问题`,
        question_id, 
        '全部'  as user_group,
        sum(recv_count) as `咨询量`,
        sum(reply_count) as `自动回复`,
        sum(no_reply_count) as `机器人未回复`,
        if(length(splitByChar('.',toString(round(`自动回复`/`咨询量`*100,2))))!=2,
        concat(toString(round(`自动回复`/`咨询量`*100,2)),'.00%'),
        if(length(splitByChar('.',toString(round(`自动回复`/`咨询量`*100,2)))[2])=1,
        concat(toString(round(`自动回复`/`咨询量`*100, 2)),'0%'),
        concat(toString(round(`自动回复`/`咨询量`*100, 2)),'%'))) as `应答率`
    from dws.shop_snick_question_stat_all
    where day between toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=week_ago }}')) 
                    and  toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
    AND shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
    AND user_group !=''
    AND question_type = 'question_b'
    group by question,question_id,user_group

    union all

    select question AS `问题`,
        question_id, 
        user_group,
        sum(recv_count) as `咨询量`,
        sum(reply_count) as `自动回复`,
        sum(no_reply_count) as `机器人未回复`,
        if(length(splitByChar('.',toString(round(`自动回复`/`咨询量`*100,2))))!=2,
        concat(toString(round(`自动回复`/`咨询量`*100,2)),'.00%'),
        if(length(splitByChar('.',toString(round(`自动回复`/`咨询量`*100,2)))[2])=1,
        concat(toString(round(`自动回复`/`咨询量`*100, 2)),'0%'),
        concat(toString(round(`自动回复`/`咨询量`*100, 2)),'%'))) as `应答率`
    from dws.shop_snick_question_stat_all
    where day between toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=week_ago }}')) 
                    and  toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
    AND shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
    AND question_type = 'question_b'
    group by question,question_id,user_group
)
where  user_group in splitByChar(',', '{{ user_group=全部 }}')

order by `咨询量` desc