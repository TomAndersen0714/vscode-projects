-- ClickHouse
select shop_id,
    snk_name as "子账号名称",
    serve_cuv as "服务人数",
    serve_pv as "服务人次",
    received_pv as "接收问题量",
    received_cuv,
    identified_pv as "识别问题量",
    if(
        received_pv <= 0,
        0,
        round(identified_pv / received_pv * 100, 2)
    ) as "识别率",
    auto_reply_pv as "机器人自动回复量",
    click_reply_pv as "点击回复量",
    human_reply_pv as "人工回复数",
    if(
        received_pv <= 0,
        0,
        round(
            (auto_reply_pv + click_reply_pv) / received_pv * 100,
            2
        )
    ) as "应答率",
    round(robot_avg_resp_interval, 2) as "机器人平均响应时长",
    round(human_avg_resp_interval, 2) as "人工响应时长",
    round(avg_resp_interval, 2) as "人机协作响应时长",
    reminder_send_count as "催单发送量"
from (
        select shop_id,
            snk_name,
            serve_cuv,
            received_pv,
            received_cuv,
            identified_pv,
            auto_reply_pv,
            click_reply_pv,
            human_reply_pv,
            serve_pv,
            robot_avg_resp_interval,
            human_avg_resp_interval,
            avg_resp_interval
        from (
                select shop_oid as shop_id,
                    replaceAll(splitByChar(':', xd_shop_subnick) [2], 'cnjd', '') as snk_name,
                    sum(serve_cuv) as serve_cuv,
                    sum(received_pv) as received_pv,
                    sum(received_cuv) as received_cuv,
                    sum(received_cuv) as received_cuv,
                    sum(identified_pv) as identified_pv,
                    sum(auto_reply_pv) as auto_reply_pv,
                    sum(click_reply_pv) as click_reply_pv
                from app_mp.msg_day_platform_subnick
                where shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
                    and `date` between '{{ day.start=week_ago }}' and '{{ day.end=yesterday }}'
                group by shop_id,
                    snk_name
            ) as info
            left join (
                select shop_id,
                    snick as snk_name,
                    sum(serve_pv) as serve_pv,
                    sum(human_reply_pv) as human_reply_pv,
                    avg(
                        if(
                            robot_avg_resp_interval < 0,
                            0,
                            robot_avg_resp_interval
                        )
                    ) as robot_avg_resp_interval,
                    avg(
                        if(
                            human_avg_resp_interval < 0,
                            0,
                            human_avg_resp_interval
                        )
                    ) as human_avg_resp_interval,
                    avg(if(avg_resp_interval < 0, 0, avg_resp_interval)) as avg_resp_interval
                from pub_app_mp.snick_service_all
                where shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
                    and day between toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.start=week_ago }}')
                    ) and toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.end=yesterday }}')
                    )
                group by shop_id,
                    snk_name
            ) as other using(shop_id, snk_name)
    ) as snk_info
    left join (
        select shop_id,
            replaceAll(splitByChar(':', subnick) [2], 'cnjd', '') as snk_name,
            sum(send_count) as reminder_send_count
        from app_mp.reminder_subnick_stat
        where day between toYYYYMMDD(
                parseDateTimeBestEffort('{{ day.start=week_ago }}')
            ) and toYYYYMMDD(
                parseDateTimeBestEffort('{{ day.end=yesterday }}')
            )
            and shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
        group by shop_id,
            subnick
    ) as reminder_info using(shop_id, snk_name)
order by "服务人数" desc