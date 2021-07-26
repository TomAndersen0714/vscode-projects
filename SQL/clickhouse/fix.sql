select 
    question_b_name as "问题",
    sum(receive_pv) as "咨询量",
    if(
        "咨询量" < sum(robot_send_pv),
        "咨询量" - sum(click_send_pv),
        sum(robot_send_pv)
    ) as "机器人答复",
    sum(click_send_pv) as "点击答复",
    sum(receive_pv) - "机器人答复" - "点击答复" as "未答复"
from pub_app_mp.shop_question_all
where day between toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.start=week_ago }}')
    ) and toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.end=yesterday }}')
    )
    and question_b_name != '机器人无法识别的问题'
    and shop_id = '{{ shop_id=5ecf41c4d3ff36001406f16b }}'
    and question_type_name = 'industy_question'
group by "问题"
order by "咨询量" desc


isleep海外旗舰店
5f8e84e628d83700186da974


