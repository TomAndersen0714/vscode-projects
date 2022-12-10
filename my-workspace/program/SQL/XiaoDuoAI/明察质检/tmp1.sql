select question_id,
    min(question_type) as question_type,
    sum(pv) as total_pv
from app_mp.stat_question_for_shop_all
where (snick_oid = '6278800fcdb31800188eb179')
    and (
        day between 20221201 and 20221207
    )
    and (platform = 'pdd')
group by question_id
order by total_pv desc -- trace:65b0ea47d98240f30000001670481231