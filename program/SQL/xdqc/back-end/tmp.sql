select
    last_mark_id,
    count(1) AS cnt
from dwd.xdqc_dialog_all
where toYYYYMMDD(begin_time) = toYYYYMMDD(toDate('{{ ds }}'))
and seller_nick in ['方太官方旗舰店','方太京东自营旗舰店','方太厨卫旗舰店','方太烟灶旗舰店','方太京东旗舰店','方太集成烹饪中心京东自营旗舰店']
and last_mark_id != ''
and platform = 'tb'
group by last_mark_id