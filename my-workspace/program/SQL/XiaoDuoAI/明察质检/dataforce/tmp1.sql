select toDate(parseDateTimeBestEffort(toString(`day`))) as `时间`,
  shop_id,
  snk_group_name,
  recv_pv as `接收问题数`,
  idfy_pv as `识别问题数`,
  auto_reply_pv + click_answer_reply_pv as `回复问题数`,
  round(IF(recv_pv = 0, 0, idfy_pv / recv_pv) * 100, 2) AS `识别率`,
  round(
    IF(
      recv_pv = 0,
      0,
      (auto_reply_pv + click_answer_reply_pv) / recv_pv
    ) * 100,
    2
  ) AS `应答率`,
  buyer_start_session as `买家发起会话`,
  snk_start_session as `客服发起会话`
from (
    select day,
      shop_id,
      snk_group_name,
      recv_pv,
      idfy_pv,
      auto_reply_pv,
      click_answer_reply_pv,
      buyer_start_session,
      snk_start_session
    from app_mp.subnick_group_receive_all
    where shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
      and day BETWEEN toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.start=week_ago }}')
      ) AND toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.end=yesterday }}')
      )
    union all
    select day,
      shop_id,
      '全部' as snk_group_name,
      recv_pv,
      idfy_pv,
      auto_reply_pv,
      click_answer_reply_pv,
      buyer_start_session,
      snk_start_session
    from app_mp.shop_receive_v2_all
    where shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
      and day BETWEEN toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.start=week_ago }}')
      ) AND toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.end=yesterday }}')
      )
  )
where snk_group_name in splitByChar(',', '{{ snk_group_name=全部 }}')
order by `时间` asc