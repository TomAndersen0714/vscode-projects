-- 筛选有效会话, PS: 包含了售前和售后
-- 询单周期内
insert into tmp.session_filter_all
SELECT `day`,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                snick,
                session_id,
                session_start_time,
                session_end_time,
                is_start_by_cnick,
                is_end_by_cnick,
                focus_goods_ids,
                c_active_send_goods_ids ,
                s_active_send_goods_ids,
                 2 AS `cycle`
         FROM
           (SELECT session.*,
                   -- 是否仅咨询包含配件
                   if(length(focus_goods_ids) = 1
                      AND hasAny(session.focus_goods_ids,goods.goods_arr),1,0) AS flag
            FROM
              (SELECT `day`,
                      shop_id,
                      session_id,
                      replaceAll(snick,'cntaobao','') AS snick,
                      replaceAll(cnick,'cntaobao','') AS buyer_nick,
                      replaceAll(real_buyer_nick,'cntaobao','') AS real_buyer_nick,
                      [toDateTime64(recv_msg_start_time,6),toDateTime64(recv_msg_end_time,6),toDateTime64(send_msg_start_time,6),toDateTime64(send_msg_end_time,6)] AS time_arr,
                      arrayFilter(x-> toYYYYMMDD(x) != toYYYYMMDD(toDateTime64('0000-00-00 00:00:00.000000',6)), time_arr) AS new_time_arr,
                      arrayReduce('min',new_time_arr) AS session_start_time,
                      arrayReduce('max',new_time_arr) AS session_end_time,
                      if(toDateTime64(recv_msg_start_time,6) < toDateTime64(send_msg_start_time,6),'1','0') AS is_start_by_cnick,
                      if(toDateTime64(recv_msg_end_time,6) > toDateTime64(send_msg_end_time,6),'1','0') AS is_end_by_cnick,
                      session_recv_cnt,
                      session_send_cnt,
                      arrayFilter(x -> x !='',arrayDistinct(focus_goods_ids)) AS focus_goods_ids,
                      c_active_send_goods_ids ,
                      s_active_send_goods_ids
               FROM ft_dwd.session_detail_all
               WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'), 2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
               AND cnick = 'one_id_27047089'
                 AND shop_id = '5cac112e98ef4100118a9c9f'
                 -- 顾客仅有一句话的不算询单（剔除单口相声）
                 AND session_recv_cnt > 1
                 -- 会话中有被转接记录不算询单：转出的不算询单，转入和未转的算一个询单
                 AND has_transfer IN (0, 1)
                 -- 剔除广告、小二、内部沟通、测试账号（导入的方式剔除）
                 AND concat(shop_id,'-',cnick) NOT IN
                   (SELECT concat(shop_id,'-',cnick)
                    FROM ft_dim.account_filter_all) ) AS `session`
            LEFT JOIN
              (SELECT groupArray(goods_id) AS goods_arr,
                      shop_id
               FROM ft_dim.goods_info_all
               WHERE `type` = '4'
                 AND shop_id = '5cac112e98ef4100118a9c9f'
               GROUP BY shop_id) AS goods USING(shop_id)
            --剔除只咨询配件的
            WHERE flag = 0
)

--常规询单
insert into tmp.ask_order_cov_detail_all
SELECT `day`,
       shop_id,
       'tb' as platform,
       buyer_nick,
       -- BUG: session_id 不应该用于排序
       argMax(real_buyer_nick,session_id) AS real_buyer_nick,
       snick,
       session_id,
      argMax(session_start_time,session_id) AS session_start_time,
      argMax(session_end_time,session_id) AS session_end_time,
      argMax(is_start_by_cnick,session_id) AS is_start_by_cnick,
      argMax(is_end_by_cnick,session_id) AS is_end_by_cnick,
      argMax(focus_goods_ids,session_id) AS focus_goods_ids,
      argMax(c_active_send_goods_ids,session_id) AS c_active_send_goods_ids ,
      argMax(s_active_send_goods_ids,session_id) AS s_active_send_goods_ids,
      order_id,
      goods_id,
      -- 从订单*产品记录的时间中获取首个已创建状态时间, BUG: 未预先排序, 近乎随机抽取
      if(has(groupArray(status),'created'),toString(groupArray(modified)[indexOf(groupArray(status),'created')]),'') AS created_time,
      -- 从订单*产品记录的时间中获取首个已付款状态时间, BUG: 未预先排序, 近乎随机抽取
      if(has(groupArray(status),'paid'),toString(groupArray(modified)[indexOf(groupArray(status),'paid')]),'') AS paid_time,
      -- 从订单*产品记录的时间中获取首个已付款状态订单金额, BUG: 未预先排序, 近乎随机抽取
      if(has(groupArray(status),'paid'),groupArray(order_payment)[indexOf(groupArray(status),'paid')], if(has(groupArray(status),'created'),groupArray(order_payment)[indexOf(groupArray(status),'created')],0)) AS order_payment,
      -- 从订单*产品记录的时间中获取首个已付款订单的商品金额, BUG: 未预先排序, 近乎随机抽取
      if(has(groupArray(status),'paid'),groupArray(goods_payment)[indexOf(groupArray(status),'paid')], if(has(groupArray(status),'created'),groupArray(goods_payment)[indexOf(groupArray(status),'created')],0)) AS goods_payment,
      Max(step_trade_status),
      Max(step_paid_fee),
      Max(new_order_type),
      Max(goods_num),
      0 AS is_refund,
      if(paid_time != '',1,0) AS is_transf,
       2 AS `cycle`
FROM
  (SELECT order_info.*,new_modified as modified,
          -- 计算订单时间与每个会话开始时间的差值, 为正数时, 则表明订单状态生成在会话发生之后, 即售前
          arrayMap(x-> toDateTime(modified) - toDateTime(toDateTime64(x,6)),session_start_time_arr) AS time_subtract,
          -- 筛选订单状态-会话时间差大于0的值, 即订单状态记录生成时间, 与其之前发生的会话的时间差
          arrayFilter(x-> x>=0,time_subtract) AS time_subtract_filter,
          -- 在先聊天后下单的数据中, 取下单时间距离会话最近的时间差
          arrayReduce('min',time_subtract_filter) AS time_subtract_min,
          -- 定位下单时, 所处的会话序号
          indexOf(time_subtract,time_subtract_min) AS index_v,
          shop_id,
          buyer_nick,
          snick_arr[index_v] AS snick,
          day_arr[index_v] AS `day`,
          real_buyer_nick_arr[index_v] AS real_buyer_nick,
          session_id_arr[index_v] AS session_id,
          session_start_time_arr[index_v] AS session_start_time,
          session_end_time_arr[index_v] AS session_end_time,
          is_start_by_cnick_arr[index_v] AS is_start_by_cnick,
          is_end_by_cnick_arr[index_v] AS is_end_by_cnick,
          focus_goods_ids_arr[index_v] AS focus_goods_ids,
          s_active_send_goods_ids_arr[index_v] as s_active_send_goods_ids,
          c_active_send_goods_ids_arr[index_v] as c_active_send_goods_ids
   FROM
     (  -- 聚合询单周期内每个买家的会话记录
        SELECT shop_id,
             buyer_nick,
             groupArray(snick) AS snick_arr,
             groupArray(`day`) AS day_arr,
             groupArray(real_buyer_nick) AS real_buyer_nick_arr,
             groupArray(session_id) AS session_id_arr,
             groupArray(session_start_time) AS session_start_time_arr,
             groupArray(session_end_time) AS session_end_time_arr,
             groupArray(is_start_by_cnick) AS is_start_by_cnick_arr,
             groupArray(is_end_by_cnick) AS is_end_by_cnick_arr,
             groupArray(focus_goods_ids) AS focus_goods_ids_arr,
             groupArray(c_active_send_goods_ids) AS c_active_send_goods_ids_arr,
             groupArray(s_active_send_goods_ids) AS s_active_send_goods_ids_arr
      FROM
        (   -- 询单周期内所有的会话记录
            SELECT `day`,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                snick,
                session_id,
                session_start_time,
                session_end_time,
                is_start_by_cnick,
                is_end_by_cnick,
                focus_goods_ids,
                c_active_send_goods_ids,
                s_active_send_goods_ids
         FROM tmp.session_filter_all
         where `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'), 2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
                AND shop_id = '5cac112e98ef4100118a9c9f'
                AND buyer_nick = 'one_id_27047089'
                and `cycle` = 2
                and  concat(shop_id,'-',buyer_nick) IN
                -- 只看询单周期内有订单记录的数据
                (SELECT DISTINCT concat(shop_id,'-',buyer_nick)
                 FROM ft_dwd.order_detail_all
                 WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'), 2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
                   AND shop_id = '5cac112e98ef4100118a9c9f'
                   AND status IN ('created',
                                  'paid')
                   and order_type != 'step' )
                                  )
      GROUP BY shop_id,
               buyer_nick) AS session_1
   LEFT JOIN
     (SELECT shop_id,
             order_id,
             toDateTime64(arrayReduce('min',groupArray(modified)),6) AS new_modified,
             argMin(buyer_nick,modified) as buyer_nick,
             goods_id,
             argMin(goods_payment,modified) as goods_payment,
             argMin(order_payment,modified) as order_payment,
             argMin(step_trade_status,modified) as step_trade_status,
             argMin(step_paid_fee,modified) as step_paid_fee,
             argMin(order_type,modified) as new_order_type,
             argMin(goods_num,modified) as goods_num,
             status
      FROM ft_dwd.order_detail_all
      WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'),2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
        AND shop_id = '5cac112e98ef4100118a9c9f'
        AND status IN ('created',
                       'paid')
        and order_type != 'step'
        group by shop_id,order_id,status,goods_id
        ) AS `order_info`
    -- 每个买家关联其在询单周期内的所有订单状态数据, PS: 数据膨胀系数为买家平均订单数
    USING(shop_id, buyer_nick)
    -- 剔除订单状态先于会话产生的订单状态记录, 筛选订单状态记录产生在会话中, 或者产生在会话后的, 即售前
    WHERE length(time_subtract_filter) > 0)
GROUP BY `day`,
         shop_id,
         session_id,
         buyer_nick,
         order_id,
         goods_id,
         snick
HAVING buyer_nick = 'one_id_27047089'


-- 订单*状态*产品粒度询单记录-询单转化记录-非预售订单相关
insert into ft_dwd.ask_order_cov_detail_all
SELECT *,0 as is_deposited
      FROM tmp.ask_order_cov_detail_all
      WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'),2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
                     AND shop_id = '5cac112e98ef4100118a9c9f'
                     AND `cycle` = 2
                     and  order_id IN
          (SELECT order_id
                FROM
                  (
                   -- 询单周期内创建的订单
                   SELECT order_id,
                          -- 筛选创建状态
                          arrayFilter(x-> x!= '',groupArray(paid_time)) AS flag
                   FROM tmp.ask_order_cov_detail_all
                   WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'),2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
                     AND shop_id = '5cac112e98ef4100118a9c9f'
                     AND `cycle` = 2
                   GROUP BY order_id
                   -- 筛选存在创建状态的订单(有付款才算转化)
                   HAVING length(flag) != 0)
                UNION ALL
                -- 询单周期前一天创建的订单
                SELECT order_id
                FROM ft_dwd.ask_order_cov_detail_all
                WHERE `day` = toYYYYMMDD(subtractDays(toDate('2023-03-01'),2))
                  and shop_id = '5cac112e98ef4100118a9c9f'
                  AND created_time != '' and `cycle` = 2
                  AND paid_time = '' )


-- 订单*状态*产品粒度询单记录-询单未转化记录-无任何相关订单
insert into ft_dwd.ask_order_cov_detail_all
SELECT `day`,
       shop_id,
       'tb' as platform,
       buyer_nick,
       real_buyer_nick,
       snick,
       session_id,
       session_start_time,
       session_end_time,
       is_start_by_cnick,
       is_end_by_cnick,
       focus_goods_ids,
       c_active_send_goods_ids,
       s_active_send_goods_ids,
       '' as order_id,
       '' as goods_id,
       '' as created_time,
       '' as paid_time,
       0 as order_payment,
       0 as goods_payment,
       '',
       '',
       '',
       0,
       0 as is_refund,
       0 as is_transf,
       2 AS `cycle`,
       0 as is_deposited
FROM tmp.session_filter_all
   WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'), 2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
    AND shop_id = '5cac112e98ef4100118a9c9f'
    and `cycle` = 2
     -- 剔除已经关联上会话的询单记录
     AND session_id NOT IN
       (SELECT DISTINCT session_id
        FROM ft_dwd.ask_order_cov_detail_all
        WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-01'), 2 - 1)) AND toYYYYMMDD(toDate('2023-03-01'))
        )
     -- 剔除180天有过订单记录的
     AND replaceAll(buyer_nick,'cntaobao','') NOT IN
       (SELECT DISTINCT buyer_nick
        FROM ft_dwd.order_detail_all
        WHERE `day`BETWEEN toYYYYMMDD(subtractDays(subtractDays(toDate('2023-03-01'),2),180)) AND toYYYYMMDD(subtractDays(toDate('2023-03-01'),2))
          AND shop_id = '5cac112e98ef4100118a9c9f' and buyer_nick != '')
      and buyer_nick != ''