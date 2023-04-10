
     SELECT `day`,
          shop_id,
          'jd' as platform,
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
          {{cycle}} AS `cycle`
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
          FROM tmp_session_filter_all
          where `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    and `cycle` = {{cycle}}
                    and  concat(shop_id,'-',buyer_nick) IN
                    -- 只看询单周期内有订单记录的数据
                    (SELECT DISTINCT concat(shop_id,'-',buyer_nick)
                    FROM ft_dwd.order_detail_all
                    WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
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
          WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
          AND shop_id = '{{shop_id}}'
          AND status IN ('created',
                         'paid')
          and order_type != 'step'
          group by shop_id,order_id,status,goods_id
          having buyer_nick = '{{cnick}}'
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
