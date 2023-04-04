SELECT *,0 as is_deposited
      FROM tmp_ask_order_cov_detail_all
      WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-19'),2 - 1)) AND toYYYYMMDD(toDate('2023-03-19'))
                     AND shop_id = '5cac112e98ef4100118a9c9f'
                     AND `cycle` = 2
                     and  order_id IN
          (SELECT order_id
                FROM
                  (
                   -- 询单周期内创建的订单
                   SELECT order_id,
                          -- 筛选创建状态
                          arrayFilter(x-> x!= '',groupArray(created_time)) AS flag
                   FROM tmp_ask_order_cov_detail_all
                   WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('2023-03-19'),2 - 1)) AND toYYYYMMDD(toDate('2023-03-19'))
                     AND shop_id = '5cac112e98ef4100118a9c9f'
                     AND `cycle` = 2
                   GROUP BY order_id
                   -- 筛选存在创建状态的订单
                   HAVING length(flag) != 0)
                UNION ALL
                -- 询单周期前一天创建的订单
                SELECT order_id
                FROM ft_dwd.ask_order_cov_detail_all
                WHERE `day` = toYYYYMMDD(subtractDays(toDate('2023-03-19'),2))
                  and shop_id = '5cac112e98ef4100118a9c9f'
                  AND created_time != '' and `cycle` = 2
                  AND paid_time = '' )