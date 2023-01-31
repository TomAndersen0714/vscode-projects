-- 店铺*客服粒度-出库量（不含静默）
-- 店铺*客服粒度-付款量（不含静默）
-- 店铺*客服粒度-出库率（出库量 / 付款量）（不含静默）
-- 店铺*客服粒度-出库人数（不含静默）
-- 店铺*客服粒度-付款人数（不含静默）
-- 店铺*客服粒度-出库率（出库人数 / 付款人数）（不含静默）
-- insert into ft_dws.snick_day_stats_all
select `day`,
       shop_id,
       '{{platform}}' as platform,
       snick,
       'all' as goods_id,
       stat_label,
       stat_value,
       toString(now64(3, 'Asia/Shanghai')) as update_at
from
(SELECT shop_id,
       `day`,
       snick,
       toFloat64(paid_order_cnt) AS paid_order_cnt,
       toFloat64(out_of_stock_cnt) AS out_of_stock_cnt,
       if(isNull(paid_order_cnt)
          OR paid_order_cnt = 0,0,out_of_stock_cnt/paid_order_cnt) AS out_of_stock_cnt_rat,
       toFloat64(paid_order_uv) AS paid_order_uv,
       toFloat64(out_of_stock_uv) AS out_of_stock_uv,
       if(isNull(paid_order_uv)
          OR paid_order_uv = 0,0,out_of_stock_uv/paid_order_uv) AS out_of_stock_uv_rat
FROM
  (SELECT shop_id,
          `day`,
          snick,
          count(DISTINCT buyer_nick) AS paid_order_uv,
          count(DISTINCT order_id) AS paid_order_cnt
   FROM ft_dwd.ask_order_cov_detail_all
   WHERE shop_id = '{{shop_id}}'
     AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
     AND cycle = {{cycle}}
     AND paid_time != ''
   GROUP BY shop_id,
            `day`,
            snick)
FULL OUTER JOIN
  (SELECT shop_id,
          `day`,
          snick,
          count(DISTINCT buyer_nick) AS out_of_stock_uv,
          count(DISTINCT order_id) AS out_of_stock_cnt
   FROM
     (SELECT DISTINCT shop_id,
                      `day`,
                      order_id,
                      buyer_nick
      FROM ft_dwd.order_detail_all
      WHERE shop_id = '{{shop_id}}'
        AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
        AND status IN ('shipped') )
   JOIN
     (SELECT DISTINCT shop_id,
                      order_id,
                      snick
      FROM ft_dwd.ask_order_cov_detail_all
      WHERE shop_id = '{{shop_id}}'
        AND `day` BETWEEN toYYYYMMDD(subtractDays(subtractDays(toDate('{{ds}}'),{{cycle}} - 1),180)) AND toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
        AND cycle = {{cycle}}
        AND paid_time != '' ) USING (shop_id,
                                     order_id)
   GROUP BY shop_id,
            `day`,
            snick ) USING shop_id,
                          `day`,
                          snick) array join
    ['{{cycle}}_ask_order_paid_cnt','{{cycle}}_out_of_stock_cnt','{{cycle}}_out_of_stock_cnt_rat',
     '{{cycle}}_ask_order_paid_order_uv','{{cycle}}_out_of_stock_uv','{{cycle}}_out_of_stock_uv_rat'] AS stat_label,
     [paid_order_cnt,out_of_stock_cnt,out_of_stock_cnt_rat,
      paid_order_uv,out_of_stock_uv,out_of_stock_uv_rat] AS stat_value;