


WITH t1 AS
  (SELECT platform,
          shop_oid,
          xd_shop_nick,
          plat_goods_id,
          count(1) AS pv,
          count(DISTINCT cnick) AS cuv
  FROM dwd.pdd_xdrs_logs
  WHERE DAY = ${var:param_day}
     AND act = 'recv_msg'
     AND plat_goods_id != ''
  GROUP BY platform,
            shop_oid,
            xd_shop_nick,
            plat_goods_id ) 

insert overwrite app_mp.pdd_daily_request_goods_stat partition(day)
SELECT '${var:param_date}' AS `date`,
      t1.platform,
      t1.shop_oid,
      t1.xd_shop_nick,
      t1.plat_goods_id,
      t2._id AS goods_oid,
      pv,
      cuv,
      ${var:param_day} AS DAY
FROM t1
JOIN ods.pdd_xdmp_goods AS t2 ON t1.plat_goods_id = t2.plat_goods_id
AND t1.platform = t2.platform;

