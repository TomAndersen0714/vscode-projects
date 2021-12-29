upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  received_pv,
  received_cuv
  )
SELECT '${var:param_date}' AS `date`,
       platform,
       shop_oid,
       xd_shop_nick,
       count(1) AS receive_pv,
       count(DISTINCT cnick) AS receive_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND act = 'recv_msg'
GROUP BY 1,
         2,
         3,
         4
;

upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  identified_pv,
  identified_cuv
  )
SELECT '${var:param_date}' AS `date`,
       platform,
       shop_oid,
       xd_shop_nick,
       count(1) AS identified_pv,
       count(DISTINCT cnick) AS identified_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND act = 'recv_msg'
  AND is_identified = 1
GROUP BY 1,
         2,
         3,
         4
;

upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  valid_pv,
  valid_cuv
  )
SELECT '${var:param_date}' AS `date`,
       platform,
       shop_oid,
       xd_shop_nick,
       count(1) AS valid_pv,
       count(DISTINCT cnick) AS valid_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY =  ${var:param_day}
  AND is_robot_reply = 1
GROUP BY 1,
         2,
         3,
         4
;

upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  auto_reply_pv,
  click_reply_pv
  )
SELECT '${var:param_date}' AS `date`,
       platform,
       shop_oid,
       xd_shop_nick,
       SUM(if(is_robot_reply = 1 and reply_mode = 2, 1, 0)) AS auto_reply_pv,
       SUM(if(is_robot_reply = 1 and reply_mode = 1, 1, 0)) AS click_reply_pv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND is_robot_reply = 1
GROUP BY 1,
         2,
         3,
         4
;


upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  serve_cuv
  )
SELECT '${var:param_date}' AS `date`,
      platform,
      shop_oid,
      xd_shop_nick,
      count(distinct cnick) as serve_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
GROUP BY 1,
         2,
         3,
         4
;


upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  s_serve_cuv
  )
SELECT '${var:param_date}' AS `date`,
      platform,
      shop_oid,
      xd_shop_nick,
      count(distinct cnick) as s_serve_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND mode = 'SEND'
GROUP BY 1,
         2,
         3,
         4
;

upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  h_serve_cuv
  )
SELECT '${var:param_date}' AS `date`,
      platform,
      shop_oid,
      xd_shop_nick,
      count(distinct cnick) as h_serve_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND mode = 'HYBRID'
GROUP BY 1,
         2,
         3,
         4
;

upsert INTO app_mp.msg_day_platform_nick(
  `date`, 
  platform, 
  shop_oid, 
  xd_shop_nick,
  r_serve_cuv
  )
SELECT '${var:param_date}' AS `date`,
      platform,
      shop_oid,
      xd_shop_nick,
      count(distinct cnick) as r_serve_cuv
FROM dwd.pdd_xdrs_logs
WHERE DAY = ${var:param_day}
  AND mode = 'REMIND'
GROUP BY 1,
         2,
         3,
         4
;


