WITH x0 AS
  (SELECT platform,
          seller_nick,
          shop_id,
          `level`,
          count(1) AS warning_count,
          '2022-01-17 00:00:00' AS begin_time,
          '2022-01-23 23:59:59' AS end_time
   FROM xqc_ods.alert_all FINAL
   WHERE `day` >= 20220117
     AND `day` <= 20220123
     AND `time` >= '2022-01-17 00:00:00'
     AND `time` <= '2022-01-23 23:59:59'
     AND shop_id IN ('5bfe7a6a89bc4612f16586a5',
                     '5f1f97bdfbb9ba0017f73f18',
                     '5f74643b6868e200013e6d46',
                     '5f8ff0c0a3967d00188dca48',
                     '613ef1e1ec7097000e494123',
                     '61c16f73e8e6fc3cd46906a4')
     AND is_finished = 'False'
     AND `level` IN (1)
   GROUP BY platform,
            seller_nick,
            shop_id,
            `level`),
     x1 AS
  (SELECT shop_id,
          `level`,
          resp_code
   FROM xqc_ods.alert_remind_all
   WHERE `day` >= 20220124
     AND `day` <= 20220124
     AND `time` >= '2022-01-24 09:00:00'
     AND `time` <= '2022-01-24 15:48:44'
     AND shop_id IN ('5bfe7a6a89bc4612f16586a5',
                     '5f1f97bdfbb9ba0017f73f18',
                     '5f74643b6868e200013e6d46',
                     '5f8ff0c0a3967d00188dca48',
                     '613ef1e1ec7097000e494123',
                     '61c16f73e8e6fc3cd46906a4')
     AND notify_type = 2),
     x2 AS
  (SELECT shop_id,
          `level`
   FROM x1
   GROUP BY shop_id,
            `level`),
     x3 AS
  (SELECT shop_id,
          `level`,
          count(1) AS success
   FROM x1
   WHERE resp_code = 0
   GROUP BY shop_id,
            `level`),
     x4 AS
  (SELECT shop_id,
          `level`,
          count(1) AS fail
   FROM x1
   WHERE resp_code != 0
   GROUP BY shop_id,
            `level`),
     x5 AS
  (SELECT x2.shop_id AS shop_id,
          x2.`level` AS `level`,
          success,
          fail
   FROM x2 GLOBAL
   LEFT JOIN x3 ON x2.shop_id = x3.shop_id
   AND x2.`level` = x3.`level` GLOBAL
   LEFT JOIN x4 ON x2.shop_id = x4.shop_id
   AND x2.`level` = x4.`level`),
     x6 AS
  (SELECT platform,
          seller_nick,
          shop_id,
          `level`,
          warning_count,
          begin_time,
          end_time,
          success,
          fail
   FROM x0 GLOBAL
   LEFT JOIN x5 ON x0.shop_id = x5.shop_id
   AND x0.`level` = x5.`level`)
SELECT x6.*,
       shop_info.department_name AS shop_name
FROM x6 GLOBAL
LEFT JOIN xqc_dim.group_all AS shop_info ON x6.shop_id = shop_info.department_id