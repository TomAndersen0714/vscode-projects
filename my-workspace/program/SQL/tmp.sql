WITH con AS
  (SELECT DISTINCT contract_no,
                   contract_value,
                   webapp_value,
                   is_had_webapp
   FROM dws.crm_shop_contract
   WHERE day_start >= trunc(now(),'year')
     AND record_type = '{{ record_type=销售合同 }}'
     AND is_close = 'False'),
     re AS
  (SELECT round(count(DISTINCT if(is_had_webapp='是', contract_no, NULL))) AS '签约合同数',
          round(count(DISTINCT if(webapp_value>0, contract_no, NULL)),2) AS '付费合同数',
          round(sum(webapp_value),2) AS '付费合同金额'
   FROM con)
SELECT *,
       round(`付费合同金额`*`签约合同数`/`付费合同数`,2) AS '潜在收入总额'
FROM re

-- WITH t1 AS
--   (SELECT app_crm.ods_fxxk_customer.name,
--           app_crm.ods_fxxk_order.approval_time AS DAY,
--           to_date(app_crm.ods_fxxk_order.sign_date) AS sign_date,
--           app_crm.ods_fxxk_order.record_type,
--           round(app_crm.ods_fxxk_order.contract_value,2) AS contract_value,
--           round(app_crm.ods_fxxk_order.contract_value-app_crm.ods_fxxk_order.fishpond_value-app_crm.ods_fxxk_order.webapp_value,2) AS xiaoduo_value,
--           app_crm.ods_fxxk_order.xiaoduo,
--           app_crm.ods_fxxk_order.contract_type,
--           app_crm.ods_fxxk_order.fishpond_value,
--           app_crm.ods_fxxk_order.webapp_value,
--           app_crm.ods_fxxk_order.is_had_webapp,
--           app_crm.ods_fxxk_order.is_had_fishpond
--    FROM app_crm.ods_fxxk_order
--    LEFT JOIN app_crm.ods_fxxk_customer ON app_crm.ods_fxxk_customer._id = app_crm.ods_fxxk_order.customer_name
--    WHERE app_crm.ods_fxxk_customer.name != '蔡赛华03'
--      AND app_crm.ods_fxxk_customer.name NOT LIKE '%测试%'
--      AND app_crm.ods_fxxk_order.record_type = '销售合同'
--      AND app_crm.ods_fxxk_order.life_status NOT IN ('未生效',
--                                                     '作废')
--      AND app_crm.ods_fxxk_order.is_refund = '否'
--      AND is_close = 'False'),
--      t2 AS
--   (SELECT year(DAY) AS YEAR,
--           month(DAY) AS MONTH,
--           count(2) AS paid_contract_num,
--           sum(webapp_value) AS webapp_value
--    FROM t1
--    WHERE DAY >= '2020-01-01'
--      AND webapp_value != 0
--      AND webapp_value IS NOT NULL
--    GROUP BY 1,
--             2
--    UNION ALL SELECT year(sign_date) AS YEAR,
--                     month(sign_date) AS MONTH,
--                     count(2) AS paid_contract_num,
--                     sum(webapp_value) AS webapp_value
--    FROM t1
--    WHERE DAY < '2020-01-01'
--      AND sign_date < '2020-01-01'
--     --  AND webapp_value != 0
--     AND webapp_value >= 100
--      AND webapp_value IS NOT NULL
--    GROUP BY 1,
--             2),
--      t3 AS
--   (SELECT year(DAY) AS YEAR,
--           month(DAY) AS MONTH,
--           count(2) AS sign_contract_num
--    FROM t1
--    WHERE DAY >= '2020-01-01'
--      AND is_had_webapp = '是'
--    GROUP BY 1,
--             2
--    UNION ALL SELECT year(sign_date) AS YEAR,
--                     month(sign_date) AS MONTH,
--                     count(2) AS sign_contract_num
--    FROM t1
--    WHERE DAY < '2020-01-01'
--      AND sign_date < '2020-01-01'
--      AND is_had_webapp = '是'
--    GROUP BY 1,
--             2),
--      t4 AS
--   (SELECT t3.year AS '年份',
--           t3.month AS '月份',
--           concat(concat(cast(t3.year AS string),'-'),cast(t3.month AS string)) AS '年月',
--           rank() OVER (
--                        ORDER BY t3.year DESC,t3.month DESC) AS rankline,
--                       if(t2.webapp_value IS NULL,0,t2.webapp_value) AS '付费合同金额',
--                       t3.sign_contract_num AS '签约合同数',
--                       if(t2.paid_contract_num IS NULL,0,t2.paid_contract_num) AS '付费合同数'
--    FROM t3
--    LEFT JOIN t2 ON t3.year = t2.year
--    AND t3.month = t2.month
--    WHERE t3.year != 2018)
-- SELECT sum(`付费合同金额`) AS 'payment',
--        sum(`签约合同数`) AS `contract_uv`,
--        sum(`付费合同数`) AS 'paid_uv',
--        sum(`付费合同金额`) * sum(`签约合同数`) / sum(`付费合同数`) AS `upper_bound_payment`
-- FROM t4
-- WHERE rankline <= 13
--   AND rankline >= 2