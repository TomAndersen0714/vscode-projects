-- JD查询聊天日志手动计算问题pv(Impala)
SELECT platform,
    xd_shop_nick AS snick,
    shop_oid AS snick_oid,
    question_type,
    question_oid AS question_id,
    qid AS qoid,
    count(1) AS pv,
    count(DISTINCT cnick) AS cuv
FROM dwd.xdrs_logs
WHERE day = 20210815
    AND shop_oid = '60f91df16124ce0016b5ebbf'
    AND act = 'recv_msg'
    AND strleft(cnick, 10) != 'comxiaoduo'
GROUP BY 1,2,3,4,5,6
ORDER BY pv DESC
-- JD查询聊天日志手动计算问题pv(Impala)
SELECT 
    question_b_qid,
    COUNT(1) AS pv
FROM ods.jd_xdrs_logs
WHERE year=2021 AND month=202108 AND day = 20210815
AND shop_id = '60f91df16124ce0016b5ebbf'
AND act = 'recv_msg'
GROUP BY question_b_qid
ORDER BY pv DESC

-- JD查询前一天接收问题的聊天日志(tmp.xdrs_logs_receive)手动计算问题pv(Impala)
SELECT
    qid,
    COUNT(1) AS pv
FROM tmp.xdrs_logs_receive
WHERE shop_oid = '60f91df16124ce0016b5ebbf'
GROUP BY qid
ORDER BY pv DESC

-- JD查询聊天日志手动计算问题pv(ClickHouse)
SELECT 
    question_b_qid,
    COUNT(1) AS pv
FROM ods.xdrs_logs_all
WHERE day = 20210815
AND shop_id = '60f91df16124ce0016b5ebbf'
AND act = 'recv_msg'
GROUP BY question_b_qid
ORDER BY pv DESC
-- 加上问题的中文描述
SELECT
    t1.*,
    t2.question
FROM (
    SELECT 
        question_b_qid,
        COUNT(1) AS pv
    FROM ods.xdrs_logs_all
    WHERE day = 20210815
    AND shop_id = '60f91df16124ce0016b5ebbf'
    AND act = 'recv_msg'
    GROUP BY question_b_qid
) AS t1
LEFT JOIN (
    SELECT
        qid AS question_b_qid,
        question
   FROM ods.question_b
) AS t2
USING question_b_qid
ORDER BY pv DESC


-- JD数据统计-基础数据-接待数据-问题数据(Impala)
SELECT question_id,qoid,pv
FROM app_mp.stat_question_for_shop
WHERE snick_oid = '60f91df16124ce0016b5ebbf'
AND day=20210815
ORDER BY pv DESC

-- JD查询聊天日志(ClickHouse)
SELECT concat('****', '', substringUTF8(snick, lengthUTF8(snick) / 2)) AS `卖家昵称`,
       concat('****', '', substringUTF8(snick, lengthUTF8(cnick) / 2)) AS `买家昵称`,
       act AS `动作`,
       msg AS `消息内容`,
       remind_answer AS `机器人提示内容`,
       toString(toDateTime(cast(msg_time AS Int64), 'Asia/Shanghai')) AS `客户端时间`,
       create_time AS `创建时间`,
       plat_goods_id AS `当前商品`,
       question AS `问题分类`,
       question_b_qid AS `QID`,
       question_b_proba AS `问题分类概率`,
       current_sale_stage AS `销售阶段`,
       `mode` AS `模式`,
       msg_id AS `msgid`,
       '' AS `未回复原因`
FROM
  (SELECT replaceOne(snick, 'cnjd', '') AS snick,
          replaceOne(cnick, 'cnjd', '') AS cnick,
          act,
          msg,
          remind_answer,
          msg_time,
          plat_goods_id,
          question_b_qid,
          question_b_proba,
          current_sale_stage,
          `mode`,
          create_time,
          msg_id
   FROM ods.xdrs_logs
   WHERE `day` = 20200702
     AND snick = '飞科官方旗舰店:官旗售前4号'
     AND cnick = 'jd_4df9c9a9541cd' ) t1
LEFT JOIN
  (SELECT qid AS question_b_qid,
          question
   FROM ods.question_b) t2 USING question_b_qid
ORDER BY cnick,
         create_time ASC