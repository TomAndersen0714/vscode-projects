WITH t1 AS
  (SELECT shop_id,
          split_part(snick, ':', 1) as snick,
          platform,
          plat_goods_id,
          if(shop_question_id NOT in ("-", ""), 3, 1) AS question_type,
          if(shop_question_id NOT in ("-", ""), shop_question_id, CAST (CAST (cast(question_b_qid AS DOUBLE) AS int) AS string)) AS question_id
  FROM xd_data.chat_event
  WHERE DAY = ${var:param_day}
     AND platform = 'tb'
     AND strleft(cnick, 10) != 'comxiaoduo'
     AND act = 'recv_msg'
     AND plat_goods_id != ''
     AND regexp_replace(split_part(snick, ':', 1), "cntaobao", "") != regexp_replace(regexp_replace(split_part(cnick, ':', 1), "cntaobao", ""), "cnalichn", "")),
     t2 AS
  (SELECT shop_id,
          platform,
          snick,
          plat_goods_id,
          question_type,
          question_id,
          count(1) AS cnt
  FROM t1
  WHERE question_id != '0'
  GROUP BY shop_id,
            platform,
            snick,
            plat_goods_id,
            question_type,
            question_id),
     t5 AS
  (SELECT _id,
          qid
  FROM xd_data.question_b)

insert overwrite xd_stat.day_shop_question partition(year, month, day)
SELECT
     "${var:param_date}",
      platform,
      shop_id,
      snick,
      plat_goods_id,
      question_type,
      IF (question_type = 3,
          question_id,
          _id) AS question_id,
      cnt as num,
      question_id AS origin_qid,
      ${var:param_year},
      ${var:param_month},
      ${var:param_day}
FROM t2
LEFT JOIN t5 ON t2.question_id = t5.qid;

