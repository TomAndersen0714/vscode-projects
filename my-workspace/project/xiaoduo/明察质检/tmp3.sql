-- 消费者需求分布 饼图
SELECT name,
       CASE
           WHEN '{{ count_type=count_by_cnick }}'='count_by_cnick' THEN groupBitmapOr(cnick_id_bitmap)
           WHEN '{{ count_type=count_by_cnick }}'='count_by_dialog' THEN sum(dialog_sum)
       END AS COUNT
FROM
  (SELECT cnick_id_bitmap,
          day,
          question_id,
          dialog_sum
   FROM dws.voc_goods_question_stat_all
   WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=month_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
     AND platform = 'tb'
     AND question_id GLOBAL IN
       (SELECT DISTINCT qid AS question_id
        FROM dim.question_b_v2_all
        WHERE third_category_id IN ['6045c92df9fbadde8fcca533','616932c3980a37bdebf19a6e','60785d962648234a2b7bc9dc'] )-- 下拉店铺
AND ('{{ shop_ids }}'=''
     OR shop_id IN splitByChar(',','{{ shop_ids }}') )-- 下拉订单状态
AND ('{{ order_status }}'=''
     OR recent_order_status IN splitByChar(',',replaceAll('{{ order_status }}', 'unorder', '')) )-- 下拉会话轮次
AND ('{{ round_count }}'=''
     OR toString(dialog_qa_stage) IN splitByChar(',','{{ round_count }}') )-- 当前企业对应的店铺
AND shop_id GLOBAL IN
       (SELECT DISTINCT shop_id
        FROM xqc_dim.xqc_shop_all
        WHERE day = toYYYYMMDD(yesterday())
          AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
          AND platform = 'tb' )-- 当前企业对应的子账号
AND snick GLOBAL IN
       (SELECT DISTINCT snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
          AND platform = 'tb'
          AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}' -- 下拉框-子账号分组id

          AND ('{{ department_ids }}'=''
               OR department_id IN splitByChar(',','{{ department_ids }}') )-- 下拉子账号

          AND ('{{ snicks }}'=''
               OR snick IN splitByChar(',','{{ snicks }}')) ) ) AS voc_question_info -- 获取上级分类
GLOBAL
LEFT JOIN
  (SELECT qid,
          question,
          name,
          fourth_category_id
   FROM
     (SELECT qid,
             question,
             fourth_category_id
      FROM dim.question_b_v2_all
      WHERE fourth_category_id!=''
        AND third_category_id IN ['6045c92df9fbadde8fcca533','616932c3980a37bdebf19a6e','60785d962648234a2b7bc9dc']) AS question_info GLOBAL
   LEFT JOIN dim.fourth_category_all AS fourth_category ON question_info.fourth_category_id = fourth_category._id)AS qid_info ON voc_question_info.question_id=qid_info.qid
WHERE name!=''
GROUP BY name
ORDER BY COUNT DESC