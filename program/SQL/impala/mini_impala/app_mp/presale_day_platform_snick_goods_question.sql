WITH x1 AS (
  SELECT *
  FROM dim.subcategory
  WHERE name != "无法识别"
),
x2 AS (
  SELECT _id,
    questions
  FROM dim.shop_question
),
t1 AS (
  SELECT DAY,
    platform,
    snick,
    shop_id AS snick_oid,
    plat_goods_id,
    question_type,
    question_id,
    num AS ask_count
  FROM app_mp.day_shop_question
  WHERE DAY = {{ macros.ds_format(macros.ds_add(ds,params.interval),"%Y-%m-%d","%Y%m%d") }}
),
t2 AS (
  SELECT t1.*,
    question,
    qid,
    subcategory_id
  FROM t1
    LEFT JOIN dim.question_b ON t1.question_id = dim.question_b._id
),
t3 AS (
  SELECT t2.*,
    name
  FROM t2
    LEFT JOIN x1 ON t2.subcategory_id = x1._id
),
t4 AS (
  SELECT *
  FROM t3
  WHERE question_type = 3
    OR (
      question_type = 1
      AND name IS NOT NULL
    )
),
t5 AS (
  SELECT t4.*,
    x2.questions AS s_q
  FROM t4
    LEFT JOIN x2 ON t4.question_id = x2._id
),
t6 AS (
  SELECT DAY,
    platform,
    snick,
    snick_oid,
    plat_goods_id,
    question_type,
    IF(question_type = 3, s_q, question) AS question,
    IF(question_type = 3, "自定义问题", name) AS subcategory_name,
    ask_count,
    subcategory_id,
    question_id
  FROM t5
)
INSERT overwrite app_mp.presale_day_platform_snick_goods_question partition(DAY)
SELECT "{{ macros.ds_format(macros.ds_add(ds,params.interval),"%Y-%m-%d","%Y%m%d") }}" AS stat_day,
  platform,
  snick,
  snick_oid,
  plat_goods_id,
  question_type,
  question_id,
  ask_count,
  question,
  subcategory_id,
  subcategory_name,
  {{ macros.ds_format(macros.ds_add(ds,params.interval),"%Y-%m-%d","%Y%m%d") }}
FROM t6