-- mini_impala: dim.question_b
DROP TABLE IF EXISTS dim.question_b;
CREATE TABLE IF NOT EXISTS dim.question_b(
    _id STRING,
    is_dynamic BOOLEAN,
    answers STRING,
    qid STRING,
    tags STRING,
    is_editable BOOLEAN,
    question STRING,
    is_transfer BOOLEAN,
    create_time STRING,
    subcategory_id STRING,
    is_dynamicable BOOLEAN,
    update_time STRING,
    replies STRING,
    auto_send_in_auto_mode BOOLEAN
) STORED AS PARQUET;
-- mini_impala: dim.subcategory
DROP TABLE IF EXISTS dim.subcategory;
CREATE TABLE IF NOT EXISTS dim.subcategory(
    _id STRING,
    scid STRING,
    create_time STRING,
    `name` STRING
) STORED AS PARQUET;
-- mini_impala: dim.shop_questions
DROP TABLE IF EXISTS dim.shop_question;
CREATE TABLE IF NOT EXISTS dim.shop_question (
    _id STRING,
    question STRING,
    is_keyword BOOLEAN,
    answers STRING,
    shop_id STRING,
    is_enabled BOOLEAN,
    is_transfer BOOLEAN,
    create_time STRING,
    update_time STRING,
    question_status STRING,
    source STRING,
    questions STRING,
    replies STRING,
    version BIGINT
) STORED AS PARQUET;
-- mini_impala: app_mp.day_shop_question
DROP TABLE IF EXISTS app_mp.day_shop_question;
CREATE TABLE IF NOT EXISTS app_mp.day_shop_question (
    stat_day STRING,
    platform STRING,
    shop_id STRING,
    snick STRING,
    plat_goods_id STRING,
    question_type INT,
    question_id STRING,
    num BIGINT,
    origin_qid STRING
) PARTITIONED BY (year INT, month INT, day INT) STORED AS PARQUET -- mini_impala: app_mp.presale_day_platform_snick_goods_question
DROP TABLE IF EXISTS app_mp.presale_day_platform_snick_goods_question;
CREATE TABLE IF NOT EXISTS app_mp.presale_day_platform_snick_goods_question (
    stat_day STRING,
    platform STRING,
    snick STRING,
    snick_oid STRING,
    plat_goods_id STRING,
    question_type BIGINT,
    question_id STRING,
    ask_count BIGINT,
    question STRING,
    subcategory_id STRING,
    subcategory_name STRING
) PARTITIONED BY (day INT) STORED AS PARQUET;
-- mini_impala: app_mp.presale_day_platform_snick_goods
DROP TABLE IF EXISTS app_mp.presale_day_platform_snick_goods;
CREATE TABLE IF NOT EXISTS app_mp.presale_day_platform_snick_goods (
    stat_day STRING,
    platform STRING,
    snick STRING,
    snick_oid STRING,
    plat_goods_id STRING,
    pv BIGINT,
    reception_uv BIGINT,
    paid_uv BIGINT,
    payment DOUBLE
) PARTITIONED BY (day INT) STORED AS PARQUET;

-- imapal: app_mp.pdd_goods_question_stat
DROP TABLE IF EXISTS tmp.pdd_goods_question_stat
CREATE TABLE IF NOT EXISTS app_mp.pdd_goods_question_stat (
    name STRING,
    snick STRING,
    plat_goods_id STRING,
    plat_goods_name STRING,
    cnt BIGINT
) PARTITIONED BY (day INT) STORED AS PARQUET LOCATION 'hdfs://zjk-bigdata002:8020/user/hive/warehouse/app_mp.db/pdd_goods_question_stat'