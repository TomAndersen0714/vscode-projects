-- 如果表存在,则删除表
DROP TABLE IF EXISTS ods_start_log;
-- 创建外部表,字段名为line,类型为string
CREATE EXTERNAL TABLE ods_start_log(line string)
-- 将表按照日期进行分区,日期字段名为dt,类型为string
PARTITIONED BY('dt' string)


CREATE EXTERNAL TABLE IF NOT EXISTS kudu_test_tbl
STORED AS KUDU
TBLPROPERTIES('kudu.table_name'='kudu_test_table');



-- Impala不支持Analytic Function中使用DISTINCT,可以使用WITH clause代替
-- 'main_acount'指的是晓多客服主账号,一个主账号可以有多个子账号,web_log记录的是
-- 这些客服账号访问晓多后台管理网站的记录.'distinct_id'认为是子账号名.
-- 'category_id'认为是主账号/店铺类型
SELECT
    vtbl.category_id,
    vtbl.create_time_hour,
    COUNT(distinct_id) AS PV,
    COUNT(DISTINCT distinct_id) AS UV
FROM
(
    WITH t1 AS (
        SELECT
            distinct_id,
            SUBSTR(create_time,1,13) AS create_time_hour,
            SPLIT_PART(distinct_id,':',1) AS m_count
        FROM ods.web_log
    )
    SELECT t1.distinct_id,t1.create_time_hour,t1.m_count,t2.category_id
    FROM t1
    LEFT JOIN dim.practice_category AS t2
    ON t2.main_acount = t1.m_count
    
) AS vtbl
GROUP BY category_id,create_time_hour
ORDER BY category_id,create_time_hour





