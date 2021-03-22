-- 用于练习impala-hdfs操作

-- 建立HDFS Parquet内部表
DROP TABLE IF EXISTS tmp.question_keyword_origin
CREATE TABLE IF NOT EXISTS tmp.question_keyword_origin(
    id INT,
    info STRING
)
STORED AS PARQUET;

-- 向HDFS内部表中插入数据
-- 使用查询结果插入
INSERT INTO tmp.question_keyword_origin
WITH t1 AS (
    SELECT 1,'Tom'
)
SELECT * FROM t1

-- 使用显式插入
INSERT INTO tmp.question_keyword_origin
VALUES(2,'Alise')
-- PS: 在对HDFS/Hive表中插入行记录时每次插入都会生成一个tmp小文件,插入次数越多,小文件越多
-- 很显然频繁的Insert会对HDFS NameNode造成巨大内存负担,对应的解决方案有:
-- 1.使用查询结果插入,即一次性插入多条数据(分区表可以使用动态分区,而不指定分区)
-- 2.直接LOAD文件到表路径下(分区表需要指定分区)

