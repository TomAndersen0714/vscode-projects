-- 1. 创建测试表
-- DROP TABLE tmp.test1 
CREATE TABLE tmp.test1 (
    snick STRING,
    cnick STRING,
    qid_stat STRING,
    big STRING,
    question STRING
) 
SORT BY (snick, cnick) 
STORED AS PARQUET 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/tmp.db/test1' 

-- 2. 查看其表属性
DESCRIBE EXTENDED tmp.test1;
-- 小结: 发现 SORT BY 在表属性(TBLPROPERTIES)中的变量名为 sort.columns

-- 3. 尝试修改表属性
ALTER TABLE tmp.test1 SET TBLPROPERTIES('sort.columns' = 'snick')
-- 小结: 证明此变量是可修改的

-- 4. 重建测试表, 测试此变量是否支持手动添加
DROP TABLE tmp.test1 
CREATE TABLE tmp.test1 (
    snick STRING,
    cnick STRING,
    qid_stat STRING,
    big STRING,
    question STRING
) 
STORED AS PARQUET 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/tmp.db/test1'
-- 手动修改变量
ALTER TABLE tmp.test1 SET TBLPROPERTIES('sort.columns' = 'snick')
-- 查看建表语句
SHOW CREATE TABLE tmp.test1
-- 小结: 说明 SORT BY 子句支持渐变之后创建和修改.
-- PS: 后续还需要使用实际聊天数据来测试性能.

-- 5. 索引性能测试
老淘宝中Impala的聊天日志数据落到OSS盘/opt/bigdata/,线下创建两个不同的表结构,即排序和不排序
排序表的创建要在原始表导入数据文件之后, 使用"CREATE TABLE AS"

-- 6. 测试用例
SELECT snick,
    cnick,
    msg,
    act,
    msg_time,
    create_time,
    question_b_qid,
    question_b_standard_q,
    plat_goods_id
FROM dwd.mini_xdrs_log
WHERE DAY >= 20211111
    AND DAY <= 20211111
    AND send_msg_from NOT IN ('0', '1')
    AND shop_id = "5cf0d72498ef41000f114a0f"
ORDER BY cnick,
    msg_time
limit 500000 -- trace:9d54519d4bc7676fe15756f22189759b
-- PS: 通过CDH Impala UI对比两者的性能