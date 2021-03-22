-- 表1数据
/*
用户姓名,登录日期,登录次数
u01	2017/1/21	5
u02	2017/1/23	6
u03	2017/1/22	8
u04	2017/1/20	3
u01	2017/1/23	6
u01	2017/2/21	8
u02	2017/1/23	6
u01	2017/2/22	4
*/
-- 创建表
DROP TABLE IF EXISTS action;
CREATE EXTERNAL TABLE IF NOT EXISTS action(id STRING,visitDate STRING,visitCount INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '/tmp/hive/data/action.txt' INTO TABLE action;

-- 题1:查询每个用户每月的登录次数以及当前累计登录总数(日期采用yyyy-MM-dd的格式)

-- 第一步:将所有的登录记录转换成按月统计
select id,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') as visitDate,visitCount from action;
-- 第二步:基于结果表t1,将所有访问次数按月分组统计总数
select id,visitDate,sum(visitCount) as monthVisitCount
from (
    select 
    id,
    date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') as visitDate,
    visitCount 
    from action
) as t1
group by id,visitDate;
-- 第三步:基于结果表t2,按照用户分组,然后按照时间排序,统计目前为止的登录总数
select
    id,
    visit_date,
    month_visit_count,
    sum(month_visit_count) over(partition by id order by visit_date) as current_visit_Count
from (
    select id,visitDate as visit_date,sum(visitCount) as month_visit_count
    from (
        select 
        id,
        date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') as visitDate,
        visitCount 
        from action
    ) as t1
    group by id,visitDate
) as t2;

-- 或
select distinct
    id,
    from_unixtime(unix_timestamp(visitDate,'yyyy/MM/dd'),'yyyy-MM') as visit_date,
    -- 按用户按月统计该用户当月总访问次数
    sum(visitCount) over(partition by id,substring(visitDate,1,6)) as month_visit_count,
    -- 按用户按月排序统计该用户目前总访问次数
    sum(visitCount) over(partition by id order by substring(visitDate,1,6)) as current_visit_Count
from
    action
order by id,visit_Date;


-- 表2数据
/*
用户id,用户访问的店铺名
u1	a
u2	b
u1	b
u1	a
u3	c
u4	b
u1	a
u2	c
u5	b
u4	b
u6	c
u2	c
u1	b
u2	a
u2	a
u3	a
u5	a
u5	a
u5	a
*/

-- 创建表
DROP TABLE IF EXISTS visit;
CREATE TABLE IF NOT EXISTS visit(id STRING,store_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '/tmp/hive/data/visit.txt' INTO TABLE visit;

-- 题1:统计每个店铺的UV(unique visitor)(访客数量).输出店铺名,访客数量,访客集合
select
    store_name,
    count(distinct id) as UV,
    collect_set(id) as users
from visit
group by store_name;
-- 或使用Group by代替Distinct进行优化
-- 第一步:数据去重
select id,store_name from action group by id,store_name;
-- 第二步:统计不同店铺的UV
select 
    store_name,
    count(id) as UV,
    collect_list(id) as visitors
from (
    select id,store_name from visit group by id,store_name
) as t1
group by t1.store_name;

-- 题2:查询每个店铺访问次数Top3的访客信息.输出店名,访客ID,访问次数.
-- 第一步:统计各个店铺各个用户的访问次数,并计算其在对应店铺中其排名.
-- 输出店名,访客ID,访问次数,排名
select 
    store_name,
    id,
    count(1) as visitCount,
    rank() over(partition by store_name order by count(1) desc) as rank
from visit 
group by store_name,id;

-- 第二步:基于第一步的查询结果,查询排名小于等于3的访客信息.
-- 输出店名,访客ID,访问次数,排名
select
    store_name,
    id,
    visitCount,
    rank
from(
    select 
        store_name,
        id,
        count(1) as visitCount,
        rank() over(partition by store_name order by count(1) desc) as rank
    from visit 
    group by store_name,id
) as a
where a.rank<=3;
-- 或
-- 第一步:统计各个店铺各个用户的访问次数
-- 输出店名,访客ID,访问次数.
select
    store_name,
    id,
    count(1) as ct
from visit
group by store_name,id;

-- 第二步:按照店铺名和访问次数对t1中的记录进行降序排序,并行记录计算其在对应店铺中的排名
select
    store_name, id, ct,
    rank() over(partition by store_name order by ct desc) as rank
from (
    select
        store_name,
        id,
        count(1) as ct
    from visit
    group by store_name,id
)as t1;

-- 第三步:查询t2表中,rank值小于等于3的记录
select
    store_name, id, ct, rank
from (
    select
        store_name, id, ct,
        rank() over(partition by store_name order by ct desc) as rank
    from (
        select
            store_name,
            id,
            count(1) as ct
        from visit
        group by store_name,id
    )as t1
) as t2
where t2.rank<=3;
