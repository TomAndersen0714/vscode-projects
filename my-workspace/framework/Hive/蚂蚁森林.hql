-- 背景说明
/*
表user_low_carbon:
    记录了用户每天的蚂蚁森林低碳生活领取的记录流水,累计减少的碳排放量
    可以用于兑换环保植物,为环境保护贡献一份力量.
字段名:
    user_id,data_dt,low_carbon
字段解释:
    用户名,日期,当前减少的碳排放量
表user_low_carbon数据示例:
u_001	2017/1/1	10
u_001	2017/1/2	150
u_001	2017/1/2	110
建表hql:
drop table if exists user_low_carbon;
create table if not exists user_low_carbon(user_id STRING,data_dt STRING,low_carbon int)
row format delimited
fields terminated by '\t';
load data local inpath '/tmp/hive/data/user_low_carbon.txt' into table user_low_carbon;

表plant_carbon:
    记录了各种环保植物换取时所需减少的碳排放量.
字段名:
    plant_id, plant_name, low_carbon
字段解释:
    植物编号,植物名,换购植物所需碳排放量
表plant_carbon数据示例:
p001	梭梭树	17
p002	沙柳	19
p003	樟子树	146
p004	胡杨	215
建表hql:
drop table if exists plant_carbon;
create table if not exists plant_carbon(plant_id STRING,plant_name STRING,low_carbon int)
row format delimited
fields terminated by '\t';
load data local inpath '/tmp/hive/data/plant_carbon.txt' into table plant_carbon;
*/


---- 题目
题1:假设2017年1月1日开始记录低碳数据(user_low_carbon),2017年10月1日之前满足
申领条件的用户全都申领了一颗"p004-胡杨",剩余的能量全部用来领取"p002-沙柳".
统计在10月1日累计申领"p002-沙柳" 排名前10的用户信息；以及他比后一名多领了几颗沙柳.

-- 第一步:统计2017/10/01时各个用户节省的低碳总量,生成结果表t1
SELECT user_id, sum(low_carbon) AS sum_carbon
FROM user_low_carbon
WHERE to_date(regexp_replace(data_dt,'/','-'))<'2017-10-01'
GROUP BY user_id; t1
-- 查询兑换"p004-胡杨"所需碳排放量,生成t2
SELECT low_carbon FROM plant_carbon WHERE plant_id='p004'; t2
-- 查询兑换"p002-沙柳"所需碳排放量,生成t3
SELECT low_carbon FROM plant_carbon WHERE plant_id='p002'; t3

-- 第二步:基于t1,t2,t3,计算所能够领取的"p002-沙柳"数量plant_count
-- 并将结果按照plant_count进行降序排序,取前11名,生成结果表t4,便于作差
SELECT 
    user_id, 
    floor(if(sum_carbon>=t2.low_carbon,sum_carbon-t2.low_carbon,sum_carbon)/t3.low_carbon) AS plant_count
FROM (
    SELECT user_id, sum(low_carbon) AS sum_carbon
    FROM user_low_carbon
    WHERE to_date(regexp_replace(data_dt,'/','-'))<'2017-10-01'
    GROUP BY user_id
    ) AS t1,
    (SELECT low_carbon FROM plant_carbon WHERE plant_id='p004') AS t2,
    (SELECT low_carbon FROM plant_carbon WHERE plant_id='p002') AS t3
ORDER BY plant_count DESC
LIMIT 11;
-- 第三步:按照plant_count给t4中的记录进行排序,取前10位显示,并计算其比后一位多兑换的植物数量,生成t5
SELECT 
    user_id, 
    plant_count,
    plant_count - lead(plant_count,1,plant_count) over(order by plant_count DESC) AS more_plant_count
FROM (
    SELECT 
        user_id, 
        floor(if(sum_carbon>=t2.low_carbon,sum_carbon-t2.low_carbon,sum_carbon)/t3.low_carbon) AS plant_count
    FROM (
        SELECT user_id, sum(low_carbon) AS sum_carbon
        FROM user_low_carbon
        WHERE to_date(regexp_replace(data_dt,'/','-'))<'2017-10-01'
        GROUP BY user_id
        ) AS t1,
        (SELECT low_carbon FROM plant_carbon WHERE plant_id='p004') AS t2,
        (SELECT low_carbon FROM plant_carbon WHERE plant_id='p002') AS t3
    ORDER BY plant_count DESC
    LIMIT 11
    ) AS t4
LIMIT 10;

题2:查询user_low_carbon表中每日流水记录,条件为:
用户在2017年,连续三天（或以上）的时间里,每天减少碳排放（low_carbon）都超过100g的用户低碳流水.
例如用户u_002符合条件的记录如下,因为2017/1/2~2017/1/5连续四天每天的碳排放量之和都大于等于100g:
user_id data_dt low_carbon
u_002  2017/1/2  150
u_002  2017/1/2  70
u_002  2017/1/3  30
u_002  2017/1/3  80
u_002  2017/1/4  150
u_002  2017/1/5  101
-- 第一步:查询表 user_low_carbon,统计各个用户2017年单日减少碳排放量,分组并过滤大于等于100g的记录
-- 输出:user_id,data_dt,生成t1
-- PS:因为后续查询不需要使用每日的减少的总碳排放量,因此只需要用于过滤,不需要显示在结果表中.
SELECT
    user_id,data_dt
FROM
    user_low_carbon
WHERE
    substring(data_dt,1,4) = '2017'
GROUP BY
    user_id,data_dt
HAVING
    sum(low_carbon) >=100;
-- 第二步:查询t1,根据 user_id 进行分区,根据data_dt进行降序排序,计算data_dt的本年天数与
-- row_number()的差值 diff.生成t2
-- 在同一个 user_id 中,diff 相同的记录的 data_dt 属于同一个连续时间段.
-- 因为row_numer()的变化间隔为1,如果data_dt天数变化间隔为1,则连续日期记录中,
-- 两者的差值 diff 必定相同.如:
/*
date    row_number    diff
2017/01/02  1   2017/01/01
2017/01/03  2   2017/01/01
2017/01/04  3   2017/01/01
2017/01/06  4   2017/01/02
*/
SELECT
    user_id,data_dt,
    date_sub(to_date(regexp_replace(data_dt,'/','-')),
        row_number() over(partition by user_id order by data_dt)) AS diff
FROM (
    SELECT
        user_id,data_dt
    FROM
        user_low_carbon
    WHERE
        substring(data_dt,1,4) = '2017'
    GROUP BY
        user_id,data_dt
    HAVING
        sum(low_carbon) >=100
) AS t1;
-- 第三步:基于t2的结果对于所有的记录使用Group By按照 user_id,diff 进行分组,计算连续出现的
-- 记录个数,以及连续记录的起止时间,生成表t3
SELECT
    user_id,
    min(data_dt) AS begin_date,
    max(data_dt) AS end_date,
    count(1) AS count
FROM (
    SELECT
        user_id,data_dt,
        date_sub(to_date(regexp_replace(data_dt,'/','-')),
            row_number() over(partition by user_id order by data_dt)) AS diff
    FROM (
        SELECT
            user_id,data_dt
        FROM
            user_low_carbon
        WHERE
            substring(data_dt,1,4) = '2017'
        GROUP BY
            user_id,data_dt
        HAVING
            sum(low_carbon) >=100
    ) AS t1
) AS t2
GROUP BY user_id,diff
HAVING count>=3;
-- 第四步:根据t3的结果,对原始表进行连接查询,t3为小表在左as a,原始表为大表在右as b
-- 连接条件为a.user_id=b.user_id
-- 过滤条件为 b.data_dt>=a.begin_date and b.data_dt<=a.end_date
-- 查询列表为b中的所有字段
SELECT
    b.user_id,
    b.data_dt,
    b.low_carbon
FROM (
    SELECT
        user_id,
        min(data_dt) AS begin_date,
        max(data_dt) AS end_date,
        count(1) AS count
    FROM (
        SELECT
            user_id,data_dt,
            date_sub(to_date(regexp_replace(data_dt,'/','-')),
                row_number() over(partition by user_id order by data_dt)) AS diff
        FROM (
            SELECT
                user_id,data_dt
            FROM
                user_low_carbon
            WHERE
                substring(data_dt,1,4) = '2017'
            GROUP BY
                user_id,data_dt
            HAVING
                sum(low_carbon) >=100
        ) AS t1
    ) AS t2
    GROUP BY user_id,diff
    HAVING count>=3
) AS a
INNER JOIN user_low_carbon AS b
ON a.user_id = b.user_id
WHERE b.data_dt >= a.begin_date AND b.data_dt<=a.end_date
ORDER BY user_id,data_dt;

