-- 180. Consecutive Numbers
/*
    Write an SQL query to find all numbers that appear at least 3 times consecutively. 
    Return the result table in any order.
*/

-- 方法1:使用Where实现自连接(Self Join)查询
-- PS:此方法的空间复杂度为O(n*3)
-- PS:此方法不支持扩展
SELECT
    DISTINCT T1.Num AS ConsecutiveNums
FROM 
    Logs AS T1,
    Logs AS T2,
    Logs AS T3
WHERE
    T1.Id = T2.Id+1 AND T2.Id = T3.Id+1 -- The id must be continuous
    AND
    T1.Num = T2.Num AND T2.Num = T3.Num -- The continuous num must be same at least three times

-- 方法2:使用开窗函数 Lag() Lead() 获得指定列之后和之前的值(默认为上下第一行),代替自连接(Self Join)
-- PS:此方法不支持扩展
SELECT
    DISTINCT Num AS ConsecutiveNums
FROM(
    SELECT
        Num,
        LAG(Num) OVER(ORDER BY Id) AS previous_num, -- Get the previous num
        LEAD(Num) OVER(ORDER BY Id) AS next_num -- Get the next num
    FROM
        Logs
) AS T1
WHERE
    T1.Num = T1.previous_num AND T1.Num = T1.next_num;

-- 方法2.1:使用开窗函数 Lag() 计算Num列当前行与下一行的差值diff,SUM()计算Num列的当前行及之前Num列所有行之和
-- 当相同的Num连续出现3次时,diff会先出现一个非零值,然后紧跟2个连续0,因此通过Sum()计算时,连续相同的Num对应的
-- 的Sum也是相同的,由于Num的差值不可预测,因此可能出现Sum不唯一的情况,在后续进行分组统计数量时,可能会产生误差
-- 为了避免这种情况的出现,就需要使用 CASE WHEN <exp> THEN <exp> ELSE <exp> END 表达式,来保证Sum是递增的,
-- 即唯一的.
-- PS:开窗函数的默认开窗范围为某一列的首行到当前行
-- PS:此方法支持扩展
/* select distinct Num as consecutiveNums
from (
    select 
        Num,
        sum(diff) over(order by Id) as flag 
    from (
            select 
                Id, 
                Num, 
                case when LAG(Num) over(order by Id)- Num = 0 then 0 else 1 end as diff
            from logs
        ) a
    ) b
group by Num,flag
having count(1) >=3 -- (could change 3 to any number) */
SELECT DISTINCT T2.Num AS ConsecutiveNums 
FROM (
    SELECT
        T1.Num,
        SUM(T1.Diff) OVER(ORDER BY Id) AS Flag
    FROM
    (
        SELECT
            Id,
            Num,
            CASE WHEN Lag(Num) OVER(ORDER BY Id)-Num = 0 THEN 0 ELSE 1 END AS Diff
        FROM
            Logs
    ) AS T1
)AS T2
GROUP BY Flag
Having COUNT(1) >2 -- (could change 3 to any number)

-- 方法3:使用开窗函数 row_number()
-- 此方法的关键点在于如果连续的Num相同,则其对应的Id
-- PS:row_number()的返回值类型为 BIGINT UNSIGNED,在进行计算时会导致隐式类型转换,如果计算结果出现负值,则会抛出
-- 错误 "BIGINT UNSIGNED value is out of range",因此在MySQL中进行计算时,需要将其结果转换成 signed 类型才能
-- 继续进行计算
-- PS:此方法支持扩展
select 
    distinct num as ConsecutiveNums 
from (
    select 
        num, 
        id-CAST(row_number() over(partition by num order by id) AS signed) AS flag
    from logs
    ) T1
group by 
    num,flag
having count(1) >2
/* 
select 
    distinct num as ConsecutiveNums 
from (
    select
        num,
        id-CAST(row_number() over(order by num, id) AS signed) AS flag 
    from logs
) a
group by 
    num,flag
having 
    count(1) >2
 */