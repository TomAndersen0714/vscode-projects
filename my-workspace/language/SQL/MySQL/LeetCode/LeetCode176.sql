-- 176. Second Highest Salary
-- If there is no second highest salary, then the query should return null.


-- 方法1:使用DISTINCT+LIMIT
-- PS:此方式对应的为dense rank,即数值相同时并不影响后续名次
SELECT (
    SELECT 
    DISTINCT Salary
    FROM Employee
    ORDER BY Salary DESC
    LIMIT 1,1
) AS SecondHighestSalary; -- 为了满足无数值则返回null的要求,在外面嵌套标量查询

/* SELECT(
    SELECT DISTINCT Salary
    FROM Employee 
    ORDER BY Salary DESC
    LIMIT 1 OFFSET 1
) As SecondHighestSalary; */

-- 方法1.1:使用IFNULL函数
-- PS:此方式对应的为dense rank,即数值相同时并不影响后续名次
-- PS:DISTINCT 关键字对应的算法十分耗时
SELECT IFNULL(
    (
    SELECT 
    DISTINCT Salary
    FROM Employee
    ORDER BY Salary DESC
    LIMIT 1,1
    ),
    NULL
)AS SecondHighestSalary;

-- 方法2:使用标量子查询
-- PS:此方式对应的为dense rank,即数值相同时并不影响后续名次
SELECT
    max(Salary) as SecondHighestSalary
FROM
    Employee
WHERE
    Salary <(
        SELECT MAX(Salary)
        FROM Employee
    );

-- 方法3:使用 DENSE_RANK() 开窗函数
-- PS:此方式对应的为dense rank,即数值相同时并不影响后续名次
SELECT (
    SELECT T1.Salary
    FROM(
        SELECT
            DENSE_RANK() OVER(ORDER BY Salary DESC) as d_rank,
            Salary
        FROM
            Employee
    )AS T1
    WHERE T1.d_rank=2
    LIMIT 1
)AS SecondHighestSalary;
