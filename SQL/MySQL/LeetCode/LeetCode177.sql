-- 177. Nth Highest Salary
-- If there is no nth highest salary, then the query should return null.
-- 查找工资第N高的工资

-- 方法1:使用Limit语法,配合函数中声明变量(最优解)
-- PS:在function中,如果Return的结果为空,则默认会返回null
-- PS:此方法计算的是Dense Rank
CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT 
BEGIN
    DECLARE x INT;
    SET x=N-1; -- 由于Limit子句中不支持运算表达式,因此需要提前声明变量进行计算,正好复习一下函数的变量声明和赋值语法.
    RETURN (
        -- Write your MySQL query statement below.
        SELECT DISTINCT Salary -- 去除相同工资
        FROM Employee
        ORDER BY Salary DESC
        LIMIT x,1
    );
END

-- 方法2:使用相关子查询(效率较低)
-- PS:此方法计算的是Dense Rank
SELECT T1.Salary
FROM (
    SELECT DISTINCT Salary
    FROM Employee
)AS T1
WHERE (
    SELECT COUNT(1)
    FROM (
        SELECT DISTINCT Salary
        FROM Employee
    )AS T2
    WHERE T1.Salary<T2.Salary
) = N-1

-- 方法2.1:使用 CTE(Common Table Expression) 公共表表达式(效率较低)
WITH T1 AS (
    SELECT DISTINCT Salary
    FROM Employee
)
SELECT T2.Salary
FROM T1 AS T2
WHERE (
    SELECT COUNT(1)
    FROM T1
    WHERE T2.Salary < T1.Salary
)=N-1

-- 方法3:使用开窗函数 dense_rank
SELECT Salary
FROM(
    SELECT DENSE_RANK() OVER(ORDER BY Salary DESC) as d_rank,Salary
    FROM Employee
) AS T1
WHERE T1.d_rank = N
LIMIT 1