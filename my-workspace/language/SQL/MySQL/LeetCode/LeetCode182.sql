-- 182. Duplicate Emails
/* 
Write a SQL query to find all duplicate emails in a table named Person.
+----+---------+
| Id | Email   |
+----+---------+
| 1  | a@b.com |
| 2  | c@d.com |
| 3  | a@b.com |
+----+---------+
For example, your query should return the following for the above table:
+---------+
| Email   |
+---------+
| a@b.com |
+---------+
 */


-- 方法1:使用Group By+Having 实现分组过滤
SELECT Email FROM Person
GROUP BY Email HAVING COUNT(Id)>1;

-- 方法2:相关子查询+标量子查询+DISTINCT(不推荐)
SELECT DISTINCT T1.Email
FROM Person AS T1
WHERE (
    SELECT COUNT(1)
    FROM Person AS T2
    WHERE T2.Email = T1.Email
)>1;