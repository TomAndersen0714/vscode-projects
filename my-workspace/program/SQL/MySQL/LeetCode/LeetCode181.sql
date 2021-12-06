-- 181. Employees Earning More Than Their Managers
/* 
The Employee table holds all employees including their managers. 
Every employee has an Id, and there is also a column for the manager Id.
Given the Employee table, write a SQL query that finds out employees who earn more than their managers. 
For the above table, Joe is the only employee who earns more than his manager.
 */
-- 方法1: Join连接查询
SELECT T1.Name AS Employee
FROM Employee T1 JOIN Employee T2 ON T1.ManagerId = T2.Id
WHERE T1.Salary > T2.Salary;