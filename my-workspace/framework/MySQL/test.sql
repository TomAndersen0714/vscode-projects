-- 此脚本主要用于SQL练习
CREATE TABLE employees(
    `employee_id` int(6) NOT NULL auto_increment,
    -- '员工编号'
    `salary` double(10, 2) DEFAULT NULL,
    -- '月薪'
    `department_id` int(6) DEFAULT NULL,
    -- '部门ID'
    PRIMARY KEY(`employee_id`)
);
SELECT a.employee_id,
    a.salary,
    a.department_id,
    b.employee_id as r_employee_id,
    b.salary as r_salary
FROM employees a
    LEFT JOIN employees b ON a.department_id = b.department_id
    AND a.salary < b.salary;
SELECT department_id,
    count(r_employee_id) AS rank employee_id,
    salary
FROM (
        SELECT a.employee_id,
            a.salary,
            a.department_id,
            b.employee_id as r_employee_id,
            b.salary as r_salary
        FROM employees a
            LEFT JOIN employees b ON a.department_id = b.department_id
            AND a.salary < b.salary
    ) AS t1
GROUP BY t1.department_id,
    t1.employee_id
HAVING count(r_employee_id) <= 2;
SELECT deptno,
    empno,
    sal,
    rank() over(
        PARTITION BY deptno
        ORDER BY sal
    )
FROM emp;
SELECT *
FROM (
        SELECT deptno,
            empno,
            sal,
            rank() over(
                PARTITION BY deptno
                ORDER BY sal DESC
            ) AS rank
        FROM emp
    ) AS t1
WHERE t1.rank <= 3;
SELECT *
FROM (
        SELECT department_id,
            employee_id,
            salary,
            rank() over(
                partition by department_id
                order by salary DESC
            ) as rank
        FROM employees
    ) AS t1
WHERE t1.rank <= 3;
SELECT department_id,
    employee_id,
    salary
FROM employees t1
WHERE (
        SELECT count(t2.employee_id)
        FROM employees t2
        WHERE t1.department_id = t2.department_id
            AND t1.salary <= t2.salary
    );

-- Q1:
WITH t1 AS (
    SELECT distinct_id,
        create_time,
        SPLIT_PART(distinct_id, ':', 1) AS m_count
    FROM ods.web_log
)
LEFT JOIN dim.practice_category ON