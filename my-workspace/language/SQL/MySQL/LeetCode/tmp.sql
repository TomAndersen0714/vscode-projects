/* 
mysql 5.7
table: id itemname approvaldate price limitdate
获取每个商品提交日期最新的一条,如果日期相同,则取截止日期最大的一条
*/

-- 方法1:使用开窗函数 row_number(存在重复查询)
SELECT *
FROM (
    SELECT
        *,
        row_numer() over(partition by itemname order by approvaldate,limitdate DESC) as row
    FROM table
)AS T1
WHERE T1.row = 1;

-- 方法2:使用WHERE相关子查询+order by(存在重复查询)
SELECT *
FROM table AS T1
WHERE T1.id = (
    SELECT DISTINCT id
    FROM table AS T2
    WHERE T2.itemname = T1.itemname -- 存在重复查询
    ORDER BY approvaldate,limitdate DESC
    LIMIT 1
)

-- 方法3:使用SELECT相关子查询+order by
SELECT *
FROM table
WHERE id IN (
    SELECT (
        SELECT id
        FROM table AS T2
        WHERE T2.itemname = T1.itemname
        ORDER BY approvaldate,limitdate DESC
        LIMIT 1
    )id
    FROM (
        SELECT DISTINCT itemname
        FROM table
    )AS T1
)