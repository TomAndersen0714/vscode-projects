-- 178. Rank Scores
/* 
    Write a SQL query to rank scores. If there is a tie between two scores, 
    both should have the same ranking. Note that after a tie, the next ranking 
    number should be the next consecutive integer value. In other words, there 
    should be no "holes" between ranks.

    Important Note: For MySQL solutions, to escape reserved words used as column 
    names, you can use an apostrophe before and after the keyword. For example `Rank`.
 */
-- 此题为Dense Rank,要求按照成绩排名输出所有人的成绩(Score)和排名(`Rank`),排名从1开始

-- 方法1:相关子查询(标准解)
SELECT
    T1.Score, 
    (
        SELECT COUNT(DISTINCT T2.Score)+1
        FROM Scores AS T2
        WHERE T2.Score > T1.Score
    )AS `Rank`
FROM Scores AS T1
ORDER BY T1.Score DESC;
-- 建议不要使用COUNT(DISTINCT),实测效率较低,如果想要提高执行效率,可以通过多次嵌套子查询,在每个子查询中过滤无用数据.
SELECT
    T1.Score,
    (
        SELECT COUNT(1)+1
        FROM(
            SELECT DISTINCT Score
            FROM Scores
        ) T2
        WHERE T2.Score > T1.Score
    )AS `Rank`
FROM Scores AS T1
ORDER BY T1.Score DESC;

-- 方法2:连接查询(Join)
-- 此连接查询会导致表中间表膨胀到原来的n*n倍,禁止使用此方法
/* SELECT
    T1.Score, COUNT(DISTINCT T2.Score) AS `Rank`
FROM Scores AS T1
LEFT JOIN
    Scores AS T2
ON T1.Score <= T2.Score
GROUP BY
    T1.Id
ORDER BY T1.Score DESC; */

-- 方法3:开窗函数(DENSE_RANK)(最优解)
-- 一般而言,开窗函数是最快的
SELECT
    Score,
    DENSE_RANK() OVER(ORDER BY Score DESC) AS `Rank`
FROM
    Scores;
