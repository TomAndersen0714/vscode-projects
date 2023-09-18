SELECT
    province, city, gdp, gdp/province_gdp AS gdp_pct,
    row_number() over(partition by province order by gdp desc) AS gdp_rank
FROM city_gdp_table
JOIN (
    SELECT province, SUM(gdp) AS province_gdp
    FROM city_gdp
    GROUP BY province
) AS province_gdp_table
USING(province)


1. 先求各部门平均分, 然后筛选平均分大于80的部门
SELECT
    dept_id,
    AVG(score) AS avg_score
FROM Emp
GROUP BY dept_id
HAVING avg_score>80

2. 关联部门名称
SELECT
    dept_id,
    dept_name,
    avg_score
FROM (
    SELECT
        dept_id,
        AVG(score) AS avg_score
    FROM Emp
    GROUP BY dept_id
    HAVING avg_score>80
) AS Dept_score_tmp
JOIN Dept
USING(dept_id)
