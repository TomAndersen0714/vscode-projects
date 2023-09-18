SELECT city.province,
    city.name AS city_name,
    city.gdp / province.total_gdp AS gdp_ratio,
    ROW_NUMBER() OVER (
        PARTITION BY city.province
        ORDER BY city.gdp DESC
    ) AS ranking_in_province
FROM city
    JOIN (
        SELECT province,
            SUM(gdp) AS total_gdp
        FROM city
        GROUP BY province
    ) AS province ON city.province = province.province;