SELECT
    name,
    third_category_id,
    concat(name,'//',third_category_id) AS third_category_name FROM dim.third_category_all
GLOBAL INNER JOIN
(
    SELECT third_category_id FROM dim.question_b_v2
    WHERE
        if('{{ subcategory_id }}' != '全部', subcategory_id = '{{ subcategory_id }}', 0)
    GROUP BY third_category_id
) AS t2
ON t2.third_category_id = dim.third_category_all._id
UNION ALL
SELECT '全部' AS name, '全部' AS third_category_id, '全部' AS third_category_name
