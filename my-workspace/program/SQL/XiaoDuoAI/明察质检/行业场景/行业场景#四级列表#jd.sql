SELECT
    name,
    fourth_category_id,
    concat(name,'//',fourth_category_id) AS fourth_category_name
FROM dim.fourth_category_all
GLOBAL INNER JOIN
(
    SELECT fourth_category_id
    FROM dim.question_b_v2
    WHERE subcategory_id = '{{ subcategory_id }}' AND third_category_id = '{{ third_category_id }}'
    GROUP BY fourth_category_id
) AS t2
ON t2.fourth_category_id = dim.fourth_category_all._id
UNION ALL
SELECT '全部' AS name, '全部' AS fourth_category_id, '全部' AS fourth_category_name