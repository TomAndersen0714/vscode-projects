-- 融合版行业场景分类
SELECT DISTINCT
    name,
    subcategory_id,
    concat(name,'//',subcategory_id) AS subcategory_name
FROM dim.subcategory_all
GLOBAL INNER JOIN 
(
    SELECT subcategory_id
    FROM dim.category_subcategory_all
    WHERE category_id IN
    (
        SELECT category_id
        FROM dim.xdre_shop_all
        WHERE _id = '{{ shop_id }}'
    )
) AS t2
ON t2.subcategory_id = dim.subcategory_all._id
ORDER BY name DESC
UNION ALL
SELECT '全部' AS name, '全部' AS subcategory_id, '全部' AS subcategory_name