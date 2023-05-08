
insert into dim.shop_category_question_all
SELECT
    shop_id,
    category_id,
    subcategory_id,
    _id AS question_id,
    1 AS model_type
FROM
(
    SELECT
        shop_id,
        category_id,
        subcategory_id
    FROM
    (
        SELECT
            shop_id,
            b.category_id AS category_id
        FROM
        (
            SELECT
                _id AS shop_id,
                category_id
            FROM dim.xdre_shop_all
            WHERE (platform = 'jd') AND (model_type = '1')
        ) AS a
        LEFT JOIN
        (
            SELECT DISTINCT
                _id,
                arrayJoin(domain_categories_ids) AS category_id
            FROM dim.kaleidoscope_category_domain_all
        ) AS b ON a.category_id = b._id
    )
    LEFT JOIN dim.category_subcategory_all USING (category_id)
)
LEFT JOIN
(
    SELECT
        _id,
        arrayJoin(subcategory_ids) AS subcategory_id
    FROM dim.question_b_all
) USING (subcategory_id)
WHERE (shop_id != '') AND (question_id != '');