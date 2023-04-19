INSERT INTO {sink_table}
SELECT
    company_id,
    question_b_qid,
    question_b_name,
    group_id,
    group_name,
    group_level,
    parent_group_id,
    parent_group_name,
    first_group_id,
    first_group_name,
    second_group_id,
    second_group_name,
    third_group_id,
    third_group_name,
    fourth_group_id,
    fourth_group_name
    create_time,
    update_time
FROM (
    SELECT
        company_id,
        question_b_qid,
        question_b_name,
        group_id,
        create_time,
        update_time
    FROM (
        SELECT
            company_id,
            name AS question_b_name,
            group_id, 
            create_time,
            update_time
        FROM dim.voc_question_b_all
        WHERE company_id IN {VOC_COMPANY_IDS}
    ) AS voc_question_info
    LEFT JOIN (
        -- 获取企业店铺行业场景
        SELECT
            company_id,
            shop_id,
            category_id,
            subcategory_id,
            question_b_qid,
            question_b_name
        FROM (
            -- 获取企业店铺行业场景一级分组
            SELECT
                company_id,
                shop_id,
                category_id,
                subcategory_id
            FROM (
                -- 获取企业店铺品类
                SELECT
                    company_id,
                    shop_id,
                    category_id
                FROM (
                    SELECT
                        _id AS shop_id,
                        category_id
                    FROM dim.xdre_shop_all
                    WHERE _id IN {VOC_SHOP_IDS}
                ) AS shop_subcategory_info
                GLOBAL INNER JOIN (
                    SELECT
                        company_id,
                        shop_id
                    FROM numbers(1)
                    ARRAY JOIN
                        {VOC_COMPANY_IDS} AS company_id,
                        {VOC_SHOP_IDS} AS shop_id
                ) AS voc_shop_info
                USING(shop_id)
            ) AS company_shop_subcategory_info
            GLOBAL INNER JOIN (
                SELECT
                    category_id,
                    subcategory_id
                FROM dim.category_subcategory_all
            ) AS cate_map_info
            USING(category_id)
        ) AS company_shop_subcategory_info
        INNER JOIN (
            SELECT
                qid AS question_b_qid,
                question AS question_b_name,
                subcategory_id
            FROM dim.question_b_v2_all
        ) AS question_b_info
        USING(subcategory_id)
    ) AS robot_question_info
    USING(company_id, question_b_name)
)
LEFT JOIN (
    SELECT
        company_id,
        group_id,
        group_name,
        group_level,
        parent_group_id,
        parent_group_name,
        first_group_id,
        first_group_name,
        second_group_id,
        second_group_name,
        third_group_id,
        third_group_name,
        fourth_group_id,
        fourth_group_name
    FROM dim.voc_question_b_group_detail_all
    WHERE company_id IN {VOC_COMPANY_IDS}
) AS company_group_info
USING(company_id, group_id)
