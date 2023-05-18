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
                fourth_group_name,
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
                    WHERE company_id IN ['63fc50f0a06a5ecd9a249ac9']
                ) AS voc_question_info
                INNER JOIN (
                    -- 获取企业店铺行业场景
                    SELECT DISTINCT
                        company_id,
                        shop_id,
                        question_b_qid,
                        question_b_name
                    FROM (
                        -- 获取企业店铺行业场景一级分组, PS: category_id:subcategory_id=N:N
                        SELECT DISTINCT
                            company_id,
                            shop_id,
                            subcategory_id
                        FROM (
                            -- 获取企业店铺专属品类
                            SELECT
                                company_id,
                                shop_id,
                                domain_category_id AS category_id
                            FROM (
                                SELECT
                                    shop_id,
                                    domain_category_id
                                FROM (
                                    SELECT
                                        _id AS shop_id,
                                        category_id
                                    FROM dim.xdre_shop_all
                                    WHERE _id IN ['61616faa112fa5000dcc7fba']
                                    -- 筛选专属模型店铺
                                    AND model_type = '1'
                                ) AS shop_category_info
                                INNER JOIN
                                (
                                    SELECT DISTINCT
                                        _id AS category_id,
                                        arrayJoin(domain_categories_ids) AS domain_category_id
                                    FROM dim.kaleidoscope_category_domain_all
                                ) AS domain_category_info
                                USING(category_id)
                            ) AS shop_subcategory_info
                            GLOBAL INNER JOIN (
                                SELECT
                                    company_id,
                                    shop_id
                                FROM numbers(1)
                                ARRAY JOIN
                                    ['63fc50f0a06a5ecd9a249ac9'] AS company_id,
                                    ['61616faa112fa5000dcc7fba'] AS shop_id
                            ) AS voc_shop_info
                            USING(shop_id)
                        ) AS company_shop_subcategory_info
                        GLOBAL INNER JOIN (
                            SELECT DISTINCT
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
                            arrayJoin(subcategory_ids) AS subcategory_id
                        FROM dim.question_b_all
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
                WHERE company_id IN ['63fc50f0a06a5ecd9a249ac9']
            ) AS company_group_info
            USING(company_id, group_id)