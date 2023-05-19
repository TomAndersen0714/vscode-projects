SELECT
    old_id,
    new_id,
    new_sid,
    t2.sid
FROM (
    SELECT
        old._id AS old_id,
        new._id AS new_id,
        new.sid AS new_sid
    FROM (
        SELECT
            _id,
            qid AS question_b_id,
            question AS question_b_name
        FROM dim.question_b_v2_all
        WHERE 1=1
        AND qid IN (
            SELECT DISTINCT
                question_b_qid
            FROM ods.xdrs_logs_all
            WHERE day BETWEEN 20230401 AND 20230414
            AND platform = 'jd' -- 商品ID
            AND question_b_qid != ''
            AND shop_id GLOBAL IN (
                SELECT DISTINCT shop_id
                FROM xqc_dim.xqc_shop_all
                WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '63fc50f0a06a5ecd9a249ac9'
                    AND platform = 'jd'
            )
        )
        AND qid NOT IN (
            -- 获取企业店铺行业场景
            SELECT
                question_b_qid
            FROM (
                -- 获取企业店铺行业场景一级分组
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
                    subcategory_id
                FROM dim.question_b_v2_all
            ) AS question_b_info
            USING(subcategory_id)
        )
    ) AS old
    GLOBAL INNER JOIN (
        -- 获取企业店铺行业场景
        SELECT
            _id,
            sid,
            question_b_qid,
            question_b_name
        FROM (
            -- 获取企业店铺行业场景一级分组
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
                _id,
                sid,
                qid AS question_b_qid,
                question AS question_b_name,
                subcategory_id
            FROM dim.question_b_v2_all
        ) AS question_b_info
        USING(subcategory_id)
    ) AS new
    ON old.question_b_name = new.question_b_name
    WHERE old._id != new._id
) AS t1
LEFT JOIN (
    SELECT
        qid,
        sid
    FROM dim.question_b_v2_all
) AS t2
ON t1.old_id = t2.qid