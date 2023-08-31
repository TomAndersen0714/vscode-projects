        SELECT
            day,
            platform,
            shop_id,
            snick,
            sid,
            question_id,
            dialog_qa_stage,
            dialog_goods_id,
            recent_order_id,
            recent_order_status,
            recent_order_status_timestamp,
            cnick_id_bitmap,
            dialog_sum
        FROM (
            SELECT
                day,
                platform,
                shop_id,
                snick,
                question_id,
                dialog_qa_stage,
                dialog_goods_id,
                recent_order_id,
                recent_order_status,
                recent_order_status_timestamp,
                groupBitmapState(cnick_id) AS cnick_id_bitmap,
                bitmapCardinality(cnick_id_bitmap) AS dialog_sum
            FROM (
                SELECT
                    day,
                    platform,
                    shop_id,
                    snick,
                    cnick_id,
                    question_b_qid AS question_id,
                    CASE
                        WHEN dialog_qa_sum=0 THEN 0
                        WHEN dialog_qa_sum>0 AND dialog_qa_sum<=3 THEN 1
                        WHEN dialog_qa_sum>3 AND dialog_qa_sum<=10 THEN 2
                        ELSE 3
                    END AS dialog_qa_stage,
                    plat_goods_id AS dialog_goods_id,
                    recent_order_id,
                    recent_order_status,
                    recent_order_status_timestamp
                FROM dwd.voc_chat_log_detail_all
                WHERE day = 20230826
                AND shop_id = '60b72d421edc070017428380'
                AND plat_goods_id = '718154483681'
            )
            GROUP BY day,
                platform,
                shop_id,
                snick,
                question_id,
                dialog_qa_stage,
                dialog_goods_id,
                recent_order_id,
                recent_order_status,
                recent_order_status_timestamp
        ) AS question_stat_info
        LEFT JOIN (
            SELECT
                sid,
                qid AS question_id
            FROM dim.question_b_all
        ) AS question_info
        USING(question_id)