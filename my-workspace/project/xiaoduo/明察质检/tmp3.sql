select count(distinct plat_goods_id) as count
from (
        select plat_goods_id,
            shop_id,
            question_b_id,
            subcategory_name,
            question,
            create_time,
            sum(ask_count) as ask_count,
            sum(goods_no_reply_count) as goods_no_reply_count
        from (
                select shop_id,
                    question_b_id,
                    subcategory_name,
                    question,
                    plat_goods_id,
                    ask_count,
                    goods_no_reply_count,
                    create_time
                from (
                        select shop_id,
                            question_b_id,
                            subcategory_name,
                            question,
                            plat_goods_id,
                            ask_count,
                            goods_no_reply_count,
                            create_time
                        from (
                                SELECT shop_id,
                                    question_b_id,
                                    plat_goods_id,
                                    question,
                                    subcategory_id,
                                    subcategory_name,
                                    ask_count,
                                    goods_no_reply_count,
                                    no_reply_reason_v2,
                                    no_reply_sub_reason_v2,
                                    no_reply_reason_detail_v2,
                                    sale_status,
                                    no_reply_count,
                                    now() AS create_time,
                                    20231101 AS day
                                FROM (
                                        SELECT shop_id,
                                            question_b_id,
                                            question,
                                            plat_goods_id,
                                            subcategory_id,
                                            subcategory_name,
                                            ask_count
                                        FROM (
                                                SELECT shop_id,
                                                    question_b_qid,
                                                    plat_goods_id,
                                                    count() AS ask_count
                                                FROM ods.xdrs_logs_all
                                                WHERE (day = 20231101)
                                                    AND notEmpty(plat_goods_id)
                                                GROUP BY shop_id,
                                                    question_b_qid,
                                                    plat_goods_id
                                            ) AS t4
                                            LEFT JOIN (
                                                SELECT dim.question_b_all._id AS question_b_id,
                                                    qid,
                                                    question,
                                                    subcategory_id,
                                                    name AS subcategory_name
                                                FROM dim.question_b_all
                                                    INNER JOIN dim.subcategory_all ON dim.question_b_all.subcategory_id = dim.subcategory_all._id
                                            ) AS t5 ON t4.question_b_qid = t5.qid
                                    ) AS t6
                                    LEFT JOIN (
                                        SELECT shop_id,
                                            question_b_id,
                                            plat_goods_id,
                                            no_reply_reason_v2,
                                            no_reply_sub_reason_v2,
                                            no_reply_reason_detail_v2,
                                            sale_status,
                                            no_reply_count,
                                            goods_no_reply_count
                                        FROM (
                                                SELECT shop_id,
                                                    question_b_id,
                                                    plat_goods_id,
                                                    no_reply_reason_v2,
                                                    no_reply_sub_reason_v2,
                                                    no_reply_reason_detail_v2,
                                                    sale_status,
                                                    countDistinct(msgid) AS no_reply_count
                                                FROM ods.no_reply_logs_v2_all
                                                WHERE (day = 20231101)
                                                    AND notEmpty(question_b_id)
                                                    AND notEmpty(plat_goods_id)
                                                GROUP BY shop_id,
                                                    question_b_id,
                                                    plat_goods_id,
                                                    no_reply_reason_v2,
                                                    no_reply_sub_reason_v2,
                                                    no_reply_reason_detail_v2,
                                                    sale_status
                                            ) AS t1
                                            LEFT JOIN (
                                                SELECT shop_id,
                                                    question_b_id,
                                                    plat_goods_id,
                                                    countDistinct(msgid) AS goods_no_reply_count
                                                FROM ods.no_reply_logs_v2_all
                                                WHERE (day = 20231101)
                                                    AND notEmpty(question_b_id)
                                                    AND notEmpty(plat_goods_id)
                                                GROUP BY shop_id,
                                                    question_b_id,
                                                    plat_goods_id
                                            ) AS t2 USING (shop_id, question_b_id, plat_goods_id)
                                    ) AS t3 USING (shop_id, question_b_id, plat_goods_id)
                            )
                        where shop_id = '6136d454ec7097000e494038'
                            and question_b_id = '5639bf1b89bc4603d5c6137e'
                    )
                group by shop_id,
                    question_b_id,
                    subcategory_name,
                    question,
                    plat_goods_id,
                    ask_count,
                    goods_no_reply_count,
                    create_time
            )
        group by plat_goods_id,
            shop_id,
            question_b_id,
            subcategory_name,
            question,
            create_time
    ) -- trace:9e3964313cc040a60000001698853321