SELECT
    dialog_info.dialog_id,
    dialog_info.seller_nick,
    dialog_info.cnick,
    dialog_info.is_after_sale,
    dialog_info.qid,
    qid_info.subcategory_name,
    qid_info.question
FROM (
        -- 筛选售后质检会话
        SELECT _id AS dialog_id,
            seller_nick,
            cnick,
            is_after_sale,
            toString(arrayJoin(qid)) AS qid
        FROM dwd.xdqc_dialog_all
        WHERE seller_nick = '全棉时代官方旗舰店'
        AND toYYYYMMDD(begin_time) = 20230129
        AND is_after_sale = 1
        and  qid
        LIMIT 100
) AS dialog_info
INNER JOIN (
    -- 获取行业场景及其问题分类
    SELECT
        qid,
        question,
        subcategory_name
    FROM (
        SELECT qid, question, subcategory_id
        FROM dim.question_b_v2_all
    ) AS question_b_info
    right JOIN (
        SELECT DISTINCT
            _id AS subcategory_id,
            name AS subcategory_name
        FROM dim.subcategory_all
        where name in
        ('商品问题','物流问题')
    ) AS subcategory_info
    USING(subcategory_id)
) AS qid_info
USING(qid)