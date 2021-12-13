SELECT
    seller_nick,
    c_emotion_type,
    c_emotion_sum,
    emotion_name
FROM (
    -- 统计买家负面情绪
    SELECT
        seller_nick,
        c_emotion_type,
        sum(c_emotion_count) AS c_emotion_sum
    FROM dwd.xdqc_dialog_all
    ARRAY JOIN
        c_emotion_type,
        c_emotion_count
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND platform = '{{平台}}'
    AND seller_nick = '{{店铺}}'
    AND c_emotion_count!=0
    AND c_emotion_type>=4
    GROUP BY seller_nick,c_emotion_type
    -- 过滤出当前店铺Top1的顾客负面情绪
    ORDER BY c_emotion_sum DESC
    LIMIT 1
) AS cnick_emotion_info
GLOBAL LEFT JOIN (
    SELECT
        toUInt16(emotion_id) AS c_emotion_type,
        emotion_name
    FROM numbers(1)
    ARRAY JOIN
        [0,1,2,3,4,5,6,7,8,9] AS emotion_id,
        ['中性','满意','感激','期待','对客服态度不满','对发货物流不满',
        '对产品不满','其他不满意','骂人','对收货少件不满'] AS emotion_name
)
USING(c_emotion_type)