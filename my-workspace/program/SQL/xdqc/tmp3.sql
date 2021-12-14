SELECT
    '{{平台}}' AS `平台`,
    shop_id,
    abnormal_dialog_info.seller_nick AS `店铺`,
    qc_dialog_cnt AS `质检会话量`,
    abnormal_dialog_cnt AS `异常会话量`,
    concat(
        toString(
            if(qc_dialog_cnt!=0,round((`异常会话量` * 100 / qc_dialog_cnt), 2), 0.0)
        ),
        '%'
    ) AS `异常会话占比`,
    emotion_name AS `顾客负面情绪Top1`,
    c_emotion_sum AS `顾客负面情绪触发次数`
FROM (
    SELECT
        seller_nick,
        sum(score>0) AS abnormal_dialog_cnt -- 扣分会话量/异常会话量
    FROM (
        SELECT
            toYYYYMMDD(begin_time) AS day,
            *
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND platform = '{{平台}}'
    ) AS dialog_info
    GROUP BY seller_nick
) AS abnormal_dialog_info
GLOBAL LEFT JOIN (
    SELECT
        seller_nick,
        COUNT(1) AS qc_dialog_cnt -- 质检会话量
    FROM (
        SELECT
            toInt32(toYYYYMMDD(begin_time)) AS day,
            seller_nick,
            snick,
            _id
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND platform = '{{平台}}'
        -- 过滤关联了质检标准的店铺
        AND (toInt32(toYYYYMMDD(begin_time)),seller_nick) GLOBAL IN (
            SELECT DISTINCT 
                day, seller_nick
            FROM ods.xinghuan_qc_norm_relate_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND platform = '{{平台}}'
        )
        -- 过滤关联了质检标注的子账号
        AND (toInt32(toYYYYMMDD(begin_time)),snick) GLOBAL IN (
            -- 查询所有关联了质检标准的子账号分组下的子账号
            SELECT DISTINCT 
                day, snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND platform = '{{平台}}'
            AND (day,department_id) GLOBAL IN (
                -- 查询关联了质检标准的子账号分组ID
                SELECT DISTINCT 
                    day, department_id
                FROM ods.xinghuan_qc_norm_relate_all
                WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
                AND platform = '{{平台}}'
            )
        )
    ) AS dialog_info
    GROUP BY seller_nick
) AS qc_dialog_info
ON abnormal_dialog_info.seller_nick = qc_dialog_info.seller_nick
GLOBAL LEFT JOIN (
    SELECT
        seller_nick,
        most_c_emotion_type,
        emotion_name,
        max_c_emotion_sum AS c_emotion_sum
    FROM (
        -- 过滤出各个店铺Top1的顾客负面情绪
        SELECT
            seller_nick,
            argMax(c_emotion_type,c_emotion_sum) AS most_c_emotion_type,
            max(c_emotion_sum) AS max_c_emotion_sum
        FROM (
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
            AND c_emotion_count!=0
            AND c_emotion_type>=4
            GROUP BY seller_nick, c_emotion_type
        )
        GROUP BY seller_nick
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
    ) AS emotion_info
    ON cnick_emotion_info.most_c_emotion_type = emotion_info.c_emotion_type
) AS ai_c_emotion_info
ON abnormal_dialog_info.seller_nick = ai_c_emotion_info.seller_nick
GLOBAL LEFT JOIN (
    SELECT
        shop_id,
        plat_shop_name AS seller_nick
    FROM dim.shop_nick_all
    WHERE expire_time >= today()
) AS shop_info
ON abnormal_dialog_info.seller_nick = shop_info.seller_nick
