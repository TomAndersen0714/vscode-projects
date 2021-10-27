SELECT a.platform AS platform,
    type,
    b.qc_id AS qc_id,
    b.qc_name AS qc_name,
    round(b.count_info / a.count_all_info, 4) AS qc_proportion
FROM (
        SELECT platform,
            sum(qc_count) AS count_all_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
            AND `type` IN (
                'ai',
                's_emotion',
                'c_emotion'
            )
        GROUP BY platform
    ) AS a
    LEFT JOIN (
        SELECT platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) AS count_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND `type` IN (
                'ai',
                's_emotion',
                'c_emotion'
            )
            AND shop_name IN ('方太官方旗舰店')
        GROUP BY platform,
            `type`,
            qc_id,
            qc_name
        ORDER BY count_info DESC
    ) AS b ON a.platform = b.platform

UNION ALL

SELECT a.platform AS platform,
    's_emotion' AS `type`,
    b.qc_id AS qc_id,
    b.qc_name AS qc_name,
    round(b.count_info / a.count_all_info, 4) AS qc_proportion
FROM (
        SELECT platform,
            sum(qc_count) AS count_all_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
            AND `type` = 's_emotion'
        GROUP BY platform,
            `type`
    ) AS a
    LEFT JOIN (
        SELECT platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) AS count_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
            AND `type` = 's_emotion'
        GROUP BY platform,
            `type`,
            qc_id,
            qc_name
    ) AS b ON a.platform = b.platform

UNION ALL

SELECT a.platform AS platform,
    'manual' AS `type`,
    b.qc_id AS qc_id,
    b.qc_name_all AS qc_name,
    round(b.count_info / a.count_all_info, 4) AS qc_proportion
FROM (
        SELECT platform,
            sum(qc_count) AS count_all_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
            AND `type` = 'manual'
        GROUP BY platform,
            `type`
    ) AS a
    LEFT JOIN (
        SELECT platform,
            `type`,
            qc_id,
            replaceAll(replaceAll(qc_name, '未设置一级标签/', ''), '未设置二级标签/', '') AS qc_name_all,
            sum(qc_count) AS count_info
        FROM ods.qc_question_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND `type` = 'manual'
            AND shop_name IN ('方太官方旗舰店')
        GROUP BY platform,
            `type`,
            qc_id,
            qc_name
        LIMIT 10
    ) AS b ON a.platform = b.platform
ORDER BY qc_proportion DESC

UNION ALL

SELECT b.platform AS platform,
    'qc_word' AS `type`,
    '' AS qc_id,
    a.word AS qc_name,
    round((a.words_count_info / b.words_count_all), 4) AS qc_proportion
FROM (
        SELECT platform,
            word,
            sum(words_count) AS words_count_info
        FROM ods.qc_words_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
        GROUP BY platform,
            word
        ORDER BY words_count_info DESC
    ) a
    LEFT JOIN (
        SELECT platform,
            sum(words_count) AS words_count_all
        FROM ods.qc_words_detail_all
        WHERE date >= 1632153600
            AND date < 1632758399
            AND shop_name IN ('方太官方旗舰店')
        GROUP BY platform
        ORDER BY words_count_all
    ) b ON a.platform = b.platform
LIMIT 10