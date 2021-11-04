-- ods.qc_question_detail_all
-- ods.qc_words_detail_all

select a.platform as platform,
    type,
    b.qc_id as qc_id,
    b.qc_name as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
            and `type` in ('ai', 's_emotion', 'c_emotion')
        group by platform
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= %d
            and date < %d
            and `type` in ('ai', 's_emotion', 'c_emotion')
            and shop_name in %s -- startDate, endDate, shopStr
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
    ) as b on a.platform = b.platform

UNION ALL

SELECT a.platform AS platform, -- 客服情感
    's_emotion' AS `type`,
    b.qc_id AS qc_id,
    b.qc_name AS qc_name,
    round(b.count_info / a.count_all_info, 4) AS qc_proportion
FROM (
        SELECT platform,
            sum(qc_count) AS count_all_info
        FROM ods.qc_question_detail_all
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
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
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
            AND `type` = 's_emotion'
        GROUP BY platform,
            `type`,
            qc_id,
            qc_name
    ) AS b ON a.platform = b.platform

union all

select a.platform as platform, -- 人工质检问题
    'manual' as `type`,
    b.qc_id as qc_id,
    b.qc_name_all as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
            and `type` = 'manual'
        group by platform,
            `type`
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            replaceAll(replaceAll(qc_name, '未设置一级标签/', ''), '未设置二级标签/', '') as qc_name_all,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= %d
            and date < %d
            and `type` = 'manual'
            and shop_name in %s -- startDate, endDate, shopStr
        group by platform,
            `type`,
            qc_id,
            qc_name
        limit 10
    ) as b on a.platform = b.platform
order by qc_proportion desc

union all

select b.platform as platform, -- 质检词分布
    'qc_word' as `type`,
    '' as qc_id,
    a.word as qc_name,
    round((a.words_count_info / b.words_count_all), 4) as qc_proportion
from (
        select platform,
            word,
            sum(words_count) as words_count_info
        from ods.qc_words_detail_all
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
        group by platform, word
        order by words_count_info desc
    ) a
    left join (
        select platform,
            sum(words_count) as words_count_all
        from ods.qc_words_detail_all
        WHERE date >= %d
            and date < %d
            and shop_name in %s -- startDate, endDate, shopStr
        group by platform
        order by words_count_all DESC
        LIMIT 10
    ) b on a.platform = b.platform
limit 10