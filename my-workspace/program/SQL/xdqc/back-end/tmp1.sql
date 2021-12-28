select a.platform as platform,
    type,
    b.qc_id as qc_id,
    b.qc_name as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1640016000 and date < 1640102399
            and platform = 'tb'
            and shop_name in ['方太官方旗舰店'] 
            and (
                `type` = 'ai'
                OR (`type` = 's_emotion')
                OR (`type` = 'c_emotion' AND qc_id>='4')
            )
        group by platform
    ) as a
    global inner join (
        select platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1640016000 and date < 1640102399 
            and (
                `type` = 'ai'
                OR (`type` = 's_emotion')
                OR (`type` = 'c_emotion' AND qc_id>='4')
            )
            and shop_name in ['方太官方旗舰店']
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
        limit 10
    ) as b
    on a.platform = b.platform
order by qc_proportion desc
limit 10
UNION ALL
select a.platform as platform,
    'manual' as `type`,
    b.qc_id as qc_id,
    b.qc_name_all as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1640016000 and date < 1640102399
            and shop_name in ['方太官方旗舰店']
            and `type` = 'manual'
        group by platform,
            `type`
    ) as a
    global inner join (
        select platform,
            `type`,
            qc_id,
            replaceAll(
                replaceAll(qc_name, '未设置一级标签/', ''),
                '未设置二级标签/',
                ''
            ) as qc_name_all,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1640016000 and date < 1640102399
            and `type` = 'manual'
            and shop_name in ['方太官方旗舰店']
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info DESC
        limit 10
    ) as b
    on a.platform = b.platform
order by qc_proportion desc
LIMIT 10
union all
select b.platform as platform,
    'qc_word' as `type`,
    '' as qc_id,
    a.word as qc_name,
    round((a.words_count_info / b.words_count_all), 4) as qc_proportion
from (
        select platform,
            word,
            sum(words_count) as words_count_info
        from ods.qc_words_detail_all
        WHERE date >= 1640016000 and date < 1640102399
            and shop_name in ['方太官方旗舰店']
        group by platform,
            word
        order by words_count_info desc
    ) a
    global inner join (
        select platform,
            sum(words_count) as words_count_all
        from ods.qc_words_detail_all
        WHERE date >= 1640016000 and date < 1640102399
            and shop_name in ['方太官方旗舰店']
        group by platform
        order by words_count_all desc
        limit 10
    ) b on a.platform = b.platform
order by qc_proportion DESC
limit 10