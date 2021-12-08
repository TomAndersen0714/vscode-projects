-- 创建临时表, 存储演算数据
CREATE TABLE tmp.qc_question_detail
AS ods.qc_question_detail_all
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date) ORDER BY (shop_name, qc_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE tmp.qc_words_detail
AS ods.qc_words_detail_all
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date) 
ORDER BY (shop_name, word)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- 写入 2021-12-06 的统计数据
-- AI质检-扣分问题汇总
insert into tmp.qc_question_detail
SELECT toDate('2021-12-06'),
    ai_qc_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    ai_qc_info.seller_nick as shop_name,
    ai_qc_info.`group`,
    ai_qc_info.`type`,
    ai_qc_info.qc_id,
    ai_qc_info.qc_name,
    ai_qc_info.qc_count
from (
    select `seller_nick`,
        platform,
        `group`,
        'ai' as type,
        snick,
        qc_id,
        '' as qc_name,
        qc_count
    from dwd.xdqc_dialog_all
    array join
        abnormals_type as qc_id,
        abnormals_count as qc_count
    where toYYYYMMDD(begin_time) = 20211206
        and qc_count != 0
        and platform = 'tb'
) ai_qc_info
GLOBAL LEFT JOIN (
    SELECT a.company_id AS company_id,
        a._id AS department_id,
        a.name AS department_name,
        b.employee_id AS employee_id,
        b.employee_name AS employee_name,
        b.snick AS snick
    FROM (
        SELECT *
        FROM ods.xinghuan_department_all
        WHERE day = 20211206
    ) AS a 
    GLOBAL LEFT JOIN (
        SELECT a._id AS employee_id,
            b.department_id AS department_id,
            a.username AS employee_name,
            b.snick AS snick
        FROM (
            SELECT *
            FROM ods.xinghuan_employee_all
            WHERE day = 20211206
        ) AS a 
        GLOBAL RIGHT JOIN (
            SELECT *
            FROM ods.xinghuan_employee_snick_all
            WHERE day = 20211206
                and platform = 'tb'
        ) AS b ON a._id = b.employee_id
    ) AS b ON a._id = b.department_id
) dim_info 
on ai_qc_info.snick = dim_info.snick

-- 人工质检-质检标签汇总
insert into tmp.qc_question_detail
SELECT toDate('2021-12-06'),
    'tb' as platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    manual_qc_info.seller_nick as shop_name,
    manual_qc_info.`group`,
    manual_qc_info.`type`,
    manual_qc_info.qc_id,
    manual_qc_info.qc_name,
    manual_qc_info.qc_count
from (
        select tag_info.seller_nick,
            tag_info.`group`,
            'manual' as type,
            tag_info.snick,
            tag_info.tag_id as qc_id,
            all_tag_name_info.all_tag_name as qc_name,
            tag_info.qc_count as qc_count
        from (
                select tag_id,
                    seller_nick,
                    `group`,
                    snick,
                    count(1) as qc_count
                from ods.xinghuan_dialog_tag_score_all
                where day = 20211206
                    and cal_op = 0
                group by tag_id,
                    seller_nick,
                    `group`,
                    snick
            ) as tag_info
            left join (
                select norm_tag.tag_id,
                    toString(
                        concat(
                            if(
                                norm_tag.qc_norm_name = '',
                                '未设置一级标签',
                                norm_tag.qc_norm_name
                            ),
                            '/',
                            if(
                                sub_category.name = '',
                                '未设置二级标签',
                                sub_category.name
                            ),
                            '/',
                            norm_tag.tag_name
                        )
                    ) as all_tag_name
                from (
                        select b._id as qc_norm_id,
                            b.name as qc_norm_name,
                            a._id as tag_id,
                            a.name as tag_name,
                            a.sub_category_id as sub_category_id
                        from (
                                select _id,
                                    category_id,
                                    sub_category_id,
                                    seller_nick,
                                    qc_norm_id,
                                    name
                                from ods.xdqc_tag_all
                                where day = 20211206
                            ) as a
                            left join (
                                select _id,
                                    name
                                from ods.xinghuan_qc_norm_all
                                where day = 20211206
                                    and status = 1
                            ) as b on a.qc_norm_id = b._id
                    ) as norm_tag
                    left join (
                        select _id,
                            name
                        from ods.xdqc_tag_sub_category_all
                        where day = 20211206
                    ) as sub_category on norm_tag.sub_category_id = sub_category._id
            ) as all_tag_name_info on tag_info.tag_id = all_tag_name_info.tag_id
    ) as manual_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = 20211206
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = 20211206
                    ) AS a 
                    GLOBAL RIGHT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = 20211206
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on manual_qc_info.snick = dim_info.snick

-- AI质检-客服情绪汇总
insert into tmp.qc_question_detail
SELECT toDate('2021-12-06'),
    ai_qc_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    ai_qc_info.seller_nick as shop_name,
    ai_qc_info.`group`,
    ai_qc_info.`type`,
    ai_qc_info.qc_id,
    ai_qc_info.qc_name,
    ai_qc_info.qc_count
from (
        select `seller_nick`,
            platform,
            `group`,
            's_emotion' as type,
            snick,
            qc_id,
            '' as qc_name,
            qc_count
        from dwd.xdqc_dialog_all
        array join
            s_emotion_type as qc_id,
            s_emotion_count as qc_count
        where toYYYYMMDD(begin_time) = 20211206
            and qc_count != 0
            and platform = 'tb'
    ) ai_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = 20211206
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = 20211206
                    ) AS a 
                    GLOBAL RIGHT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = 20211206
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on ai_qc_info.snick = dim_info.snick 

-- AI质检-买家情绪汇总
insert into tmp.qc_question_detail
SELECT toDate('2021-12-06'),
    ai_qc_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    ai_qc_info.seller_nick as shop_name,
    ai_qc_info.`group`,
    ai_qc_info.`type`,
    ai_qc_info.qc_id,
    ai_qc_info.qc_name,
    ai_qc_info.qc_count
from (
        select `seller_nick`,
            platform,
            `group`,
            'c_emotion' as type,
            snick,
            qc_id,
            '' as qc_name,
            qc_count
        from dwd.xdqc_dialog_all array
            join c_emotion_type as qc_id,
            c_emotion_count as qc_count
        where toYYYYMMDD(begin_time) = 20211206
            and qc_count != 0
            and platform = 'tb'
    ) ai_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = 20211206
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = 20211206
                    ) AS a 
                    GLOBAL RIGHT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = 20211206
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on ai_qc_info.snick = dim_info.snick

-- AI质检-客服和买家质检词触发次数统计
insert into tmp.qc_words_detail
SELECT
    toDate('2021-12-06'),
    words_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    words_info.shop_name,
    words_info.`group`,
    words_info.snick,
    words_info.source,
    words_info.word,
    words_info.words_count
FROM (
        SELECT `date`,
            platform,
            seller_nick AS shop_name,
            `group`,
            snick,
            source,
            word,
            sum(count) AS words_count
        FROM dwd.xdqc_dialog_all 
        array JOIN 
            qc_word_word AS word,
            qc_word_source AS source,
            qc_word_count AS count
        WHERE toYYYYMMDD(begin_time) = 20211206
            AND qc_word_word != []
        GROUP BY `date`,
            platform,
            seller_nick,
            `group`,
            snick,
            source,
            word
    ) AS words_info 
    GLOBAL LEFT JOIN (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = 20211206
            ) AS a 
            GLOBAL LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = 20211206
                    ) AS a
                    GLOBAL RIGHT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = 20211206
                            and platform = 'tb'
                    ) AS b 
                    ON a._id = b.employee_id
            ) AS b 
            ON a._id = b.department_id
    ) dim_info 
    on words_info.snick = dim_info.snick


-- 执行后端查询, 对比结果
-- AI质检问题排行TOP10
-- PS: AI质检问题包括 type 的值为 ('ai', 's_emotion', 'c_emotion')
select a.platform as platform,
    type,
    b.qc_id as qc_id,
    b.qc_name as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from tmp.qc_question_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
            and `type` in ('ai', 's_emotion', 'c_emotion')
        group by platform
    ) as a
    global right join (
        select platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) as count_info
        from tmp.qc_question_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
            and `type` in ('ai', 's_emotion', 'c_emotion')
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
        limit 10
    ) as b on a.platform = b.platform
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
        from tmp.qc_question_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
            and `type` = 'manual'
        group by platform,
            `type`
    ) as a
    global right join (
        select platform,
            `type`,
            qc_id,
            replaceAll(replaceAll(qc_name, '未设置一级标签/', ''), '未设置二级标签/', '') as qc_name_all,
            sum(qc_count) as count_info
        from tmp.qc_question_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
            and `type` = 'manual'
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
        from tmp.qc_words_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
        group by platform,
            word
    ) a
    global right join (
        select platform,
            sum(words_count) as words_count_all
        from tmp.qc_words_detail
        WHERE date >= 1638720000 and date < 1638806399
            and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
        group by platform
        order by words_count_all desc
        limit 10
    ) b on a.platform = b.platform
order by qc_proportion DESC
limit 10
