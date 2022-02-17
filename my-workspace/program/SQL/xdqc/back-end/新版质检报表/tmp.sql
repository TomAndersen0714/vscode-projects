-- 平台/店铺/子账号维度-人工质检结果
SELECT *
FROM (
    -- 人工质检-子账号维度扣分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        groupArray(tag_score_label_id) AS tag_score_label_ids
        groupArray(tag_score_label_sum) AS tag_score_label_cnts
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            tag_score_label_id,
            sum(tag_score_label_cnt) AS tag_score_label_sum
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            tag_score_id AS tag_score_label_id, 
            tag_score_count AS tag_score_label_cnt
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        )
        AND tag_score_id!=[]
        GROUP BY platform, seller_nick, snick, tag_score_label_id
    ) AS tag_score_label_stat
    GROUP BY platform, seller_nick, snick
) AS tag_score_info
GLOBAL FULL OUTER JOIN (
    -- 人工质检-子账号维度加分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        groupArray(tag_score_add_stats_label_id) AS tag_score_add_stats_label_ids
        groupArray(tag_score_add_stats_label_sum) AS tag_score_add_stats_label_cnts
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            tag_score_add_stats_label_id,
            sum(tag_score_add_stats_label_cnt) AS tag_score_add_stats_label_sum
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            tag_score_add_stats_id AS tag_score_add_stats_label_id, 
            tag_score_add_stats_count AS tag_score_add_stats_label_cnt
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        )
        AND tag_score_add_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_score_add_stats_label_id
    ) AS tag_score_add_stats_label_stat
    GROUP BY platform, seller_nick, snick
) AS tag_score_add_info
USING(platform, seller_nick, snick)