SELECT
    day,
    platform,
    seller_nick,
    snick,
    dim_tag.tag_group_id AS tag_group_id,
    dim_tag.tag_group_name AS tag_group_name,
    tag_type,
    tag_id,
    dim_tag.tag_name AS tag_name,
    tag_cnt_sum,
    tag_score_sum
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id,
        SUM(tag_cnt + if(tag_md>0, 1, 0)) AS tag_cnt_sum,
        -- 同一个ID分数可能发生变化, 以实际打标为准
        SUM(tag_score*(tag_cnt + if(tag_md>0, 1, 0))) AS tag_score_sum
    FROM (
        -- 人工质检项-加分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'manual_subtract' AS tag_type,
            tag_score_stats_id AS tag_ids,
            tag_score_stats_count AS tag_cnts,
            tag_score_stats_score AS tag_scores,
            -- 是否打标在会话上
            if(
                tag_score_stats_md=[],
                arrayResize([0], length(tag_score_stats_id)),
                tag_score_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220401 AND 20220430
        AND tag_score_stats_id != []
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )

        -- 人工质检项-扣分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'manual_add' AS tag_type,
            tag_score_add_stats_id AS tag_ids,
            tag_score_add_stats_count AS tag_cnts,
            tag_score_add_stats_score AS tag_scores,
            -- 是否打标在会话上
            if(
                tag_score_add_stats_md=[],
                arrayResize([0], length(tag_score_add_stats_id)),
                tag_score_add_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220401 AND 20220430
        AND tag_score_add_stats_id != []
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )

    ) AS ods_manual_tag
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_mds AS tag_md,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE (tag_cnt!=0 OR tag_md>0)
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_manual_tag_stat
GLOBAL LEFT JOIN (
    SELECT
        *
    FROM (
        -- 查询人工质检项
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND rule_category = 2
    ) AS dim_tag
    GLOBAL LEFT JOIN (
        -- 关联质检项分组
        -- PS: 已删除的分组无法获取
        SELECT
            _id AS tag_group_id,
            full_name AS tag_group_name
        FROM xqc_dim.qc_norm_group_full_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS dim_tag_group
    USING(tag_group_id)
) AS dim_tag
USING(tag_id)