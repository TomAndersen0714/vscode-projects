SELECT '{{ shop_id_var=6143eec86ebd17000d941c60 }}' AS "店铺id",
    t4.name AS "组别",
    t1.snick AS "昵称",
    t3.name AS "姓名",
    t1.uv AS "咨询顾客数",
    t1.effective_tag_uv AS "有效打标顾客数",
    t1.effective_tag_rat AS "咨询打标率",
    t2.group_name AS "标签组",
    if (
        "标签组" = '产品需求',
        concat(t2.tag_name, '(自动)'),
        concat(t2.tag_name, '(人工)')
    ) as "标签",
    multiIf(
        "标签组" = '意向等级',
        1,
        "标签组" = '关联销售',
        2,
        "标签组" = '客户进线第一需求',
        3,
        "标签组" = '未成交原因',
        4,
        "标签组" = '确定意向产品',
        5,
        6
    ) AS "标签排序",
    toString(t2.tag_cnt) AS "数量"
FROM (
        SELECT t1.snick,
            t1.uv AS uv,
            t2.effective_tag_uv AS effective_tag_uv,
            concat(toString(round(effective_tag_uv * 100 / uv, 2)), '%') AS effective_tag_rat
        FROM (
                SELECT splitByChar(':', snick) [2] AS snick,
                    uniqExact(cnick) AS uv
                FROM cdp_ods.mark_stat_xdrs_logs_medium_all
                WHERE `day` >= cast(
                        replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int
                    )
                    AND `day` <= cast(
                        replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int
                    )
                    AND shop_id = '{{ shop_id_var=6143eec86ebd17000d941c60 }}'
                GROUP BY snick
                ORDER BY snick
            ) t1
            INNER JOIN (
                SELECT t1.snick AS snick,
                    uniqExact(t1.cnick) AS effective_tag_uv
                FROM (
                        SELECT `day`,
                            splitByChar(':', snick) [2] AS snick,
                            cnick,
                            shop_id
                        FROM cdp_ods.mark_stat_xdrs_logs_medium_all
                        WHERE `day` >= cast(
                                replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int
                            )
                            AND `day` <= cast(
                                replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int
                            )
                            AND shop_id = '{{ shop_id_var=6143eec86ebd17000d941c60 }}'
                    ) t1
                    JOIN (
                        SELECT `day`,
                            cnick,
                            tag_id,
                            shop_id
                        FROM cdp_ods.tag_snapshot_all
                        WHERE `day` = (
                                SELECT max(`day`) AS `day`
                                FROM cdp_ods.tag_snapshot_all
                            )
                    ) t2 ON t1.shop_id = t2.shop_id
                    AND replace(t1.cnick, 'cnjd', '') = t2.cnick
                    JOIN (
                        SELECT tag_id,
                            group_name,
                            shop_id
                        FROM cdp_dim.wt_tag_all
                        WHERE group_name IN ('意向等级', '无效咨询')
                    ) t3 ON t2.tag_id = t3.tag_id
                    AND t2.shop_id = t3.shop_id
                GROUP BY t1.snick
                ORDER BY t1.snick
            ) t2 USING (snick)
        ORDER BY snick
    ) t1
    LEFT JOIN (
        SELECT t1.snick AS snick,
            t3.group_name AS group_name,
            t3.tag_name AS tag_name,
            uniqExact(t1.cnick) AS tag_cnt
        FROM (
                SELECT `day`,
                    splitByChar(':', snick) [2] AS snick,
                    cnick,
                    shop_id
                FROM cdp_ods.mark_stat_xdrs_logs_medium_all
                WHERE `day` >= cast(
                        replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int
                    )
                    AND `day` <= cast(
                        replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int
                    )
                    AND shop_id = '{{ shop_id_var=6143eec86ebd17000d941c60 }}'
            ) t1
            JOIN (
                SELECT `day`,
                    cnick,
                    tag_id,
                    shop_id
                FROM cdp_ods.tag_snapshot_all
                WHERE `day` = (
                        SELECT max(`day`) AS `day`
                        FROM cdp_ods.tag_snapshot_all
                    )
            ) t2 ON replace(t1.cnick, 'cnjd', '') = t2.cnick
            AND t1.shop_id = t2.shop_id
            JOIN (
                SELECT tag_id,
                    group_name,
                    tag_name,
                    shop_id
                FROM cdp_dim.wt_tag_all
            ) t3 ON t2.tag_id = t3.tag_id
            AND t2.shop_id = t3.shop_id
        GROUP BY t1.snick,
            t3.group_name,
            t3.tag_name
        ORDER BY t1.snick,
            t3.group_name,
            t3.tag_name
    ) t2 ON t1.snick = t2.snick
    INNER JOIN (
        SELECT snick,
            argMax(name, `day`) AS name
        FROM sxx_ods.snick_relation_daily_all
        WHERE shop_id = '{{ shop_id_var=6143eec86ebd17000d941c60 }}'
        GROUP BY snick
    ) t3 ON t1.snick = t3.snick
    INNER JOIN (
        SELECT snick,
            name1 as name
        FROM (
                SELECT splitByChar(':', subnick) [2] AS snick,
                    argMax(name, `day`) AS name1
                FROM ods.sub_user_group_all
                WHERE shop_id = '{{ shop_id_var=6143eec86ebd17000d941c60 }}'
                    and name = '售前客服'
                GROUP BY subnick
            ) -- WHERE name='售前客服'
    ) t4 ON t1.snick = t4.snick
HAVING "标签" != '电话顾客(人工)'
ORDER BY t1.snick,
    "标签排序",
    "标签"