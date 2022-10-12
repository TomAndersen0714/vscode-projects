SELECT a.shop_id AS "店铺id",
    a.`day` AS "日期",
    a.uv AS "咨询顾客数",
    a.effective_tag_uv AS "有效打标顾客数",
    a.effective_tag_rat AS "有效打标顾客占比",
    b.uv AS "新增顾客数",
    b.effective_tag_uv AS "新增顾客有效打标数",
    b.effective_tag_rat AS "新增顾客打标率"
FROM (
    SELECT '6143f37218f6b6000e173bc3' AS shop_id,
        a.`day` AS `day`,
        a.uv AS uv,
        b.effective_tag_uv AS effective_tag_uv,
        concat(
            toString(round(effective_tag_uv * 100 / uv, 2)),
            '%'
        ) AS effective_tag_rat
    FROM (
        SELECT `day`,
            uniqExact(cnick) AS uv
        FROM cdp_ods.mark_stat_xdrs_logs_medium_all
        WHERE `day` >= 20221007
            AND `day` <= 20221009
            AND shop_id = '6143f37218f6b6000e173bc3'
        GROUP BY `day`
        ORDER BY `day`
    ) AS a
    JOIN (
        SELECT t1_t2.day AS `day`,
            uniqExact(t1_t2.cnick) AS effective_tag_uv
        FROM (
            SELECT
                `day`, shop_id, cnick, tag_id
            FROM (
                SELECT `day`, cnick, shop_id
                FROM cdp_ods.mark_stat_xdrs_logs_medium_all
                WHERE `day` >= 20221007
                AND `day` <= 20221009
                AND shop_id = '6143f37218f6b6000e173bc3'
            ) AS t1
            JOIN (
                SELECT `day`, cnick, tag_id, shop_id
                FROM cdp_ods.tag_snapshot_all
                WHERE tag_id GLOBAL IN (
                    SELECT DISTINCT
                        tag_id
                    FROM cdp_dim.wt_tag_all
                    WHERE group_name IN('意向等级', '无效咨询')
                    AND shop_id = '6143f37218f6b6000e173bc3'
                )
                AND `day` >= 20221007
                AND `day` <= 20221009
                AND shop_id = '6143f37218f6b6000e173bc3'
            ) AS t2
            ON t1.`day` = t2.`day`
            AND t1.cnick = t2.cnick
            AND t1.shop_id = t2.shop_id
        ) AS t1_t2
        GROUP BY t1_t2.day
        ORDER BY t1_t2.day
    ) AS b
    USING(`day`)
) AS a
JOIN (
    SELECT
        '6143f37218f6b6000e173bc3' AS shop_id,
        a.`day` AS `day`,
        a.uv AS uv,
        b.effective_tag_uv AS effective_tag_uv,
        concat(
            toString(round(effective_tag_uv * 100 / uv, 2)),
            '%'
        ) AS effective_tag_rat
    FROM (
        SELECT `day`,
            uniqExact(cnick) AS uv
        FROM sxx_dws.snick_new_ask_detail_daily_all
        WHERE `day` >= 20221007
            AND `day` <= 20221009
            AND shop_id = '6143f37218f6b6000e173bc3'
        GROUP BY `day`
        ORDER BY `day`
    ) AS a
    JOIN (
        SELECT t1.day AS `day`,
            uniqExact(t1.cnick) AS effective_tag_uv
        FROM (
            SELECT `day`, cnick, shop_id
            FROM sxx_dws.snick_new_ask_detail_daily_all
            WHERE `day` >= 20221007
            AND `day` <= 20221009
            AND shop_id = '6143f37218f6b6000e173bc3'
        ) t1
        JOIN (
            SELECT `day`, cnick, tag_id, shop_id
            FROM cdp_ods.tag_snapshot_all
            WHERE `day` >= 20221007
            AND `day` <= 20221009
            AND shop_id = '6143f37218f6b6000e173bc3'
        ) t2 ON t1.`day` = t2.`day`
        AND t1.cnick = t2.cnick
        AND t1.shop_id = t2.shop_id
        JOIN (
            SELECT tag_id, group_name, shop_id
            FROM cdp_dim.wt_tag_all
            WHERE group_name IN('意向等级', '无效咨询')
        ) t3 ON t2.tag_id = t3.tag_id
        AND t2.shop_id = t3.shop_id
        GROUP BY t1.day
        ORDER BY t1.day
    ) AS b
    USING(`day`)
) AS b
USING(shop_id, `day`)