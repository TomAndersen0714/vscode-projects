-- 人工质检-会话扣分标签
SELECT a.seller_nick,
    a.platform,
    a.`group`,
    a.snick,
    a._id AS dialog_id,
    a.cnick,
    a.tag_id,
    b.tag_name,
    a.tag_score,
    0 AS cal_op,
    {ds_nodash}
FROM (
    SELECT platform,
        seller_nick,
        `group`,
        snick,
        _id,
        cnick,
        tag_id,
        tag_score
    FROM dwd.xdqc_dialog_all
    ARRAY JOIN
        tag_score_stats_id AS tag_id,
        tag_score_stats_score AS tag_score
    WHERE toYYYYMMDD(begin_time) = {ds_nodash}
    AND length(tag_score_stats_id) > 0
) AS a
LEFT JOIN (
    -- 关联人工质检项
    SELECT
        _id AS tag_id,
        name AS tag_name
    FROM xqc_dim.qc_rule_all
    WHERE day = {snapshot_ds_nodash}
    AND rule_category = 2
) AS b 
USING(tag_id)


-- 人工质检-会话加分标签
SELECT
    a.seller_nick,
    a.platform,
    a.`group`,
    a.snick,
    a._id AS dialog_id,
    a.cnick,
    a.tag_id,
    b.tag_name,
    a.tag_score,
    1 AS cal_op,
    {ds_nodash}
FROM (
    SELECT platform,
        seller_nick,
        `group`,
        snick,
        _id,
        cnick,
        tag_id,
        tag_score
    FROM dwd.xdqc_dialog_all
    ARRAY JOIN
        tag_score_add_stats_id AS tag_id,
        tag_score_add_stats_score AS tag_score
    WHERE toYYYYMMDD(begin_time) = {ds_nodash}
    AND length(tag_score_add_stats_id) > 0
) AS a
LEFT JOIN (
    -- 关联人工质检项
    SELECT
        _id AS tag_id,
        name AS tag_name
    FROM xqc_dim.qc_rule_all
    WHERE day = {snapshot_ds_nodash}
    AND rule_category = 2
) AS b
USING(tag_id)