INSERT INTO xqc_dws.qc_norm_stat_all WITH qc_norm AS (
        SELECT *
        FROM ods.xinghuan_qc_norm_all
        WHERE `day` = toYYYYMMDD(yesterday())
    ),
    customer AS (
        SELECT *
        FROM ods.xinghuan_company_all
        WHERE `day` = toYYYYMMDD(yesterday())
    ),
    shop AS (
        SELECT distinct(seller_nick, platform),
            shop_id,
            seller_nick,
            platform,
            plat_shop_name
        FROM xqc_dim.xqc_shop_all
        WHERE `day` = toYYYYMMDD(yesterday())
    ),
    rule_all AS (
        SELECT *
        FROM xqc_dim.qc_rule_all
        WHERE `day` = toYYYYMMDD(yesterday())
    ),
    group_info AS (
        SELECT *
        FROM xqc_dim.qc_norm_group_full_all
        WHERE `day` = toYYYYMMDD(yesterday())
    ),
    tags AS (
        SELECT *
        FROM xqc_dws.tag_stat_all
        WHERE `day` = { { ds_nodash } }
    ),
    qc_base_v2 AS (
        SELECT qc_norm._id AS qc_norm_id,
            qc_norm.company_id AS company_id,
            qc_norm.name AS qc_norm_name,
            rule_all._id AS qc_rule_id,
            rule_all.name AS qc_rule_name,
            rule_all.rule_category AS rule_category,
            rule_all.rule_type AS rule_type,
            rule_all.check AS is_check,
            rule_all.status AS status,
            rule_all.alert_level AS alert_level,
            rule_all.notify_way AS notify_way,
            rule_all.notify_target AS notify_target,
            rule_all.qc_norm_group_id AS qc_norm_group_id
        FROM qc_norm
            LEFT JOIN rule_all ON rule_all.qc_norm_id = qc_norm._id
    ),
    qc_base_v3 AS (
        SELECT qc_base_v2.*,
            group_info.name AS qc_norm_group_name,
            group_info.full_name AS qc_norm_group_full_name
        FROM qc_base_v2
            LEFT JOIN group_info ON group_info._id = qc_base_v2.qc_norm_group_id
    ),
    qc AS (
        SELECT tags.day AS `day`,
            qc_base_v3.company_id AS company_id,
            customer.name AS company_name,
            tags.platform AS platform,
            shop.shop_id AS shop_id,
            shop.plat_shop_name AS shop_name,
            tags.seller_nick AS seller_nick,
            qc_base_v3.qc_norm_group_id AS qc_norm_group_id,
            qc_base_v3.qc_norm_group_name AS qc_norm_group_name,
            qc_base_v3.qc_norm_group_full_name AS qc_norm_group_full_name,
            qc_base_v3.qc_norm_id AS qc_norm_id,
            qc_base_v3.qc_norm_name AS qc_norm_name,
            qc_base_v3.qc_rule_id AS qc_rule_id,
            qc_base_v3.qc_rule_name AS qc_rule_name,
            qc_base_v3.rule_category AS rule_category,
            qc_base_v3.rule_type AS rule_type,
            qc_base_v3.is_check AS is_check,
            qc_base_v3.status AS status,
            qc_base_v3.alert_level AS alert_level,
            qc_base_v3.notify_way AS notify_way,
            qc_base_v3.notify_target AS notify_target,
            tags.tag_cnt_sum AS trigger_cnt
        FROM tags
            LEFT JOIN qc_base_v3 ON tags.tag_id = qc_base_v3.qc_rule_id
            LEFT JOIN customer ON customer._id = qc_base_v3.company_id
            LEFT JOIN shop ON shop.seller_nick = tags.seller_nick
            AND shop.platform = tags.platform
    )
SELECT *
FROM qc