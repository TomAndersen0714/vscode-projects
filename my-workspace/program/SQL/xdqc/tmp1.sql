CREATE VIEW app_csm.shop_nick_view AS WITH old_shop AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY platform,
            plat_user_id
            ORDER BY `_id` DESC
        ) rank_number
    FROM dim.shop_nick
),
t_base AS (
    SELECT new_shop.shop_id `_id`,
        new_shop.category_name category_zh,
        new_shop.create_time,
        new_shop.expire_time,
        new_shop.update_time,
        new_shop.plat_shop_name,
        new_shop.plat_user_id,
        new_shop.platform,
        old_shop.xd_shop_nick,
        old_shop.category,
        old_shop.version,
        old_shop.plat_shop_id,
        old_shop.plat_shop_cid,
        old_shop.user_id,
        old_shop.reminder_version,
        old_shop.account_limit
    FROM dim.platform_shop_nick new_shop
        LEFT OUTER JOIN old_shop ON new_shop.platform = old_shop.platform
        AND old_shop.rank_number = 1
        AND new_shop.plat_user_id = old_shop.plat_user_id
),
t1_0 AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY platform,
            main_nick
            ORDER BY `_id` DESC
        ) rank_number
    FROM app_crm.ods_fxxk_shop
),
t1 AS (
    SELECT `_id`,
        main_nick,
        platform_map.value platform,
        create_time,
        shop_name,
        company_id,
        `status`
    FROM t1_0
        LEFT OUTER JOIN app_csm.platform_map platform_map ON t1_0.platform = platform_map.name
    WHERE rank_number = 1
),
t2_0 AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY customer_name
            ORDER BY customer_name ASC,
                impl_owner DESC
        ) rank_number
    FROM app_crm.ods_fxxk_order
),
t2 AS (
    SELECT t_base.*,
        customer.`_id` customer_id,
        customer.name customer_name,
        customer.maintainer,
        customer.`owner` salesman,
        t2_0.impl_owner,
        t2_0.impl_owner implman,
        t2_0.train_owner,
        customer.province,
        customer.city
    FROM t_base
        LEFT OUTER JOIN t1 ON t1.main_nick = t_base.plat_user_id
        AND t1.platform = t_base.platform
        LEFT OUTER JOIN app_crm.ods_fxxk_customer customer ON t1.company_id = customer.`_id`
        LEFT OUTER JOIN t2_0 ON t1.company_id = t2_0.customer_name
        AND t2_0.rank_number = 1
)
SELECT *
FROM t2