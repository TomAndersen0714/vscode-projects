WITH t1 AS (
    SELECT
        *
    FROM
        qc_dim.shop_all
    WHERE
        `day` = toYYYYMMDD(yesterday())
        AND toYYYYMMDD(toDate(expire_time)) >= `day`
        AND seller_nick GLOBAL NOT IN (
            SELECT
                seller_nick
            FROM
                xqc_dim.xqc_shop_all
            WHERE
                `day` = toYYYYMMDD(yesterday())
        )
),
t2 AS (
    SELECT
        shop_id,
        seller_nick AS plat_user_id,
        toDate(expire_time) AS expired_date
    FROM
        t1
),
t3 AS (
    SELECT
        shop_id,
        platform,
        recv_cnt
    FROM
        dipper.shop_overview_day_all
    WHERE
        `day` = toYYYYMMDD(yesterday())
),
t4 AS (
    SELECT
        *
    FROM
        t2
        LEFT JOIN t3 USING shop_id
    ORDER BY
        recv_cnt DESC
)
SELECT
    COUNT(1) AS `单店版在期店铺数`
FROM
    t4 -- trace:f85570dc68924bb55766e03042c6b8df