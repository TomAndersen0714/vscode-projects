-- 近30日会话量趋势-当前平台已订阅店铺
SELECT
    tenant_label AS shop_name
FROM xqc_dim.company_tenant
WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
AND platform = '{{ platform=tb }}'
UNION ALL
SELECT '全部' AS shop_name


-- 近30日会话量趋势-柱状图和折线图
WITH
(SELECT toYYYYMMDD(today())) AS today,
(SELECT toYYYYMMDD(today()-30)) AS 30_days_ago,
(SELECT toYYYYMMDD(today()-60)) AS 60_days_ago
SELECT
    near_30_day_dialog_stat.day,
    near_30_day_dialog_stat.dialog_cnt AS near_30_day_dialog_cnt,
    last_30_day_dialog_stat.dialog_cnt AS last_30_day_dialog_cnt
FROM (
    SELECT
        day,
        date,
        dialog_cnt
    FROM (
        SELECT
            day,
            count(1) AS dialog_cnt
        FROM xqc_ods.dialog_all
        WHERE day > 30_days_ago AND day <= today
        AND if('{{shop_name}}'!='全部',seller_nick = '{{shop_name}}', seller_nick!='')
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = '{{ platform=tb }}'
        )
        GROUP BY day
    ) AS dialog_stat
    GLOBAL RIGHT JOIN (
        -- 固定时间轴
        SELECT day, date
        FROM (
            SELECT
                arrayMap(
                        x->toYYYYMMDD(toDate(x)),
                        range(toUInt32(today()-29), toUInt32(today()+1), 1)
                ) AS day,
                arrayMap(
                        x->toDate(x),
                        range(toUInt32(today()-29), toUInt32(today()+1), 1)
                ) AS date
        )
        ARRAY JOIN
            day, date
    )
    USING day
) AS near_30_day_dialog_stat
GLOBAL LEFT JOIN (
    SELECT
        day,
        date,
        dialog_cnt
    FROM (
        SELECT
            day,
            count(1) AS dialog_cnt
        FROM xqc_ods.dialog_all
        WHERE day > 60_days_ago AND day <= 30_days_ago
        AND if('{{shop_name}}'!='全部',seller_nick = '{{shop_name}}', seller_nick!='')
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = '{{ platform=tb }}'
        )
        GROUP BY day
    ) AS dialog_stat
    GLOBAL RIGHT JOIN (
        -- 固定时间轴
        SELECT day, date
        FROM (
            SELECT
                arrayMap(
                        x->toYYYYMMDD(toDate(x)),
                        range(toUInt32(today()-59), toUInt32(today()-29), 1)
                ) AS day,
                arrayMap(
                        x->toDate(x),
                        range(toUInt32(today()-59), toUInt32(today()-29), 1)
                ) AS date
        )
        ARRAY JOIN
            day, date
    )
    USING day
) AS last_30_day_dialog_stat
ON near_30_day_dialog_stat.date - 30 = last_30_day_dialog_stat.date
ORDER BY day DESC
