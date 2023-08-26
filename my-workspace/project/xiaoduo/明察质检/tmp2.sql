DELETE FROM polaris.shop_overview_dd
WHERE day = 20230823
AND platform = 'ks'
AND shop_id IN (
    SELECT DISTINCT shop_id
    FROM (
        SELECT day, shop_id, count(1) AS cnt
        FROM polaris.shop_overview_dd
        WHERE day = 20230823
        AND shop_id != ''
        GROUP BY day, shop_id
        HAVING count(1) > 1
    ) AS tmp
)


DELETE FROM polaris.shop_overview_dd
WHERE day = 20230820
AND platform = 'ks'
AND shop_id IN (
    SELECT DISTINCT shop_id
    FROM (
        SELECT day, shop_id, count(1) AS cnt
        FROM polaris.shop_overview_dd
        WHERE day = 20230820
        AND shop_id != ''
        GROUP BY day, shop_id
        HAVING count(1) > 1
    ) AS tmp
)

INSERT INTO tmp.shop_overview_dd_0825
SELECT * FROM polaris.shop_overview_dd
WHERE day = 20230820

INSERT INTO tmp.shop_overview_dd_0825
SELECT * FROM polaris.shop_overview_dd
WHERE day = 20230823