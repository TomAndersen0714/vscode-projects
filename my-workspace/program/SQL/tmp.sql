WITH x0 AS (
    SELECT id,
        platform,
        `level`,
        warning_type,
        `time`,
        seller_nick,
        shop_id,
        snick,
        cnick,
        employee_name
    FROM xqc_ods.alert_all FINAL
    WHERE `day` >= 20220124
        AND `day` <= 20220124
        AND `time` >= '2022-01-24 14:57:36'
        AND `time` <= '2022-01-24 15:57:36'
        AND shop_id IN (
            '5bfe7a6a89bc4612f16586a5',
            '5f1f97bdfbb9ba0017f73f18',
            '5f74643b6868e200013e6d46',
            '5f8ff0c0a3967d00188dca48',
            '613ef1e1ec7097000e494123',
            '61c16f73e8e6fc3cd46906a4'
        )
        AND is_finished = 'False'
        AND `level` IN (2, 3)
),
x1 AS (
    SELECT alert_id,
        round,
        resp_code
    FROM xqc_ods.alert_remind_all
    WHERE `day` >= 20220124
        AND `day` <= 20220124
        AND `time` >= '2022-01-24 14:57:36'
        AND `time` <= '2022-01-24 15:57:36'
        AND shop_id IN (
            '5bfe7a6a89bc4612f16586a5',
            '5f1f97bdfbb9ba0017f73f18',
            '5f74643b6868e200013e6d46',
            '5f8ff0c0a3967d00188dca48',
            '613ef1e1ec7097000e494123',
            '61c16f73e8e6fc3cd46906a4'
        )
        AND `source` = 1
        AND notify_type = 1
        AND alert_id GLOBAL IN (
            SELECT id
            FROM x0
        )
),
x2 AS (
    SELECT alert_id,
        round
    FROM x1
    GROUP BY alert_id,
        round
),
x3 AS (
    SELECT alert_id,
        round,
        count(1) AS success
    FROM x1
    WHERE resp_code = 0
    GROUP BY alert_id,
        round
),
x4 AS (
    SELECT alert_id,
        round,
        count(1) AS fail
    FROM x1
    WHERE resp_code != 0
    GROUP BY alert_id,
        round
),
x5 AS (
    SELECT x2.alert_id AS alert_id,
        x2.round AS round,
        success,
        fail
    FROM x2 GLOBAL
        LEFT JOIN x3 ON x2.alert_id = x3.alert_id
        AND x2.round = x3.round GLOBAL
        LEFT JOIN x4 ON x2.alert_id = x4.alert_id
        AND x2.round = x4.round
),
x6 AS (
    SELECT alert_id,
        max(round) AS round
    FROM x5
    GROUP BY alert_id
),
x7 AS (
    SELECT x5.*
    FROM x5 GLOBAL
        JOIN x6 ON x5.alert_id = x6.alert_id
        AND x5.round = x6.round
),
x8 AS (
    SELECT id,
        platform,
        `level`,
        warning_type,
        `time`,
        seller_nick,
        shop_id,
        snick,
        cnick,
        employee_name,
        round,
        success,
        fail
    FROM x0 GLOBAL
        LEFT JOIN x7 ON x0.id = x7.alert_id
)
SELECT x8.*,
    shop_info.department_name AS shop_name
FROM x8 GLOBAL
    LEFT JOIN xqc_dim.group_all AS shop_info ON x8.shop_id = shop_info.department_id