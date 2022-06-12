-- 客户评价满意度(二期)-统计-评价转化趋势
SELECT
    day,
    stat.*
FROM (
    SELECT '全部次数' AS eval_type, 100 AS cnt
    UNION ALL
    SELECT '评价次数' AS eval_type, 80 AS cnt
    UNION ALL
    SELECT '好评次数' AS eval_type, 50 AS cnt
    UNION ALL
    SELECT '挽回次数' AS eval_type, 20 AS cnt
) AS stat
GLOBAL CROSS JOIN (
    SELECT arrayJoin(
        arrayMap(
            x->toYYYYMMDD(toDate(x)),
            range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=today }}') + 1), 1)
        )
    ) AS day
) AS day_axis