SELECT '{{ a1=yesterday }}' AS begin_date, '昨日({{ a2=yesterday }})' AS describe, concat(describe,'//',begin_date) AS item
UNION ALL
SELECT '{{ b1=week_ago }}' AS begin_date, '近一周({{ b2=week_ago }}~{{ d=yesterday }})' AS describe, concat(describe,'//',begin_date) AS item
UNION ALL
SELECT '{{ c1=month_ago }}' AS begin_date, '近一月({{ c2=month_ago }}~{{ d=yesterday }})' AS describe, concat(describe,'//',begin_date) AS item