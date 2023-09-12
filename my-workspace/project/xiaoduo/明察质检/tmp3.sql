SELECT COUNT(1)
FROM remote('10.20.133.174:9000', 'dwd.xdqc_dialog_all')
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'dy'

UNION ALL
SELECT COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'dy'

UNION ALL
SELECT COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'ks'

UNION ALL
SELECT COUNT(1)
FROM remote('10.20.133.175:9000', 'dwd.xdqc_dialog_all')
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'ks'