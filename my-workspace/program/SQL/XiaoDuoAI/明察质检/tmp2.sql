-- 测试用例1: 对比会话总量
SELECT day, COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN 20221120
    AND 20221123
AND seller_nick = '方太官方旗舰店'
GROUP BY toYYYYMMDD(begin_time) AS day
ORDER BY day

-- 测试用例2: 对比已检会话总量
SELECT
    day, COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN 20221120
    AND 20221123
AND seller_nick = '方太官方旗舰店'
AND last_mark_id != ''
GROUP BY toYYYYMMDD(begin_time) AS day
ORDER BY day

-- 测试用例3: 对比会话总量和已检会话量
SELECT day, COUNT(1), SUM(last_mark_id!='')
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date.start}}'))
    AND toYYYYMMDD(toDate('{{date.end}}'))
AND seller_nick = '木月瑞希尔'
GROUP BY toYYYYMMDD(begin_time) AS day
ORDER BY day