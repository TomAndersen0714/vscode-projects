ALTER TABLE xqc_dwd.xplat_manual_tag_local ON CLUSTER cluster_3s_2r
DROP PARTITION (20230813, 'tb'),
DROP PARTITION (20230814, 'tb'),
DROP PARTITION (20230815, 'tb')

SELECT day, count(1)
FROM buffer.xqc_dwd_xplat_manual_tag_buffer
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day

SELECT day, count(1)
FROM ods.xinghuan_dialog_tag_score_all
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day


ALTER TABLE xqc_dws.xplat_snick_stat_local ON CLUSTER cluster_3s_2r
DROP PARTITION (20230813, 'tb'),
DROP PARTITION (20230814, 'tb'),
DROP PARTITION (20230815, 'tb')


SELECT day, count(1)
FROM xqc_dws.tag_stat_all
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day


SELECT day, count(1)
FROM buffer.xqc_dws_xplat_tag_stat_buffer
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day


ALTER TABLE xqc_dws.xplat_tag_stat_local ON CLUSTER cluster_3s_2r
DROP PARTITION (20230813, 'tb'),
DROP PARTITION (20230814, 'tb'),
DROP PARTITION (20230815, 'tb')

SELECT day, count(1)
FROM buffer.xqc_dws_xplat_snick_stat_buffer
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day

SELECT day, count(1)
FROM xqc_dws.snick_stat_all
WHERE day BETWEEN 20230813 AND 20230815
AND platform = 'tb'
GROUP BY day
ORDER BY day


