-- 新实时告警-店铺告警-下拉框-获取告警等级
SELECT
    CONCAT(name,'//',level) AS name_level
FROM (
    SELECT
        ['初级告警','中级告警','高级告警'] AS name,
        ['1','2','3'] AS level
    FROM numbers(1)
) AS columns
ARRAY JOIN name, level
ORDER BY level ASC