-- 新实时告警-店铺告警-下拉框-获取告警处理状态
SELECT
    CONCAT(state,'//',label) AS alert_state
FROM (
    SELECT
        ['已处理','未处理'] AS state,
        ['True','False'] AS label
    FROM numbers(1)
) AS columns
ARRAY JOIN
    state, label