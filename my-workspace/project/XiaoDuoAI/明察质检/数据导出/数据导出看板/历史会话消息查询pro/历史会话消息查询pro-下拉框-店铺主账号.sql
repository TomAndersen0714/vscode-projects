-- 历史会话消息查询-下拉框-店铺主账号
-- PS: 没有绑定质检标准的, 则不展示
SELECT DISTINCT
    seller_nick
FROM ods.xinghuan_qc_norm_relate_all
WHERE day = toYYYYMMDD(yesterday())
-- 下拉框-平台
AND platform = '{{ platform=tb }}'