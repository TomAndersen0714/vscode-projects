SELECT DISTINCT b.name AS "标准名称",
                g.full_name AS "分组",
                r.name AS "质检项名称",
                if(r.rule_category=1,'AI',if(r.rule_category=2,'人工','自定义')) AS "质检类型",
                r.check AS "是否检查",
                CASE
                    WHEN r.alert_level=0 THEN '不告警'
                    WHEN r.alert_level=1 THEN '初级告警'
                    WHEN r.alert_level=2 THEN '中级告警'
                    WHEN r.alert_level=3 THEN '高级高级'
                    ELSE '-'
                END AS "告警等级",
                if(r.notify_way=1,'自动通知',if(r.notify_way=2,'手动通知','')) AS "告警通知方式",
                r.score AS "质检分值"
FROM xqc_dim.qc_rule_all r,
     ods.xinghuan_qc_norm_all b,
     xqc_dim.qc_norm_group_full_all g
WHERE r.day = toYYYYMMDD(yesterday())
  AND r.day=b.day
  AND g.day=b.day
  AND r.platform = 'tb'
  AND r.platform=b.platform
  AND r.company_id = '6281c39e55fe7d13b95b5bcb'
  AND r.company_id=b.company_id
  AND r.qc_norm_id=b._id
  AND r.qc_norm_id='63058f491b58f39d18b97282'
  AND g.qc_norm_id=r.qc_norm_id
  AND r.qc_norm_group_id=g._id
  AND r.status=1
  
ORDER BY g.full_name