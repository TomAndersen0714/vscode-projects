-- 会话总量: 后端查询去除department_id, 由于子账号不定时对齐, 导致子账号不一定能关联上department_id, 即统计时不能加此过滤条件
-- xh-mc/handler/data_monitor.go:445
sqlBuilder.WriteString(fmt.Sprintf("%d and %d and shop_name in %s and department_id != '' ", startDate, endDate, shopStr))

sqlBuilder.WriteString(fmt.Sprintf("%d and %d and shop_name in %s ", startDate, endDate, shopStr))

-- AI质检问题Top10: 
-- xh-mc/handler/data_monitor.go:465
sqlPartThree := "group by platform,`type`,qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 UNION ALL "

sqlPartThree := "group by platform,`type`,qc_id,qc_name order by count_info desc ) as b on a.platform = b.platform order by qc_proportion desc UNION ALL "