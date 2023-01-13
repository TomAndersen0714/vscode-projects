-- 会话总量
-- PS: 由于未分组子账号无法获取到department_id, 因此原逻辑中未分组子账号无法计入质检总览页面的会话总量统计中
xh-mc/handler/data_monitor.go:445
sqlBuilder.WriteString(fmt.Sprintf("%d and %d and shop_name in %s and department_id != '' ", startDate, endDate, shopStr))

sqlBuilder.WriteString(fmt.Sprintf("%d and %d and shop_name in %s ", startDate, endDate, shopStr))

-- AI质检异常会话量
-- PS: 由于score字段被修改为统计扣分总数(Ai质检+人工质检+自定义质检), 因此在计算AI异常质检量时, 需要将其剥离出来
mini_dags/dialog_transfer.py:1190
mini_dags/dialog_transfer.py:2276
ods.qc_session_count_all 表统计逻辑中:
	sum(if(score > 0,1,0)) as ai_subtract_score_count
修改为: AI质检扣分=总扣分(score)-自定义质检扣分(rule_score_info)-人工质检扣分(mark_score)
	sum(if(score - rule_score_info -  mark_score > 0,1,0)) as ai_subtract_score_count


-- AI质检问题Top10
-- PS: 修正未实现过滤正面情感, 导致扣分问题统计百分比之和不足一百, 同时各问题百分比偏低的问题
xh-mc/handler/data_monitor.go:455
func BuildQcSql(startDate, endDate int64, shopName []string) string {
	shopStr := BuildShopStr(shopName)
	var sqlBuilder strings.Builder
	sqlPartOne := "select a.platform as platform ,type,b.qc_id as qc_id,b.qc_name as qc_name , round(b.count_info / a.count_all_info ,4) as qc_proportion from (select platform, " +
		"sum(qc_count) as count_all_info from  ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartOne)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartTwo := "and `type` in ('ai', 's_emotion', 'c_emotion') group by platform) as a left join (select platform,`type`, qc_id, qc_name, sum(qc_count) as count_info from ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartTwo)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and `type` in ('ai', 's_emotion', 'c_emotion') and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartThree := "group by platform,`type`,qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 UNION ALL "
	sqlBuilder.WriteString(sqlPartThree)

func BuildQcSql(startDate, endDate int64, shopName []string) string {
	shopStr := BuildShopStr(shopName)
	var sqlBuilder strings.Builder
	sqlPartOne := "select a.platform as platform ,type,b.qc_id as qc_id,b.qc_name as qc_name , round(b.count_info / a.count_all_info ,4) as qc_proportion from (select platform, " +
		"sum(qc_count) as count_all_info from  ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartOne)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartTwo := "and (`type` = 'ai' OR (`type` = 's_emotion') OR (`type` = 'c_emotion' AND qc_id>='4')) group by platform) as a left join (select platform,`type`, qc_id, qc_name, sum(qc_count) as count_info from ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartTwo)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and (`type` = 'ai' OR (`type` = 's_emotion') OR (`type` = 'c_emotion' AND qc_id>='4')) and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartThree := "group by platform,`type`,qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 UNION ALL "
	sqlBuilder.WriteString(sqlPartThree)

-- 