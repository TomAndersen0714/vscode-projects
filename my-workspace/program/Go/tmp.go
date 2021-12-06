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

	sqlPartFour := "select a.platform as platform,'manual' as `type`,b.qc_id as qc_id, " +
		"b.qc_name_all as qc_name, round(b.count_info / a.count_all_info ,4) as qc_proportion from (select platform, sum(qc_count) as count_all_info from ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartFour)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartFive := "and `type` = 'manual' group by platform,`type`) as a left join  (select platform,`type`, qc_id, replaceAll(replaceAll(qc_name,'未设置一级标签/',''),'未设置二级标签/','') as qc_name_all, " +
		"sum(qc_count) as count_info from  ods.qc_question_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartFive)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and `type` = 'manual' and shop_name in %s ", startDate, endDate, shopStr))

	sqlPartSix := "group by platform,`type`, qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 union all select b.platform as platform, 'qc_word' as `type`, '' as qc_id, a.word " +
		"as qc_name, round((a.words_count_info/b.words_count_all),4) as qc_proportion from (select platform, word, sum(words_count) as words_count_info from ods.qc_words_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartSix)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and shop_name in %s ", startDate, endDate, shopStr))
	sqlPartSeven := "group by platform,word order by words_count_info desc) a left join (select platform, sum(words_count) as words_count_all from ods.qc_words_detail_all WHERE date >= "
	sqlBuilder.WriteString(sqlPartSeven)
	sqlBuilder.WriteString(fmt.Sprintf("%d and date < %d and shop_name in %s group by platform order by words_count_all desc limit 10) b on a.platform = b.platform order by qc_proportion desc limit 10", startDate, endDate, shopStr))

	return sqlBuilder.String()
}