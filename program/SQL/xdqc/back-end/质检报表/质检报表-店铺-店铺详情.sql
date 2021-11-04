func BuildShopDetailSql(ctx *gin.Context) string {
	req := api.DefaultRequest(ctx).(*protocol.ShopDetailReq)
	sess := api.GetCtxSession(ctx)
	s0 := "select shop_name,platform,department_name,department_id,employee_name,employee_id,all_count,ai_count,ai_abnormal_count,ai_excellent_count,read_count,suggest_count,check_rate,tag_score_count,tag_add_score_count,round(avg_score,2) as " +
		" avg_score, concat(CAST(round((ai_abnormal_count*100/all_count), 2), 'String'), '%') AS ai_abnormal_rate,concat(CAST(round((tag_score_count*100/all_count), 2), 'String'), '%') AS tag_score_rate" +
		" from ("
	s1 := " select company_id,seller_nick as shop_name,platform,department_id,department_name,employee_name,employee_id," +
		"sum(sessionc_count) as all_count,sum(qc_count) as ai_count,sum(abnormals_count) as ai_abnormal_count," +
		"sum(excellents_count) as ai_excellent_count,sum(read_mark_count) as read_count,round((0.9604*all_count)/(0.0025*all_count+0.9604), 0) as suggest_count," +
		"concat(CAST(round((read_count*100/all_count), 2) , 'String'), '%') as check_rate,sum(tag_score_stats_count) as tag_score_count,sum(tag_score_add_stats_count) as tag_add_score_count " +
		"from ods.qc_statistical_all "

	wherePart := ""
	if req.DepartmentId == "" {
		wherePart = fmt.Sprintf("where platform = '%s' and seller_nick = '%s' and company_id = '%s' and date >= %d and date <= %d ", sess.Platform, req.ShopName, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
	} else {
		departmentIds, _, err := service.GetSubDepartment(bson.ObjectIdHex(req.DepartmentId))
		if err != nil {
			logrus.Errorf("get sub department error %+v", err)
			return ""
		}
		wherePart = fmt.Sprintf("where platform = '%s' and has(%s, department_id) and seller_nick = '%s' and company_id = '%s' and date >= %d and date <= %d ", sess.Platform, service.BuildSqlArrayById(departmentIds), req.ShopName, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
	}

	if req.EmployeeId != "" {
		wherePart = fmt.Sprintf("and employee_id = '%s' ", req.EmployeeId)
	}
	s1 += wherePart

	s1 += "group by company_id,seller_nick,platform,department_id,department_name,employee_name,employee_id) as emp left join ("

	s2 := "SELECT company_id,shop_name,platform,department_id,department_name,employee_id," +
		"(sum(session_count) *100 +sum(ai_add_score) -sum(ai_subtract_score)) /sum(session_count) AS avg_score" +
		" FROM ods.qc_session_count_all " + strings.ReplaceAll(wherePart, "seller_nick", "shop_name") + " group by company_id,shop_name,platform,department_name,department_id, employee_name,employee_id) as score using(company_id,department_id, employee_id) "
	return s0 + s1 + s2
}