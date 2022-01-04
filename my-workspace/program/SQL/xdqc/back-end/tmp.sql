
req := api.DefaultRequest(ctx).(*protocol.KefuHumanResultReq)
s1 := "select shop_name,snick,department_id,total_score,total_score_add,toString(check_human) as check_human,name,tag_id,score,cal_op,tag_score from (" +
	"select seller_nick,snick,department_id,mark_list as check_human,name,tag_id,score,cal_op,tag_score from " +
	fmt.Sprintf("(select seller_nick,snick,department_id,arrayReduce('groupUniqArray',flatten(groupArray(mark_list))) as mark_list from ods.qc_statistical_all where date between %d and %d and employee_id = '%s' ", req.StartDate, req.EndDate, req.EmployeeId)
if req.ShopName != "" {
	s1 += fmt.Sprintf("and seller_nick = '%s'", req.ShopName)
}

s1 += " group by seller_nick,snick,department_id) as qc_statistical " +
	"left join (" +
	"select snick,name,tag_id,cal_op,score,sum(score) as tag_score from  ods.xinghuan_dialog_tag_score_all " +
	fmt.Sprintf("where day between toInt32(toYYYYMMDD(toDate(%d))) and toInt32(toYYYYMMDD(toDate(%d))) ", req.StartDate, req.EndDate) +
	"group by snick,name,tag_id,cal_op ,score" +
	") as tag_score " +
	"on qc_statistical.snick = tag_score.snick " +
	") as score left join (select seller_nick as shop_name,snick,department_id,sum(mark_score) as total_score,sum(mark_score_add) as total_score_add " +
	fmt.Sprintf("from ods.qc_statistical_all where employee_id = '%s' and `date` >= %d and `date` <= %d ", req.EmployeeId, req.StartDate, req.EndDate) +
	"group by seller_nick,snick,department_id) as tag on score.snick = tag.snick and score.seller_nick = tag.shop_name and score.department_id = tag.department_id "

if req.DepartmentId != "" {
	departmentIds, _, err := service.GetSubDepartment(bson.ObjectIdHex(req.DepartmentId))
	if err != nil {
		logrus.Errorf("get sub department error %+v", err)
		return ""
	}
	s1 += fmt.Sprintf("where has(%s, department_id) ", service.BuildSqlArrayById(departmentIds))
}

 return s1