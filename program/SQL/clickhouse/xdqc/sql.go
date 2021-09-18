func buildGetAlertInfoListSqlCondition1(ctx context.Context, req *proto.AlertInfoListReq, group *model.Group) (string, error) {
	condition := fmt.Sprintf("company_id = %s AND is_shop = 'True'", req.CompanyId.Hex())

	if !req.BG.IsZero() {
		node := service.FindNodeFromTree(group.SubGroups, req.BG)
		if node != nil {
			condition += fmt.Sprintf(" AND parent_department_path[1] = %s", req.BG)
		}
	} else {
		condition += " AND parent_department_path[1] != ''"
	}

	if !req.BU.IsZero() {
		node := service.FindNodeFromTree(group.SubGroups, req.BG)
		if node != nil {
			condition += fmt.Sprintf(" AND parent_department_path[2] = %s", req.BU)
		}
	} else {
		condition += " AND parent_department_path[2] != ''"
	}
	if !req.Shop.IsZero() {
		shop, err := model.FindOneShop(ctx, bson.M{"company_id": req.CompanyId, "shop_id": req.Shop})
		if err != nil {
			return "", err
		}
		condition += fmt.Sprintf(" AND shop_name = %s", shop.SellerNick)
	}

	return condition, nil
}

func buildGetAlertInfoListSqlCondition2(req *proto.AlertInfoListReq, allowedSnicks []string) string {
	condition := fmt.Sprintf("company_id = %s", req.CompanyId.Hex())
	if len(allowedSnicks) > 0 {
		condition += fmt.Sprintf(" AND snick IN [%s]", strings.Join(allowedSnicks, ","))
	} else {
		condition += " AND snick !=''"
	}
	return condition
}

func buildGetAlertInfoListSqlCondition3(req *proto.AlertInfoListReq, allowedSnicks []string) string {
	startDate := utils.MsTimeStampToInt(req.StartTime)
	endDate := utils.MsTimeStampToInt(req.EndTime)
	condition := fmt.Sprintf("day BETWEEN %d AND %d", startDate, endDate)

	if req.IsFinished != nil {
		condition += fmt.Sprintf(" AND is_finished = %v", *req.IsFinished)
	} else {
		condition += " AND is_finished != ''"
	}

	if req.Level > 0 {
		condition += fmt.Sprintf(" AND level = %d", req.Level)
	} else {
		condition += " AND level != 0"
	}

	if len(req.WarningType) > 0 {
		condition += fmt.Sprintf(" AND warning_type = %s", req.WarningType)
	} else {
		condition += " AND warning_type != ''"
	}

	if len(allowedSnicks) > 0 {
		condition += fmt.Sprintf(" AND snick IN [%s]", strings.Join(allowedSnicks, ","))
	} else {
		condition += " AND snick !=''"
	}

	if len(req.Keyword) > 0 {
		condition += fmt.Sprintf(" AND (snick LIKE '%[1]s' OR employee_name LIKE '%[1]s' OR superior_name LIKE '%[1]s')", "%"+req.Keyword+"%")
	}
	return condition
}