func buildGetAlertInfoListSql(ctx context.Context, count bool, req *proto.AlertInfoListReq, withoutLimit bool) (string, error) {
	base := `
SELECT
	%s
FROM (
		SELECT
            shop_info.company_id AS company_id,
            shop_info.bg_id AS bg_id,
            bg_info.department_name AS BG,
            shop_info.bu_id AS bu_id,
            bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM (
            SELECT
                parent_department_path[1] AS bg_id,
                parent_department_path[2] AS bu_id,
                parent_department_path,
                company_id,
                department_id,
                department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'True'
        ) AS shop_info
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'False'
        ) AS bg_info
        ON shop_info.bg_id = bg_info.department_id
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'False'
        ) AS bu_info
        ON shop_info.bu_id = bu_info.department_id
        WHERE %s
)
GLOBAL INNER JOIN(
    SELECT *
    FROM xqc_ods.alert_all FINAL
    WHERE %s
) AS alert_info
USING shop_id
`

	selectInfo := `
	BG, BU,
    shop_name,
    superior_name,
    employee_name,
    snick,
    cnick,
    dialog_id,
    message_id,
	alert_info.id AS alert_id,
    level,
    warning_type,
    time,
	time as warning_time,
    toInt64(if(
        is_finished='True',
        round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    )) AS warning_duration,
    finish_time,
    is_finished
`
	if count {
		selectInfo = "count(1) as count"
	}
	group, err := model.FindOneGroup(ctx, req.CompanyId)
	if err != nil {
		return "", errors.WithStack(err)
	}
	allowedShopIds, err := getAllowedShopIds(ctx, req)
	if err != nil {
		return "", err
	}
	allowedSnicks, err := getAllowedSnicks(ctx, req)
	if err != nil {
		return "", err
	}

	condition1, err := buildGetAlertInfoListSqlCondition1(ctx, req, group)
	if err != nil {
		return "", err
	}
	//condition2 := buildGetAlertInfoListSqlCondition2(req, allowedShopIds, allowedSnicks)
	condition2 := buildGetAlertInfoListSqlCondition2(req, allowedShopIds, allowedSnicks)

	sql := fmt.Sprintf(base, selectInfo, condition1, condition2)
	if count {
		return sql, nil
	}
	if req.Page == 0 {
		req.Page = 1
	}
	if req.PageSize == 0 {
		req.PageSize = 10
	}
	if len(req.SortField) == 0 {
		req.SortField = "time"
	}
	if len(req.SortType) == 0 {
		req.SortType = "DESC"
	}
	if withoutLimit {
		return withLimitAndOffset(sql, 10000, 0), nil
	}
	return withLimitAndOffset(withOrderBy(sql, req.SortField, req.SortType), req.PageSize, (req.Page-1)*req.PageSize), nil
}