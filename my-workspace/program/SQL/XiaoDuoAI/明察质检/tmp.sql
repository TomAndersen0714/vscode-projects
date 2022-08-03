type QcTask struct {
	Id         bson.ObjectId `bson:"_id" json:"id"`
	CreateTime time.Time     `bson:"create_time" json:"create_time"`
	UpdateTime time.Time     `bson:"update_time" json:"update_time"`
	Platform   string        `bson:"platform" json:"platform"`
	CompanyId  bson.ObjectId `bson:"company_id" json:"company_id"`
	Creator    bson.ObjectId `bson:"creator" json:"creator"`
	// 任务属性
	Name            string        `bson:"name" json:"name"`
	Type            QcTaskType    `bson:"type" json:"type"`             // 质检类型 主管质检   质检员质检
	AccountId       bson.ObjectId `bson:"account_id" json:"account_id"` // 质检员id
	AccountName     string        `bson:"-" json:"account_name"`
	CycleStrategy   CycleStrategy `bson:"cycle_strategy" json:"cycle_strategy"` // 重复策略
	CycleDate       DateRange     `bson:"cycle_date" json:"cycle_date"`         // 时间范围
	DialogDate      DialogDate    `bson:"dialog_date" json:"dialog_date"`
	DialogDateRange DateRange     `bson:"dialog_date_range" json:"dialog_date_range"`
	TaskGrade       TaskGrade     `bson:"task_grade" json:"task_grade"`
	// 会话数量设置
	QcWay       QcTaskWay       `bson:"qc_way" json:"qc_way"`             // 质检方式
	TargetNum   int             `bson:"target_num" json:"target_num"`     //目标质检量
	EmployeeIds []bson.ObjectId `bson:"employee_ids" json:"employee_ids"` //客服
	RealNum     int             `bson:"real_num" json:"real_num"`         // 实际任务量
	EachNum     int             `bson:"each_num" json:"each_num"`         //每个客服质检量
	MarkNum     int             `bson:"mark_num" json:"mark_num"`
	// 是否还需要生成记录, PS: 不需要生成记录, 即任务时间范围已过期
	GenerateRecord bool `bson:"generate_record" json:"generate_record"`
}