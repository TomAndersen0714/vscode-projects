package model

import (
	"math"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
	"gitlab.xiaoduoai.com/xiaoduo-qc/pkg/xmongo"

	"gitlab.xiaoduoai.com/xiaoduo-qc/qc-engine/algo_service"
	"gitlab.xiaoduoai.com/xiaoduo-qc/qc-engine/common/enum"
	"gitlab.xiaoduoai.com/xiaoduo-qc/qc-engine/pkg/xcollection"
)

func init() {
	for i := 1; i <= algo_service.MaxEmotion; i++ {
		cDefaultEmotionStats = append(cDefaultEmotionStats, &AlgoEmotionStat{Type: algo_service.AlgoEmotionType(i), Count: 0})
		if i == 8 { //客服骂人
			sDefaultEmotionStats = append(sDefaultEmotionStats, &AlgoEmotionStat{Type: algo_service.AlgoEmotionType(i), Count: 0})
		}
	}

	for i := enum.AbnormalTypeMin; i <= enum.AbnormalTypeMax; i++ {
		defaultAbnormalStats = append(defaultAbnormalStats, &AbnormalStat{Type: enum.AbnormalType(i), Count: 0})
	}

	for i := enum.ExcellentTypeMin; i <= enum.ExcellentTypeMax; i++ {
		defaultExcellentStats = append(defaultExcellentStats, &ExcellentStat{Type: enum.ExcellentType(i), Count: 0})
	}
}

type AlgoEmotionStat struct {
	Type  algo_service.AlgoEmotionType `bson:"type" json:"type"`
	Count int                          `bson:"count" json:"count"`
}

var (
	cDefaultEmotionStats  []*AlgoEmotionStat
	sDefaultEmotionStats  []*AlgoEmotionStat
	defaultAbnormalStats  []*AbnormalStat
	defaultExcellentStats []*ExcellentStat
)

func CDefaultEmotionStats() []*AlgoEmotionStat {
	return cDefaultEmotionStats
}

func SDefaultEmotionStats() []*AlgoEmotionStat {
	return sDefaultEmotionStats
}

func DefaultAbnormalStat() []*AbnormalStat {
	return defaultAbnormalStats
}

func DefaultExcellentStat() []*ExcellentStat {
	return defaultExcellentStats
}

type AbnormalStat struct {
	Type  enum.AbnormalType `bson:"type" json:"type"`
	Count int               `bson:"count" json:"count"`
}

type ExcellentStat struct {
	Type  enum.ExcellentType `bson:"type" json:"type"`
	Count int                `bson:"count" json:"count"`
}

type CustomizeRuleStatus int

const (
	CheckQ   CustomizeRuleStatus = 0
	CheckA   CustomizeRuleStatus = 1
	Finished CustomizeRuleStatus = 2
)

// 关联订单信息
type DialogOrderInfo struct {
	OrderId string `bson:"order_id" json:"order_id,omitempty"`
	// 和订单服务状态一致
	Status  string  `bson:"status" json:"status,omitempty"`
	Payment float64 `bson:"payment" json:"payment,omitempty"` //单位（元）
	// 订单创建
	OrderTime int64 `bson:"order_time" json:"order_time,omitempty"`
}

/*
索引去掉platform,channel字段，目前都已经分平台部署了。目前用于mini和mini-qc环境。
保留的索引：
db.dialog.createIndex({snick:1,cnick:1,date:1},{background:true})
db.dialog.createIndex({update_time:1},{background:true})
db.dialog.createIndex({seller_nick:1,cnick:1,create_time:1,order_info.order_id:1},{background:true})
db.dialog.createIndex({seller_nick:1,begin_time:1,snick:1},{background:true})
db.dialog.createIndex({create_time:1}, {background:true, expireAfterSeconds:2764800})
*/
type Dialog struct {
	CreateTime time.Time `bson:"create_time" json:"create_time,omitempty"`
	UpdateTime time.Time `bson:"update_time" json:"update_time,omitempty"`

	Id bson.ObjectId `bson:"_id,omitempty" json:"id,omitempty"`
	//平台
	Platform string `bson:"platform" json:"platform,omitempty"`
	//渠道
	Channel string `bson:"channel" json:"channel,omitempty"`
	//买家昵称
	CNick string `bson:"cnick" json:"cnick,omitempty"`
	//客服昵称
	SNick string `bson:"snick" json:"snick,omitempty"`
	//卖家昵称
	SellerNick string `bson:"seller_nick" json:"seller_nick,omitempty"`
	// 群 ID
	RoomId string `bson:"room_id,omitempty" json:"room_id"`
	//对话开始时间
	BeginTime time.Time `bson:"begin_time" json:"begin_time,omitempty"`
	//对话结束时间
	EndTime time.Time `bson:"end_time" json:"end_time,omitempty"`
	//是否是售后
	IsAfterSale bool `bson:"is_after_sale" json:"is_after_sale,omitempty"`
	//是否是内部消息
	IsInside bool `bson:"is_inside" json:"is_inside,omitempty"`
	//企业雇员名
	EmployeeName string `bson:"employee_name,omitempty" json:"employee_name,omitempty"`
	//质检数据
	//对话中客服触发过的情绪
	SEmotion []*AlgoEmotionStat `bson:"s_emotion,omitempty" json:"s_emotion,omitempty"`
	//对话中顾客
	CEmotion []*AlgoEmotionStat `bson:"c_emotion,omitempty" json:"e_emotion,omitempty"`

	//对话触发过的情绪 cx 代表顾客触发的情绪 sx 代表的是客服触发的情绪 x是情绪的类型
	Emotions []string `bson:"emotions" json:"emotions,omitempty"`
	//异常质检
	Abnormals []*AbnormalStat `bson:"abnormals" json:"abnormals,omitempty"`
	//加分质检
	Excellents []*ExcellentStat `bson:"excellents" json:"excellents,omitempty"`
	//质检词
	QcWord []*QcWordStat `bson:"qc_word" json:"qc_word,omitempty"`
	// QID 集合
	Qid []int64 `bson:"qid,omitempty" json:"qid,omitempty"`

	Mark         string `bson:"mark" json:"mark,omitempty"`                     // 人工标记 备注
	MarkJudge    int    `bson:"mark_judge" json:"mark_judge,omitempty"`         // tdl 人工标记 类型
	MarkScore    int    `bson:"mark_score" json:"mark_score,omitempty"`         // tdl 人工标记 扣分
	MarkScoreAdd int    `bson:"mark_score_add" json:"mark_score_add,omitempty"` // tb 人工标记 加分
	// tdl/xh-mc人工标记 质检员ID列表，备注，打人工tag,手动保存会话都算质检，
	MarkIds    []bson.ObjectId `bson:"mark_ids" json:"mark_ids,omitempty"`
	HumanCheck bool            `bson:"human_check" json:"human_check,omitempty"` // tdl 人工复查 标记
	//异常分

	Score int `bson:"score" json:"score,omitempty"`
	//加分
	ScoreAdd int `bson:"score_add" json:"score_add,omitempty"`

	ReadMark []bson.ObjectId `bson:"read_mark,omitempty" json:"read_mark,omitempty"`

	//疑似有问题
	SuspectedProblem bool `bson:"suspected_problem" json:"suspected_problem,omitempty"`

	//人工标签减分
	TagScoreStats []*TagScoreStat `bson:"tag_score_stats,omitempty" json:"tag_score_stats,omitempty"`
	//人工标签加分
	TagScoreAddStats []*TagScoreStat `bson:"tag_score_add_stats,omitempty" json:"tag_score_add_stats,omitempty"`

	//自定义质检项统计(减分)
	RuleStats []*CustomizeRuleStat `bson:"rule_stats,omitempty" json:"rule_stats,omitempty"`
	//自定义质检项统计(加分)
	RuleAddStats []*CustomizeRuleStat `bson:"rule_add_stats,omitempty" json:"rule_add_stats,omitempty"`
	//微信自定义质检项x统计(减分)
	WxRuleStats []*WxCustomizeRuleStat `bson:"wx_rule_stats,omitempty" json:"wx_rule_stats,omitempty"`
	//微信自定义质检项统计(加分)
	WxRuleAddStats []*WxCustomizeRuleStat `bson:"wx_rule_add_stats,omitempty" json:"wx_rule_add_stats,omitempty"`
	//当前顾客是否还有之后的会话
	HasAfter bool `bson:"has_after,omitempty" json:"has_after,omitempty"`
	//已完成(无需扣分)未发送扣分项
	NotSendRules NotSendRulesCache `bson:"not_send_rules" json:"not_send_rules"`

	//最后修改会话的质检员
	LastMarkId bson.ObjectId `bson:"last_mark_id,omitempty" json:"last_mark_id,omitempty"`

	// 会话基本信息
	AnswerCount       int64  `bson:"answer_count" json:"answer_count,omitempty"`
	QuestionCount     int64  `bson:"question_count" json:"question_count,omitempty"`
	FirstResponseTime int64  `bson:"first_answer_time" json:"first_answer_time,omitempty"`
	QATimeSum         int    `bson:"qa_time_sum" json:"qa_time_sum,omitempty"`
	QARoundSum        int    `bson:"qa_round_sum" json:"qa_round_sum,omitempty"`
	FocusGoodsId      string `bson:"focus_goods_id" json:"focus_goods_id,omitempty"`

	// 会话所属的分组
	Group string `bson:"group" json:"group,omitempty"`
	//是否有撤回机器人消息
	HasWithdrawRobotMsg bool `bson:"has_withdraw_robot_msg" json:"has_withdraw_robot_msg,omitempty"`
	IsRemind            bool `bson:"is_remind" json:"is_remind,omitempty"`
	// 关联订单信息
	OrderInfo *DialogOrderInfo `bson:"order_info,omitempty" json:"order_info,omitempty"`
	// 是否关联订单
	IsOrderMatched    bool `bson:"is_order_matched" json:"is_order_matched,omitempty"`
	EmotionDetectMode int  `bson:"emotion_detect_mode" json:"emotion_detect_mode,omitempty"` //1精度优先模式 2数量优先模式

	// 任务分配ID
	TaskListId bson.ObjectId `bson:"task_list_id,omitempty"`
	//顾客成单转化 0 未检测 1当日未转化 2 当日转化
	ConsulteTransforV2 int `bson:"consulte_transfor_v2" json:"consulte_transfor_v2,omitempty"`
	// 用于智能排序的得分
	IntelScore int `bson:"intel_score" json:"intel_score"`
	//是否是疑似正面情绪
	SuspectedPositiveEmotion bool `bson:"suspected_positive_emotion" json:"suspected_positive_emotion"`
	//是否补跟进 以最后的为准
	IsFollowUpRemind bool `bson:"is_follow_up_remind" json:"is_follow_up_remind,omitempty"`
	//提醒消息通知类型  1自动 2人工 以最后的为准
	RemindNType int `bson:"remind_ntype" json:"remind_ntype,omitempty"`
	//第一次补跟进时间
	FirstFollowUpTime time.Time `bson:"first_follow_up_time" json:"first_follow_up_time,omitempty"`
	// 会话日期
	Date int `bson:"date"`
	// 最后消息ID(session close时触发更新)
	LastMsgId bson.ObjectId `bson:"last_msg_id,omitempty" json:"last_msg_id,omitempty"`
	//买家内容触发的自定义质检
	CnickCustomizeRule []bson.ObjectId `bson:"cnick_customize_rule" json:"cnick_customize_rule"`

	//销售助手
	ProcedureRules []bson.ObjectId `bson:"procedure_rules,omitempty" json:"procedure_rules,omitempty"`
	UnorderId      bson.ObjectId   `bson:"unorder_id,omitempty"`
	SidCount       map[int64]int   `bson:"sid_count"`
	// 方太项目 qc_task标记
	QcTaskFlag int `bson:"qc_task_flag"`
}

type TagScoreStat struct {
	Id    bson.ObjectId `bson:"id" json:"id"`
	Score int           `bson:"score" json:"score"`
	MD    bool          `bson:"md" json:"md"`       // mark_dialog  是否标记dialog
	MM    bool          `bson:"mm" json:"mm"`       // mark_message 是否标记message
	Count int           `bson:"count" json:"count"` // 仅记录message的标签数量
	Name  string        `bson:"name" json:"name"`
}

type NotSendRulesCache struct {
	// 已完成
	Accomplished map[string]bool `bson:"ac" json:"ac"`
	// 满足买家内容条件
	CheckedBuyer map[string]bool `bson:"cb" json:"cb"`
}

type CustomizeRuleStat struct {
	Id    bson.ObjectId `bson:"id" json:"id"`
	Count int           `bson:"count" json:"count"`
	Score int           `bson:"score" json:"score"`
}

type WxCustomizeRuleStat struct {
	Id    bson.ObjectId `bson:"id" json:"id"`
	Count int           `bson:"count" json:"count"`
	Score int           `bson:"score" json:"score"`
}

type CustomizeRuleStatView struct {
	Id        bson.ObjectId `json:"id"`
	Count     int           `json:"count"`
	Score     int           `json:"score"`
	Name      string        `json:"name"`
	CheckStep CheckStep     `json:"check_step"`
}

type WxCustomizeRuleStatView struct {
	Id    bson.ObjectId `json:"id"`
	Count int           `json:"count"`
	Score int           `json:"score"`
	Name  string        `json:"name"`
	// 0-顾客 1-客服
	SenderType int `json:"sender_type"`
}

type QcWordStat struct {
	//质检词来源 0 顾客 1 客服
	Source int    `bson:"source" json:"source"`
	Word   string `bson:"word" json:"word"`
	Count  int    `bson:"count" json:"count"`
	// 0-客服 1-机器人
	IsRobot int `bson:"is_robot" json:"is_robot"`
}

type DialogColl struct {
	*xmongo.MongoColl
}

func NewDialogColl() *DialogColl {
	coll := xmongo.NewMongoColl("xdqc-offline", "xdqc", "dialog")
	return &DialogColl{coll}
}

func CreateDialog(docs ...*Dialog) error {
	if len(docs) == 0 {
		return errors.New("docs is empty")
	}

	storeDocs := make([]interface{}, 0, len(docs))
	for _, doc := range docs {
		if doc == nil {
			return errors.New("doc is nil")
		}
		if !doc.Id.Valid() {
			doc.Id = bson.NewObjectId()
		}
		if doc.CreateTime.IsZero() {
			doc.CreateTime = time.Now()
		}
		doc.UpdateTime = doc.CreateTime
		storeDocs = append(storeDocs, doc)
	}

	coll := NewDialogColl()
	defer coll.Close()

	err := coll.Insert(storeDocs...)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func FindOneDialog(query interface{}, options ...xmongo.QueryOpt) (*Dialog, error) {
	coll := NewDialogColl()
	defer coll.Close()
	doc := &Dialog{}
	mgoQuery := coll.Find(query)
	mgoQuery = xmongo.ApplyQueryOpts(mgoQuery, options...)
	err := mgoQuery.One(doc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return doc, nil
}

func FindDialogById(id bson.ObjectId, options ...xmongo.QueryOpt) (*Dialog, error) {
	return FindOneDialog(bson.M{"_id": id}, options...)
}

func FindAllDialog(query interface{}, options ...xmongo.QueryOpt) ([]*Dialog, error) {
	coll := NewDialogColl()
	defer coll.Close()

	docs := []*Dialog{}
	mgoQuery := coll.Find(query)
	mgoQuery = xmongo.ApplyQueryOpts(mgoQuery, options...)
	err := mgoQuery.All(&docs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return docs, nil
}

func CountDialog(query interface{}) (int, error) {
	coll := NewDialogColl()
	defer coll.Close()

	n, err := coll.Find(query).Count()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return n, nil
}

func FindAllDialogByPipe(pipe []bson.M) ([]*Dialog, error) {
	coll := NewDialogColl()
	defer coll.Close()

	docs := []*Dialog{}

	err := coll.Pipe(pipe).All(&docs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return docs, nil
}

func DeleteDialog(selector interface{}) error {
	coll := NewDialogColl()
	defer coll.Close()
	err := coll.Remove(selector)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func UpdateDialogById(id bson.ObjectId, update bson.M) error {
	return UpdateDialog(bson.M{"_id": id}, update)
}

// task-record会使用update_time判定抽检时间
func UpdateDialogByIdWithoutUpdateTime(id bson.ObjectId, update bson.M) error {
	selector := bson.M{"_id": id}
	coll := NewDialogColl()
	defer coll.Close()
	err := coll.Update(selector, update)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func UpdateDialog(selector bson.M, update bson.M) error {
	coll := NewDialogColl()
	defer coll.Close()
	err := coll.Update(selector, xmongo.SetUpdateTime(update))
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func UpdateAllDialog(selector bson.M, update bson.M) error {
	coll := NewDialogColl()
	defer coll.Close()
	_, err := coll.UpdateAll(selector, xmongo.SetUpdateTime(update))
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func DialogCount(selector interface{}) (int, error) {
	coll := NewDialogColl()
	defer coll.Close()

	count, err := coll.Find(selector).Count()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return count, nil
}

type DialogCountInfoToEs struct {
	MsgCount          int64   `json:"msg_count"`
	QAScale           float32 `json:"qa_scale"`
	FirstResponseTime int64   `json:"first_answer_time"`
	AvgResponseTime   float32 `json:"avg_response_time"`
}

func GetDialogCountInfoToEs(d *Dialog) *DialogCountInfoToEs {
	di := &DialogCountInfoToEs{
		// 消息数量
		MsgCount: d.AnswerCount + d.QuestionCount,
	}

	// 首次响应时间 = 0 ，说明没有被设置过，也就是只有Q，没有A，响应时间设置为1800s，平均响应时间也为1800s
	if d.FirstResponseTime == 0 {
		di.FirstResponseTime = 1800
		di.AvgResponseTime = 1800
	} else {
		di.FirstResponseTime = d.FirstResponseTime - d.BeginTime.Unix()
	}

	// 答问比
	if d.QuestionCount == 0 {
		di.QAScale = math.MaxFloat32
	} else {
		di.QAScale = float32(d.AnswerCount) / float32(d.QuestionCount)
	}

	if d.QARoundSum != 0 {
		di.AvgResponseTime = float32(d.QATimeSum) / float32(d.QARoundSum)
	}
	return di
}

func (d *Dialog) AddQid(qid int64) (exists bool) {
	exists = xcollection.Int64Include(d.Qid, qid)
	if !exists {
		d.Qid = append(d.Qid, qid)
	}
	return
}
func (d *Dialog) MergeQcWord(QcWordStats []*QcWordStat) {
	type Key struct {
		Source int    `bson:"source" json:"source"`
		Word   string `bson:"word" json:"word"`
	}
	mp := make(map[Key]*QcWordStat)
	for _, v := range d.QcWord {
		k := Key{Source: v.Source, Word: v.Word}
		if ptr, ok := mp[k]; ok {
			ptr.Count += v.Count
		} else {
			mp[k] = v
		}
	}

	for _, v := range QcWordStats {
		k := Key{Source: v.Source, Word: v.Word}
		if ptr, ok := mp[k]; ok {
			ptr.Count += v.Count
		} else {
			mp[k] = v
			d.QcWord = append(d.QcWord, v)
		}
	}
}

// 注意与 RuleAddState 进行区分
func (d *Dialog) MergeRuleState(ruleStats []*RuleScoreStat) {
	mp := make(map[bson.ObjectId]*CustomizeRuleStat)
	for _, r := range d.RuleStats {
		mp[r.Id] = r
	}
	for _, rule := range ruleStats {
		d.Score += rule.Score

		if ptr, ok := mp[rule.Id]; ok {
			ptr.Count++
			ptr.Score += rule.Score
		} else {
			newObj := &CustomizeRuleStat{
				Id:    rule.Id,
				Count: 1,
				Score: rule.Score,
			}
			d.RuleStats = append(d.RuleStats, newObj)
			mp[rule.Id] = newObj
		}
	}
}

// 注意与 RuleState 进行区分
func (d *Dialog) MergeRuleAddState(ruleStats []*RuleScoreStat) {
	mp := make(map[bson.ObjectId]*CustomizeRuleStat)
	for _, r := range d.RuleAddStats {
		mp[r.Id] = r
	}
	for _, rule := range ruleStats {
		d.ScoreAdd += rule.Score

		if ptr, ok := mp[rule.Id]; ok {
			ptr.Count++
			ptr.Score += rule.Score
		} else {
			newObj := &CustomizeRuleStat{
				Id:    rule.Id,
				Count: 1,
				Score: rule.Score,
			}
			d.RuleAddStats = append(d.RuleAddStats, newObj)
			mp[rule.Id] = newObj
		}
	}
}

// 注意与 WxRuleAddState 进行区分
func (d *Dialog) MergeWxRuleState(ruleStats []*WxRuleScoreStat) {
	mp := make(map[bson.ObjectId]*WxCustomizeRuleStat)
	for _, r := range d.WxRuleStats {
		mp[r.Id] = r
	}
	for _, rule := range ruleStats {
		d.Score += rule.Score

		if ptr, ok := mp[rule.Id]; ok {
			ptr.Count++
			ptr.Score += rule.Score
		} else {
			newObj := &WxCustomizeRuleStat{
				Id:    rule.Id,
				Count: 1,
				Score: rule.Score,
			}
			d.WxRuleStats = append(d.WxRuleStats, newObj)
			mp[rule.Id] = newObj
		}
	}
}

// 注意与 WxRuleState 进行区分
func (d *Dialog) MergeWxRuleAddState(ruleStats []*WxRuleScoreStat) {
	mp := make(map[bson.ObjectId]*WxCustomizeRuleStat)
	for _, r := range d.WxRuleAddStats {
		mp[r.Id] = r
	}
	for _, rule := range ruleStats {
		d.Score += rule.Score

		if ptr, ok := mp[rule.Id]; ok {
			ptr.Count++
			ptr.Score += rule.Score
		} else {
			newObj := &WxCustomizeRuleStat{
				Id:    rule.Id,
				Count: 1,
				Score: rule.Score,
			}
			d.WxRuleAddStats = append(d.WxRuleAddStats, newObj)
			mp[rule.Id] = newObj
		}
	}
}
