type Dialog struct {
    CreateTime time.Time `bson:"create_time" json:"create_time,omitempty"`
    UpdateTime time.Time `bson:"update_time" json:"update_time,omitempty"`
    Id bson.ObjectId `bson:"_id,omitempty" json:"id,omitempty"`
    //平台
    Platform string `bson:"platform" json:"platform,omitempty"`
    //渠道
    Channel string `bson:"channel" json:"channel,omitempty"`
    // 买家昵称 tb:OneID
    CNick string `bson:"cnick" json:"cnick,omitempty"`
    // 应用+店铺级别唯一
    OpenUID string `bson:"open_uid" json:"open_uid,omitempty"`
    // 脱敏后的买家昵称，无实际意义用作报表 页面展示
    RealBuyerNick string `bson:"real_buyer_nick" json:"real_buyer_nick,omitempty"`
    //客服昵称
    SNick string `bson:"snick" json:"snick,omitempty"`
    //卖家昵称
    SellerNick string `bson:"seller_nick" json:"seller_nick,omitempty"`
    // 群 ID （微信使用）
    RoomId string `bson:"room_id,omitempty" json:"room_id"`
    //对话开始时间
    BeginTime time.Time `bson:"begin_time" json:"begin_time,omitempty"`
    //对话结束时间
    EndTime time.Time `bson:"end_time" json:"end_time,omitempty"`
    //是否是售后
    IsAfterSale bool `bson:"is_after_sale" json:"is_after_sale,omitempty"`
    //是否是售后--重放
    OriginIsAfterSale bool `bson:"origin_is_after_sale" json:"origin_is_after_sale,omitempty"`
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
    QcWord []*QcWordStat `bson:"qc_word,omitempty" json:"qc_word,omitempty"`
    // QID 集合
    Qid []int64 `bson:"qid,omitempty" json:"qid,omitempty"`
    //异常分，包括所有的扣分（自定义扣分，人工标签扣分，AI扣分，情绪，微信）
    Score int `bson:"score" json:"score,omitempty"`
    //加分
    ScoreAdd int `bson:"score_add" json:"score_add,omitempty"`
    // MarkJudge deprecated 会话案例标记
    MarkJudge int `bson:"mark_judge" json:"mark_judge,omitempty"`
    //人工标记 扣分
    MarkScore int `bson:"mark_score" json:"mark_score,omitempty"`
    //人工标记 加分
    MarkScoreAdd int `bson:"mark_score_add" json:"mark_score_add,omitempty"`
    //xh-mc质检员ID列表，备注，打人工tag,点击保存按钮（无修改也算），单店版点击会话详情
    MarkIds []bson.ObjectId `bson:"mark_ids,omitempty" json:"mark_ids,omitempty"`
    // 人工标记 备注内容
    Mark string `bson:"mark" json:"mark,omitempty"`
    // 最后修改会话的质检员（只读也算）,注意修改last_mark_id的地方必然修改mark_ids
    LastMarkId bson.ObjectId `bson:"last_mark_id,omitempty" json:"last_mark_id,omitempty"`
    // 疑似有问题
    SuspectedProblem bool `bson:"suspected_problem" json:"suspected_problem,omitempty"`
    // 人工标签减分
    TagScoreStats []*TagScoreStat `bson:"tag_score_stats,omitempty" json:"tag_score_stats,omitempty"`
    // 人工标签加分
    TagScoreAddStats []*TagScoreStat `bson:"tag_score_add_stats,omitempty" json:"tag_score_add_stats,omitempty"`
    // 自定义质检项统计(减分)
    RuleStats []*CustomizeRuleStat `bson:"rule_stats,omitempty" json:"rule_stats,omitempty"`
    // 自定义质检项统计(加分)
    RuleAddStats []*CustomizeRuleStat `bson:"rule_add_stats,omitempty" json:"rule_add_stats,omitempty"`
    // 自定义规则-2022版打标(消息)
    XRuleStats []*XRuleStat `bson:"xrule_stats,omitempty" json:"xrule_stats"`
    // 自定义规则-2022版打标(顶部)
    TopXRules []*XRuleStat `bson:"top_xrules,omitempty" json:"top_xrules"`
    // 微信自定义质检项x统计(减分)
    WxRuleStats []*WxCustomizeRuleStat `bson:"wx_rule_stats,omitempty" json:"wx_rule_stats,omitempty"`
    // 微信自定义质检项统计(加分)
    WxRuleAddStats []*WxCustomizeRuleStat `bson:"wx_rule_add_stats,omitempty" json:"wx_rule_add_stats,omitempty"`
    // 客服评价
    ServiceEvaluations []*ServiceEvaluation `bson:"service_evaluations,omitempty" json:"service_evaluations,omitempty"`
    // 当前顾客是否还有之后的会话
    HasAfter bool `bson:"has_after,omitempty" json:"has_after,omitempty"`
    // 已完成(无需扣分)未发送扣分项
    NotSendRules NotSendRulesCache `bson:"not_send_rules,omitempty" json:"not_send_rules"`
    // 买家内容触发的自定义质检
    CnickCustomizeRule []bson.ObjectId `bson:"cnick_customize_rule,omitempty" json:"cnick_customize_rule"`
    // 会话基本信息
    AnswerCount int64 `bson:"answer_count" json:"answer_count,omitempty"`
    QuestionCount int64 `bson:"question_count" json:"question_count,omitempty"`
    // 首响
    FirstResponseTime int64 `bson:"first_answer_time" json:"first_answer_time,omitempty"`
    // 响应时间
    QATimeSum int `bson:"qa_time_sum" json:"qa_time_sum,omitempty"`
    // 轮次
    QARoundSum int `bson:"qa_round_sum" json:"qa_round_sum"`
    // 焦点商品ID
    FocusGoodsId string `bson:"focus_goods_id" json:"focus_goods_id,omitempty"`
    // 会话所属的分组
    Group string `bson:"group,omitempty" json:"group,omitempty"`
    // 是否有撤回机器人消息
    HasWithdrawRobotMsg bool `bson:"has_withdraw_robot_msg,omitempty" json:"has_withdraw_robot_msg,omitempty"`
    IsRemind bool `bson:"is_remind,omitempty" json:"is_remind,omitempty"`
    // 关联订单信息
    OrderInfo *DialogOrderInfo `bson:"order_info,omitempty" json:"order_info,omitempty"`
    // 原始关联订单信息--重放
    OriginOrderInfo *DialogOrderInfo `bson:"origin_order_info,omitempty" json:"origin_order_info,omitempty"`
    // 是否关联订单
    IsOrderMatched bool `bson:"is_order_matched" json:"is_order_matched,omitempty"`
    EmotionDetectMode int `bson:"emotion_detect_mode" json:"emotion_detect_mode,omitempty"` //1精度优先模式 2数量优先模式
    // 任务分配ID,单店版人工质检任务在用
    TaskListId bson.ObjectId `bson:"task_list_id,omitempty"`
    //顾客成单转化 0 未检测 1当日未转化 2 当日转化
    ConsulteTransforV2 int `bson:"consulte_transfor_v2" json:"consulte_transfor_v2,omitempty"`
    // 用于智能排序的得分
    IntelScore int `bson:"intel_score,omitempty" json:"intel_score"`
    //是否是疑似正面情绪
    SuspectedPositiveEmotion bool `bson:"suspected_positive_emotion,omitempty" json:"suspected_positive_emotion"`
    //是否补跟进 以最后的为准
    IsFollowUpRemind bool `bson:"is_follow_up_remind,omitempty" json:"is_follow_up_remind,omitempty"`
    //提醒消息通知类型 1自动 2人工 以最后的为准
    RemindNType int `bson:"remind_ntype,omitempty" json:"remind_ntype,omitempty"`
    //第一次补跟进时间
    FirstFollowUpTime time.Time `bson:"first_follow_up_time,omitempty" json:"first_follow_up_time,omitempty"`
    // 会话日期
    Date int `bson:"date" json:"date"`
    // 最后消息ID(session close时触发更新)
    LastMsgId bson.ObjectId `bson:"last_msg_id,omitempty" json:"last_msg_id,omitempty"`
    // 方太项目 qc_task标记 0 未分配 1 已分配
    QcTaskFlag int `bson:"qc_task_flag" json:"qc_task_flag"`
    // 质检时间
    MarkTime time.Time `bson:"mark_time,omitempty" json:"mark_time,omitempty"`
    // 自定义质检，向后x秒播放缓存
    SecAfterCache core.SecAfterCache `bson:"sec_after_cache,omitempty" json:"sec_after_cache"`
    // 售后周期
    AfterSaleDays int64 `json:"after_sale_days" bson:"after_sale_days"`
    // 商品id plat_goods_ids
    PlatGoodsIDs []string `json:"plat_goods_ids,omitempty" bson:"plat_goods_ids,omitempty"`
    //MessageMarks 单句备注
    MessageMarks []MessageMark `json:"message_marks,omitempty" bson:"message_marks,omitempty"`
    //企微备注 描述
    Remark string `json:"remark" bson:"remark"`
    Desc string `json:"desc" bson:"desc"`
}