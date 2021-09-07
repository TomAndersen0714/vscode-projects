type Message struct {
    //下面的字段不落库
    IsCardMsg bool
    
    //下面的字段需要落库
    CreateTime time.Time `bson:"create_time" json:"create_time,omitempty"`
    UpdateTime time.Time `bson:"update_time" json:"update_time,omitempty"`
    Id    bson.ObjectId `bson:"_id" json:"id,omitempty"`
    RawId string        `bson:"raw_id,omitempty" json:"raw_id,omitempty"` // 全平台 unique（目前仅 wx 使用）
    //对话id
    DialogId bson.ObjectId `bson:"dialog_id" json:"dialog_id,omitempty"`
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
    //群聊，成员昵称
    RoomNick string `bson:"room_nick,omitempty" json:"room_nick,omitempty"`
    //消息源
    Source int `bson:"source" json:"source,omitempty"`
    //消息内容
    Content string `bson:"content" json:"content,omitempty"`
    //消息格式( 默认为空，就是文本，其他的待处理) tb jd 未使用该字段
    ContentType string `bson:"content_type,omitempty" json:"content_type,omitempty"`
    //消息时间
    Time time.Time `bson:"time" json:"time,omitempty"`
    //是否是售后
    IsAfterSale bool `bson:"is_after_sale,omitempty" json:"is_after_sale,omitempty"`
    //是否是催单消息
    IsReminder bool `bson:"is_reminder,omitempty" json:"is_reminder,omitempty"`
    //是否是内部消息
    IsInside bool `bson:"is_inside,omitempty" json:"is_inside,omitempty"`
    //企业雇员名
    EmployeeName string `bson:"employee_name,omitempty" json:"employee_name,omitempty"`
    //算法监测数据
    //表示NLU分类结果，概率最高的几个分类比如 [ [ 0, 0.6919333944153006], [ 234006, 0.1584222382535923 ], [ 232013, 0.04302888025684726 ] ]
    Intent [][]float64 `bson:"intent,omitempty" json:"intent,omitempty"`
    //意图id
    QId int64 `bson:"qid,omitempty" json:"qid,omitempty"`
    // QID 对应语义
    AnswerExplain string `bson:"answer_explain,omitempty" json:"answer_explain,omitempty"`
    //逻辑筛选后的情绪结果
    Emotion algo_service.AlgoEmotionType `bson:"emotion,omitempty" json:"emotion,omitempty"`
    //情绪模型质检结果
    AlgoEmotion algo_service.AlgoEmotionType `bson:"algo_emotion" json:"algo_emotion,omitempty"`
    //情绪分
    EmotionScore int `bson:"emotion_score,omitempty" json:"emotion_score,omitempty"`
    //是否是疑似情绪
    SuspectedEmotion bool `bson:"suspected_emotion,omitempty" json:"suspected_emotion,omitempty"`
    //算法模型异常质检结果
    AlgoAbnormal algo_service.AlgoAbnormalType `bson:"abnormal_model" json:"abnormal_model,omitempty"`
    //算法模型加分项结果
    AlgoExcellent algo_service.AlgoExcellentType `bson:"excellent_model" json:"excellent_model,omitempty"`
    //异常质检结果
    Abnormals []enum.AbnormalType `bson:"abnormal,omitempty" json:"abnormal,omitempty"`
    //异常分
    AbnormalScore []*AbnormalScoreStat `bson:"abnormal_scroe" json:"abnormal_score,omitempty"`
    //加分质检结果
    Excellents []enum.ExcellentType `bson:"excellent,omitempty" json:"excellent,omitempty"`
    //加分
    ExcellentScore []*ExcellentScoreStat `bson:"excellent_score" json:"excellent_score,omitempty"`
    //有嫌疑的异常质检结果
    SuspectedAbnormals []enum.AbnormalType `bson:"suspected_abnormals" json:"suspected_abnormals,omitempty"`
    QcWordStats        []*QcWordStat       `bson:"qc_word_stats,omitempty" json:"qc_word_stats,omitempty"`
    AutoSend           bool                `bson:"auto_send" json:"auto_send,omitempty"`
    //是否是转接消息
    IsTransfer bool `bson:"is_transfer" json:"is_transfer,omitempty"`
    // 买家之声，需要用到，其余没有用
    PlatGoodsID string `bson:"plat_goods_id,omitempty" json:"plat_goods_id"`
    // 毫秒时间戳
    MsMsgTime      int64 `bson:"ms_msg_time" json:"ms_msg_time"`
    WithdrawMsTime int64 `bson:"withdraw_ms_time,omitempty" json:"withdraw_ms_time"`
    //自定义质检项统计(减分)
    RuleStats []*RuleScoreStat `bson:"rule_stats,omitempty" json:"rule_stats,omitempty"`
    //自定义质检项统计(加分)
    RuleAddStats []*RuleScoreStat `bson:"rule_add_stats,omitempty" json:"rule_add_stats,omitempty"`
    //微信自定义质检项统计(减分)
    WxRuleStats []*WxRuleScoreStat `bson:"wx_rule_stats,omitempty" json:"wx_rule_stats,omitempty"`
    //微信自定义质检项统计(加分)
    WxRuleAddStats []*WxRuleScoreStat `bson:"wx_rule_add_stats,omitempty" json:"wx_rule_add_stats,omitempty"`
}