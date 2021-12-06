type Shop struct {
    Id                   bson.ObjectId `bson:"_id" json:"id"`
    CreateTime           time.Time     `bson:"create_time" json:"create_time"`
    UpdateTime           time.Time     `bson:"update_time" json:"update_time"`
    Nick                 string        `bson:"nick" json:"nick"` //店铺主账号昵称
    IsCreateQcEngineShop bool          `bson:"is_create_qc_engine_shop"`
    IsClose              bool          `bson:"is_close" json:"is_close"`
    Groups               Groups        `bson:"groups" json:"groups"`
    //行业类目代码(为空表示从机器人配置中获取)
    NluCode string `bson:"nlu_code" json:"nlu_code"`
    //mp店铺唯一ID
    ShopId string `bson:"shop_id" json:"shop_id"`
    //开始使用时间 即产生第一个会话的时间
    IsStartUse   bool      `bson:"is_start_use" json:"is_start_use"`
    StartUseTime time.Time `bson:"start_use_time" json:"start_use_time"`
    ExpireTime   time.Time `bson:"expire_time" json:"expire_time"`
    // 明察版本 0高级版 1基础版 (后期可扩展版本功能)
    Version int `bson:"version" json:"version"`
    // 白名单功能
    Whitelist       []string `bson:"whitelist" json:"whitelist"`
    IsPhoneVerified bool     `bson:"is_phone_verified" json:"is_phone_verified"` // 是否验证过手机号码
    Phone           string   `bson:"phone" json:"phone"`                         // 手机号码
    Name            string   `bson:"name" json:"name"`                           // 姓名
}