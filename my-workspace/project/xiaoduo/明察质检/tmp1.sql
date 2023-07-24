type Goods struct {
    ID         primitive.ObjectID `bson:"id" json:"id"`
    CreateTime time.Time          `bson:"create_time" json:"create_time"`
    UpdateTime time.Time          `bson:"update_time" json:"update_time"`
    CompanyID  primitive.ObjectID `bson:"company_id" json:"company_id"`
    ShopID     primitive.ObjectID `bson:"shop_id" json:"shop_id"` // 这里用mp的shop_id
    Platform   string             `bson:"platform" json:"platform"`
    Name       string             `bson:"name" json:"name"`
    GoodsID    string             `bson:"goods_id" json:"goods_id"` // 整合所有id
    Price      float64            `bson:"price" json:"price"`
    Status     int                `bson:"status" json:"status"`
    Tags       []string           `bson:"tags" json:"tags"`
    AddedTime  time.Time          `bson:"added_time" json:"added_time"`
}