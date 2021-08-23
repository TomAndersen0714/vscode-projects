type CompanyShop struct {
    //db.company_shop.createIndex({company_id:1, mp_shop_id:1}, {unique:true, background:true})
    //db.company_shop.createIndex({mp_shop_id:1}, {unique:true, background:true})
    Id         bson.ObjectId `bson:"_id"`
    CreateTime time.Time     `bson:"create_time"`
    UpdateTime time.Time     `bson:"update_time"`
    CompanyId  bson.ObjectId `bson:"company_id"`
    MpShopId   bson.ObjectId `bson:"mp_shop_id"`
    Platform   string        `bson:"platform"`
 }