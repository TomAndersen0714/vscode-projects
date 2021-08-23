type TmpXdmpShop struct {
    //db.tmp_xdmp_shop.createIndex({platform:1, plat_user_id:1}, {unique:true, background:true})
    //db.tmp_xdmp_shop.createIndex({plat_shop_name:1, platform:1}, {background:true})
    Id           bson.ObjectId `bson:"_id"`
    Platform     string        `bson:"platform"`
    PlatShopId   string        `bson:"plat_shop_id"`
    PlatShopName string        `bson:"plat_shop_name"`
    PlatUserId   string        `bson:"plat_user_id"`
}