type Company struct {
    //db.company.createIndex({shot_name:1}, {unique:true, background:true})
    Id         bson.ObjectId `bson:"_id"`
    CreateTime time.Time     `bson:"create_time"`
    UpdateTime time.Time     `bson:"update_time"`
    Name       string        `bson:"name"`
    ShotName   string        `bson:"shot_name"`
    Logo       string        `bson:"logo"`
    Url        string        `bson:"url"`
}