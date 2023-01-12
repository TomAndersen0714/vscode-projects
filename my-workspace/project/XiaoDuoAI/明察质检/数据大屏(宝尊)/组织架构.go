type Group struct{
    BaseField                       `bson:",inline"`
    CompanyId   primitive.ObjectID  `json:"company_id", bson:"company_id"`
    CompanyName string              `json:"company_name" bson:"company_name"`
    SubGroups   []*SubGroup         `json:"sub_groups" bson:"sub_groups"`
}

type SubGroup struct{
    Id          primitive.ObjectID  `json:"id", bson:"id"`
    IsShop      bool                `json:"is_shop", bson:"is_shop"`
    Name        string              `json:"name", bson:"name"`
    platform    string              `json:"platform", bson:"platform"`
    SubGroups   []*SubGroup         `json:"sub_groups" bson:"sub_groups"`
}