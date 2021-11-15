1.1服务态度-暗示升级类
1.2服务禁忌-重复回复
1.3未解决问题
1.4未灵活处理
1.5问A答B
1.6未履行承诺
1.7产品知识技能
1.8回复错误
1.9未回复
2.1包装简陋
2.2发残次品
2.3发货慢
2.4漏发货
2.5错发货
2.6发其他客户退货商品
3.1吴江仓
3.2广东仓
3.3京东仓
4.1不派件
4.2丢件
4.3快递员服务态度
4.4网点收费
4.5虚假签收
4.6物流破损
4.7运输时效
4.8未送货上门
4.9物流停滞
4.10派错
5.1苏-中通
5.2粤-中通
5.3苏-顺丰
5.4粤-顺丰
5.5粤-圆通
5.6苏-韵达
5.7苏-EMS
5.8跨越物流
5.9京东物流
6.1乱收费
6.2安装师傅服务态度
6.3未准时上门
6.4无人联系安装
6.5技术问题
6.6约不到时间
7.1京东安装
7.2灯保姆
7.3神工
8.1收到外观瑕疵
8.2余光
8.3漏水
8.4噪音/异响/异味
8.5生锈/掉漆/脏污/材质
8.6耗电量快
8.7照明不亮
8.8串码
8.9做工差
8.10浴霸不取暖
8.11整机不工作
8.12WIFI链接不上
8.13频闪
8.14延迟
8.15底座发烫
9.1光线刺眼
9.2光线亮度不够
9.3价格贵
9.4热度不够
9.5凉霸/风扇灯风力不够
9.6与其它渠道商品不一致
9.7觉得产品设计不合理的
9.8产品不带插头
9.9分包发货机制
9.10保价退差流程
9.11多仓发货机制
9.12用户不理解营销活动
9.13用户不理解页面描述
10.1价格错误/失效
10.2漏铺货
10.3错铺货
10.4页面参数不全
10.5页面错误
10.6超卖
10.7缺货
10.8活动差价
10.9直播返款不及时

    sumIf(tag_cnt,tag_name='1.1服务态度-暗示升级类') AS `1.1服务态度-暗示升级类`,
    sumIf(tag_cnt,tag_name='1.2服务禁忌-重复回复') AS `1.2服务禁忌-重复回复`,
    sumIf(tag_cnt,tag_name='1.3未解决问题') AS `1.3未解决问题`,
    sumIf(tag_cnt,tag_name='1.4未灵活处理') AS `1.4未灵活处理`,
    sumIf(tag_cnt,tag_name='1.5问A答B') AS `1.5问A答B`,
    sumIf(tag_cnt,tag_name='1.6未履行承诺') AS `1.6未履行承诺`,
    sumIf(tag_cnt,tag_name='1.7产品知识技能') AS `1.7产品知识技能`,
    sumIf(tag_cnt,tag_name='1.8回复错误') AS `1.8回复错误`,
    sumIf(tag_cnt,tag_name='1.9未回复') AS `1.9未回复`,
    sumIf(tag_cnt,tag_name='2.1包装简陋') AS `2.1包装简陋`,
    sumIf(tag_cnt,tag_name='2.2发残次品') AS `2.2发残次品`,
    sumIf(tag_cnt,tag_name='2.3发货慢') AS `2.3发货慢`,
    sumIf(tag_cnt,tag_name='2.4漏发货') AS `2.4漏发货`,
    sumIf(tag_cnt,tag_name='2.5错发货') AS `2.5错发货`,
    sumIf(tag_cnt,tag_name='2.6发其他客户退货商品') AS `2.6发其他客户退货商品`,
    sumIf(tag_cnt,tag_name='3.1吴江仓') AS `3.1吴江仓`,
    sumIf(tag_cnt,tag_name='3.2广东仓') AS `3.2广东仓`,
    sumIf(tag_cnt,tag_name='3.3京东仓') AS `3.3京东仓`,
    sumIf(tag_cnt,tag_name='4.1不派件') AS `4.1不派件`,
    sumIf(tag_cnt,tag_name='4.2丢件') AS `4.2丢件`,
    sumIf(tag_cnt,tag_name='4.3快递员服务态度') AS `4.3快递员服务态度`,
    sumIf(tag_cnt,tag_name='4.4网点收费') AS `4.4网点收费`,
    sumIf(tag_cnt,tag_name='4.5虚假签收') AS `4.5虚假签收`,
    sumIf(tag_cnt,tag_name='4.6物流破损') AS `4.6物流破损`,
    sumIf(tag_cnt,tag_name='4.7运输时效') AS `4.7运输时效`,
    sumIf(tag_cnt,tag_name='4.8未送货上门') AS `4.8未送货上门`,
    sumIf(tag_cnt,tag_name='4.9物流停滞') AS `4.9物流停滞`,
    sumIf(tag_cnt,tag_name='4.10派错') AS `4.10派错`,
    sumIf(tag_cnt,tag_name='5.1苏-中通') AS `5.1苏-中通`,
    sumIf(tag_cnt,tag_name='5.2粤-中通') AS `5.2粤-中通`,
    sumIf(tag_cnt,tag_name='5.3苏-顺丰') AS `5.3苏-顺丰`,
    sumIf(tag_cnt,tag_name='5.4粤-顺丰') AS `5.4粤-顺丰`,
    sumIf(tag_cnt,tag_name='5.5粤-圆通') AS `5.5粤-圆通`,
    sumIf(tag_cnt,tag_name='5.6苏-韵达') AS `5.6苏-韵达`,
    sumIf(tag_cnt,tag_name='5.7苏-EMS') AS `5.7苏-EMS`,
    sumIf(tag_cnt,tag_name='5.8跨越物流') AS `5.8跨越物流`,
    sumIf(tag_cnt,tag_name='5.9京东物流') AS `5.9京东物流`,
    sumIf(tag_cnt,tag_name='6.1乱收费') AS `6.1乱收费`,
    sumIf(tag_cnt,tag_name='6.2安装师傅服务态度') AS `6.2安装师傅服务态度`,
    sumIf(tag_cnt,tag_name='6.3未准时上门') AS `6.3未准时上门`,
    sumIf(tag_cnt,tag_name='6.4无人联系安装') AS `6.4无人联系安装`,
    sumIf(tag_cnt,tag_name='6.5技术问题') AS `6.5技术问题`,
    sumIf(tag_cnt,tag_name='6.6约不到时间') AS `6.6约不到时间`,
    sumIf(tag_cnt,tag_name='7.1京东安装') AS `7.1京东安装`,
    sumIf(tag_cnt,tag_name='7.2灯保姆') AS `7.2灯保姆`,
    sumIf(tag_cnt,tag_name='7.3神工') AS `7.3神工`,
    sumIf(tag_cnt,tag_name='8.1收到外观瑕疵') AS `8.1收到外观瑕疵`,
    sumIf(tag_cnt,tag_name='8.2余光') AS `8.2余光`,
    sumIf(tag_cnt,tag_name='8.3漏水') AS `8.3漏水`,
    sumIf(tag_cnt,tag_name='8.4噪音/异响/异味') AS `8.4噪音/异响/异味`,
    sumIf(tag_cnt,tag_name='8.5生锈/掉漆/脏污/材质') AS `8.5生锈/掉漆/脏污/材质`,
    sumIf(tag_cnt,tag_name='8.6耗电量快') AS `8.6耗电量快`,
    sumIf(tag_cnt,tag_name='8.7照明不亮') AS `8.7照明不亮`,
    sumIf(tag_cnt,tag_name='8.8串码') AS `8.8串码`,
    sumIf(tag_cnt,tag_name='8.9做工差') AS `8.9做工差`,
    sumIf(tag_cnt,tag_name='8.10浴霸不取暖') AS `8.10浴霸不取暖`,
    sumIf(tag_cnt,tag_name='8.11整机不工作') AS `8.11整机不工作`,
    sumIf(tag_cnt,tag_name='8.12WIFI链接不上') AS `8.12WIFI链接不上`,
    sumIf(tag_cnt,tag_name='8.13频闪') AS `8.13频闪`,
    sumIf(tag_cnt,tag_name='8.14延迟') AS `8.14延迟`,
    sumIf(tag_cnt,tag_name='8.15底座发烫') AS `8.15底座发烫`,
    sumIf(tag_cnt,tag_name='9.1光线刺眼') AS `9.1光线刺眼`,
    sumIf(tag_cnt,tag_name='9.2光线亮度不够') AS `9.2光线亮度不够`,
    sumIf(tag_cnt,tag_name='9.3价格贵') AS `9.3价格贵`,
    sumIf(tag_cnt,tag_name='9.4热度不够') AS `9.4热度不够`,
    sumIf(tag_cnt,tag_name='9.5凉霸/风扇灯风力不够') AS `9.5凉霸/风扇灯风力不够`,
    sumIf(tag_cnt,tag_name='9.6与其它渠道商品不一致') AS `9.6与其它渠道商品不一致`,
    sumIf(tag_cnt,tag_name='9.7觉得产品设计不合理的') AS `9.7觉得产品设计不合理的`,
    sumIf(tag_cnt,tag_name='9.8产品不带插头') AS `9.8产品不带插头`,
    sumIf(tag_cnt,tag_name='9.9分包发货机制') AS `9.9分包发货机制`,
    sumIf(tag_cnt,tag_name='9.10保价退差流程') AS `9.10保价退差流程`,
    sumIf(tag_cnt,tag_name='9.11多仓发货机制') AS `9.11多仓发货机制`,
    sumIf(tag_cnt,tag_name='9.12用户不理解营销活动') AS `9.12用户不理解营销活动`,
    sumIf(tag_cnt,tag_name='9.13用户不理解页面描述') AS `9.13用户不理解页面描述`,
    sumIf(tag_cnt,tag_name='10.1价格错误/失效') AS `10.1价格错误/失效`,
    sumIf(tag_cnt,tag_name='10.2漏铺货') AS `10.2漏铺货`,
    sumIf(tag_cnt,tag_name='10.3错铺货') AS `10.3错铺货`,
    sumIf(tag_cnt,tag_name='10.4页面参数不全') AS `10.4页面参数不全`,
    sumIf(tag_cnt,tag_name='10.5页面错误') AS `10.5页面错误`,
    sumIf(tag_cnt,tag_name='10.6超卖') AS `10.6超卖`,
    sumIf(tag_cnt,tag_name='10.7缺货') AS `10.7缺货`,
    sumIf(tag_cnt,tag_name='10.8活动差价') AS `10.8活动差价`,
    sumIf(tag_cnt,tag_name='10.9直播返款不及时') AS `10.9直播返款不及时`