询单明细需求文档定义:


BUG: 方太数据平台, 店铺维度数据与客服维度汇总数据不一致 https://project.feishu.cn/online_bug/online_bug/detail/5648376
Bad Case: ft_dwd.ask_order_cov_detail_all
Reason: 询单记录关联的订单状态不符合需求文档描述, 即文档中是以付款优先, 但明细数据显示以创建时间优先
case1:
    day=20230318
    shop_id = '5cac112e98ef4100118a9c9f'
    cnick:
        'one_id_2382043026'
        'one_id_289620759'
        'one_id_2451930594'
        'one_id_1944146353'
        'one_id_3307170736'
conclusion:
    1. 询单明细的ETL算法存在逻辑问题, 不符合需求文档, 正常询单下单但没有记录
BUG reason:
    1. 询单明细的ETL算法存在逻辑问题, 不符合需求文档, 下单在会话之前的询单记录在算法中被剔除

Test Case:
    1. day=20230318, cnick='one_id_836119253'
    2. day=20230319, cnick='one_id_3186495212'
    3. day=20230319, cnick='one_id_1700966417'
    4. day=20230319, cnick='one_id_888730110'
    5. day=20230318, cnick='one_id_58700675'

排查看板:
    -- 近7日订单记录
    SELECT *
    FROM ft_dwd.order_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND buyer_nick = '{{cnick}}'
    ORDER BY modified


    -- 近7日询单记录
    SELECT *
    FROM ft_dwd.ask_order_cov_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND buyer_nick = '{{cnick}}'


    -- 近7日会话记录
    SELECT *
    FROM ft_dwd.session_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND cnick = '{{cnick}}'
    ORDER BY session_start_time
    LIMIT 10

    -- 订单记录
    SELECT *
    FROM ft_dwd.order_detail_all
    WHERE day = parseDateTimeBestEffort('{{day}}')
    AND shop_id = '{{shop_id}}'
    AND order_id = '{{order_id}}'


BUG: 【方太数据一期】询单转化统计规则有误, 未按照约定的询单规则计算 https://project.feishu.cn/online_bug/online_bug/detail/5768665
Bad Case: ft_dwd.order_detail_all, ft_dwd.session_detail_all, ft_dwd.ask_order_cov_detail_all
Reason: 询单记录关联的订单状态不符合需求文档描述, 2&7天询单明细表中, 有较多正常询单案例未判定为客服询单。
case1: 
    day=20230308, shop_id = '5cac112e98ef4100118a9c9f', order_id='1840271268793513693'
    Reason:
        20230308下单, 20230309咨询后付款
        20230308咨询, 20230309咨询, 20230311咨询, cnick='one_id_605519336'
        但晓多询单明细未统计
case2(无问题):
    day=20230313, shop_id = '5cac112e98ef4100118a9c9f', order_id='1845575439053523788'
    Reason:
        20230313下单, 20230313付款
        20230312咨询, 20230313咨询, cnick='one_id_59528837'
        晓多询单明细中, 多条询单记录上关联上同一笔订单, !!!是否算作Bad Case待定, 不影响指标则不修改
case3:
    day=20230301, shop_id = '5cac112e98ef4100118a9c9f',order_id='3228657732767925927'
    Reason:
        20230301下单, 付款
        20230308咨询, 20230301咨询多次, cnick='one_id_3090922759'
        下单付款, 付款前有咨询买家, 晓多未判定询单, 更正: 理论上应该归属到20230301, 但晓多归属到了20230228

conclusion: 
    1. CASE1说明, 询单明细的ETL算法可能存在逻辑问题, 不符合需求文档, 存在关联缺失
    2. CASE3说明, 询单明细的ETL算法可能存在逻辑问题, 不符合需求文档, 订单应该关联上最近的会话而非最早

BUG reason:
    1. CASE1询单明细的ETL算法没有逻辑问题, 符合需求文档, 因为此会话为单口相声, 买家未发送消息, 因此被剔除
    2. CASE3询单明细的ETL算法没有逻辑问题, 符合需求文档, 因为这两天中非单口相声的会话只有2通, 而第2通会话(20230301)在下单动作之后, 
        故会关联到第1通会话(即20230228)


排查看板:
    -- 近7日订单记录
    SELECT *
    FROM ft_dwd.order_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND order_id = '{{order_id}}'
    ORDER BY modified

    -- 近7日会话记录
    SELECT *
    FROM ft_dwd.session_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND cnick IN (
        SELECT DISTINCT
            buyer_nick
        FROM ft_dwd.order_detail_all
        WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
        AND shop_id = '{{shop_id}}'
        AND order_id = '{{order_id}}'
    )
    ORDER BY session_start_time

    -- 近7日询单记录
    SELECT *
    FROM ft_dwd.ask_order_cov_detail_all
    WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
    AND shop_id = '{{shop_id}}'
    AND buyer_nick IN (
        SELECT DISTINCT
            buyer_nick
        FROM ft_dwd.order_detail_all
        WHERE day BETWEEN toYYYYMMDD(subtractDays(toDate(parseDateTimeBestEffort('{{day}}')), 7))
        AND toYYYYMMDD(parseDateTimeBestEffort('{{day}}'))
        AND shop_id = '{{shop_id}}'
        AND order_id = '{{order_id}}'
    )