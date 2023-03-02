-- truncate table tmp.session_filter_local on cluster cluster_3s_2r;
--
-- truncate table tmp.ask_order_cov_detail_local on cluster cluster_3s_2r;
--
-- truncate table tmp.persell_ask_order_cov_detail_local on cluster cluster_3s_2r;

-- alter table ft_dwd.ask_order_cov_detail_local on cluster cluster_3s_2r delete where shop_id = '{{shop_id}}'
--   AND `cycle` = {{cycle}}
--   AND `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'));



-- 筛选有效会话, PS: 包含了售前和售后
-- 询单周期内
INSERT INTO tmp.session_filter_all
SELECT `day`,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    snick,
    session_id,
    session_start_time,
    session_end_time,
    is_start_by_cnick,
    is_end_by_cnick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
    {{cycle}} AS `cycle`
FROM (
        SELECT session.*,
            if(
                length(focus_goods_ids) = 1
                AND hasAny(session.focus_goods_ids, goods.goods_arr),
                1,
                0
            ) AS flag
        FROM (
                SELECT `day`,
                    shop_id,
                    session_id,
                    replaceAll(snick, 'cnjd', '') AS snick,
                    replaceAll(cnick, 'cnjd', '') AS buyer_nick,
                    replaceAll(real_buyer_nick, 'cnjd', '') AS real_buyer_nick,
                    [toDateTime64(recv_msg_start_time,6),toDateTime64(recv_msg_end_time,6),toDateTime64(send_msg_start_time,6),toDateTime64(send_msg_end_time,6)] AS time_arr,
                    arrayFilter(
                        x->toYYYYMMDD(x) != toYYYYMMDD(toDateTime64('0000-00-00 00:00:00.000000', 6)),
                        time_arr
                    ) AS new_time_arr,
                    arrayReduce('min', new_time_arr) AS session_start_time,
                    arrayReduce('max', new_time_arr) AS session_end_time,
                    if(
                        toDateTime64(recv_msg_start_time, 6) < toDateTime64(send_msg_start_time, 6),
                        '1',
                        '0'
                    ) AS is_start_by_cnick,
                    if(
                        toDateTime64(recv_msg_end_time, 6) > toDateTime64(send_msg_end_time, 6),
                        '1',
                        '0'
                    ) AS is_end_by_cnick,
                    session_recv_cnt,
                    session_send_cnt,
                    arrayFilter(x->x != '', arrayDistinct(focus_goods_ids)) AS focus_goods_ids,
                    c_active_send_goods_ids,
                    s_active_send_goods_ids
                FROM ft_dwd.session_detail_all
                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    -- 顾客仅有一句话的不算询单（剔除单口相声）
                    AND session_recv_cnt > 1
                    -- 会话中有被转接记录不算询单：转出的不算询单，转入和未转的算一个询单
                    AND has_transfer IN (0, 1)
                    AND concat(shop_id, '-', cnick) NOT IN (
                        -- 剔除广告、小二、内部沟通、测试账号（导入的方式剔除）
                        SELECT concat(shop_id, '-', cnick) 
                        FROM ft_dim.account_filter_all
                    )
            ) AS `session`
            LEFT JOIN (
                SELECT groupArray(goods_id) AS goods_arr,
                    shop_id
                FROM ft_dim.goods_info_all
                WHERE `type` = '4'
                    AND shop_id = '{{shop_id}}'
                GROUP BY shop_id
            ) AS goods USING(shop_id)
        --剔除只咨询配件的
        WHERE flag = 0
    );



-- 订单*状态*产品粒度询单记录-询单转化和未转化记录-非预售订单相关
-- 询单周期内
INSERT INTO tmp.ask_order_cov_detail_all
SELECT `day`,
    shop_id,
    'jd' AS platform,
    buyer_nick,
    -- BUG: session_id 不应该用于排序
    argMax(real_buyer_nick, session_id) AS real_buyer_nick,
    snick,
    session_id,
    argMax(session_start_time, session_id) AS session_start_time,
    argMax(session_end_time, session_id) AS session_end_time,
    argMax(is_start_by_cnick, session_id) AS is_start_by_cnick,
    argMax(is_end_by_cnick, session_id) AS is_end_by_cnick,
    argMax(focus_goods_ids, session_id) AS focus_goods_ids,
    argMax(c_active_send_goods_ids, session_id) AS c_active_send_goods_ids,
    argMax(s_active_send_goods_ids, session_id) AS s_active_send_goods_ids,
    order_id,
    goods_id,
    -- 从订单*产品记录的时间中获取首个已创建状态时间, BUG: 未预先排序, 近乎随机抽取
    if(
        has(groupArray(status), 'created'),
        toString(
            groupArray(modified) [indexOf(groupArray(status),'created')]
        ),
        ''
    ) AS created_time,
    -- 从订单*产品记录的时间中获取首个已付款状态时间, BUG: 未预先排序, 近乎随机抽取
    if(
        has(groupArray(status), 'paid'),
        toString(
            groupArray(modified) [indexOf(groupArray(status),'paid')]
        ),
        ''
    ) AS paid_time,
    -- 从订单*产品记录的时间中获取首个已付款状态订单金额, BUG: 未预先排序, 近乎随机抽取
    if(
        has(groupArray(status), 'paid'),
        groupArray(order_payment) [indexOf(groupArray(status),'paid')],
        if(
            has(groupArray(status), 'created'),
            groupArray(order_payment) [indexOf(groupArray(status),'created')],
            0
        )
    ) AS order_payment,
    -- 从订单*产品记录的时间中获取首个已付款订单的商品金额, BUG: 未预先排序, 近乎随机抽取
    if(
        has(groupArray(status), 'paid'),
        groupArray(goods_payment) [indexOf(groupArray(status),'paid')],
        if(
            has(groupArray(status), 'created'),
            groupArray(goods_payment) [indexOf(groupArray(status),'created')],
            0
        )
    ) AS goods_payment,
    Max(step_trade_status),
    Max(step_paid_fee),
    Max(new_order_type),
    Max(goods_num),
    0 AS is_refund,
    if(paid_time != '', 1, 0) AS is_transf,
    {{cycle}} AS `cycle`
FROM (
        SELECT order_info.*,
            new_modified AS modified,
            -- 计算订单时间与每个会话开始时间的差值, 为正数时, 则表明订单状态生成在会话发生之后
            arrayMap(
                x->toDateTime(modified) - toDateTime(toDateTime64(x, 6)),
                session_start_time_arr
            ) AS time_subtract,
            -- 筛选订单状态-会话时间差大于0的值, 即订单状态记录生成时间, 与其之前发生的会话的时间差
            arrayFilter(x->x >= 0, time_subtract) AS time_subtract_filter,
            -- 在先聊天后下单的数据中, 取下单时间距离会话最近的时间差
            arrayReduce('min', time_subtract_filter) AS time_subtract_min,
            -- 定位下单时, 所处的会话序号
            indexOf(time_subtract, time_subtract_min) AS index_v,
            shop_id,
            buyer_nick,
            snick_arr [index_v] AS snick,
            day_arr [index_v] AS `day`,
            real_buyer_nick_arr [index_v] AS real_buyer_nick,
            session_id_arr [index_v] AS session_id,
            session_start_time_arr [index_v] AS session_start_time,
            session_end_time_arr [index_v] AS session_end_time,
            is_start_by_cnick_arr [index_v] AS is_start_by_cnick,
            is_end_by_cnick_arr [index_v] AS is_end_by_cnick,
            focus_goods_ids_arr [index_v] AS focus_goods_ids,
            s_active_send_goods_ids_arr [index_v] AS s_active_send_goods_ids,
            c_active_send_goods_ids_arr [index_v] AS c_active_send_goods_ids
        FROM (
                -- 聚合询单周期内每个买家的会话记录
                SELECT shop_id,
                    buyer_nick,
                    groupArray(snick) AS snick_arr,
                    groupArray(`day`) AS day_arr,
                    groupArray(real_buyer_nick) AS real_buyer_nick_arr,
                    groupArray(session_id) AS session_id_arr,
                    groupArray(session_start_time) AS session_start_time_arr,
                    groupArray(session_end_time) AS session_end_time_arr,
                    groupArray(is_start_by_cnick) AS is_start_by_cnick_arr,
                    groupArray(is_end_by_cnick) AS is_end_by_cnick_arr,
                    groupArray(focus_goods_ids) AS focus_goods_ids_arr,
                    groupArray(c_active_send_goods_ids) AS c_active_send_goods_ids_arr,
                    groupArray(s_active_send_goods_ids) AS s_active_send_goods_ids_arr
                FROM (
                        -- 询单周期内所有的会话记录
                        SELECT `day`,
                            shop_id,
                            buyer_nick,
                            real_buyer_nick,
                            snick,
                            session_id,
                            session_start_time,
                            session_end_time,
                            is_start_by_cnick,
                            is_end_by_cnick,
                            focus_goods_ids,
                            c_active_send_goods_ids,
                            s_active_send_goods_ids
                        FROM tmp.session_filter_all
                        where `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                            AND shop_id = '{{shop_id}}'
                            AND `cycle` = {{cycle}}
                    )
                GROUP BY shop_id, buyer_nick
            ) AS session
            -- 只看询单周期内有订单记录的数据
            INNER JOIN (
                -- 获取询单周期内所有去重后的订单状态数据, 数据重复时选取最早的订单状态数据
                SELECT shop_id,
                    order_id,
                    toDateTime64(arrayReduce('min', groupArray(modified)), 6) AS new_modified,
                    argMin(buyer_nick, modified) AS buyer_nick,
                    goods_id,
                    argMin(goods_payment, modified) AS goods_payment,
                    argMin(order_payment, modified) AS order_payment,
                    argMin(step_trade_status, modified) AS step_trade_status,
                    argMin(step_paid_fee, modified) AS step_paid_fee,
                    argMin(order_type, modified) AS new_order_type,
                    argMin(goods_num, modified) AS goods_num,
                    status
                FROM ft_dwd.order_detail_all
                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    AND status IN ('created', 'paid')
                    -- 筛选非预售订单
                    AND order_type != 'step'
                group by shop_id,
                    order_id,
                    status,
                    goods_id
            ) AS `order_info`
            -- 每个买家关联其在询单周期内的所有订单状态数据, PS: 数据膨胀系数为买家平均订单数
            USING(shop_id, buyer_nick)
        -- 剔除订单状态先于会话产生的订单状态记录, 筛选订单状态记录产生在会话中, 或者产生在会话后的
        WHERE length(time_subtract_filter) > 0
    )
GROUP BY `day`,
    shop_id,
    session_id,
    buyer_nick,
    order_id,
    goods_id,
    snick;


-- 订单*状态*产品粒度询单记录-询单转化和未转化记录-非预售订单相关
-- 询单周期内
INSERT INTO ft_dwd.ask_order_cov_detail_all
SELECT *,
    0 AS is_deposited
FROM tmp.ask_order_cov_detail_all
WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
    AND shop_id = '{{shop_id}}'
    AND `cycle` = {{cycle}}
    AND order_id IN (
        -- 询单周期内, 创建的订单
        SELECT order_id
        FROM (
                SELECT order_id,
                    arrayFilter(x->x != '', groupArray(created_time)) AS flag
                FROM tmp.ask_order_cov_detail_all
                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    AND `cycle` = {{cycle}}
                GROUP BY order_id
                HAVING length(flag) != 0
            )

        UNION ALL
        -- 询单周期前一天, 创建但未付款的订单
        SELECT order_id
        FROM ft_dwd.ask_order_cov_detail_all
        WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}}))
            AND shop_id = '{{shop_id}}'
            AND created_time != ''
            AND `cycle` = {{cycle}}
            AND paid_time = ''
    );


-- 订单*状态*产品粒度询单记录-询单未转化记录-无任何相关订单
-- 询单周期首日
INSERT INTO ft_dwd.ask_order_cov_detail_all
SELECT `day`,
    shop_id,
    'jd' AS platform,
    buyer_nick,
    real_buyer_nick,
    snick,
    session_id,
    session_start_time,
    session_end_time,
    is_start_by_cnick,
    is_end_by_cnick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
    '' AS order_id,
    '' AS goods_id,
    '' AS created_time,
    '' AS paid_time,
    0 AS order_payment,
    0 AS goods_payment,
    '',
    '',
    '',
    0,
    0 AS is_refund,
    0 AS is_transf,
    {{cycle}} AS `cycle`,
    0 AS is_deposited
FROM tmp.session_filter_all
WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
    AND shop_id = '{{shop_id}}'
    AND `cycle` = {{cycle}}
    -- 剔除已经关联上会话的
    AND session_id NOT IN (
        SELECT DISTINCT session_id
        FROM ft_dwd.ask_order_cov_detail_all
        WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
    )
    -- 剔除180天有过订单记录的
    AND replaceAll(buyer_nick, 'cnjd', '') NOT IN (
        SELECT DISTINCT buyer_nick
        FROM ft_dwd.order_detail_all
        WHERE `day` BETWEEN toYYYYMMDD(
                subtractDays(subtractDays(toDate('{{ds}}'), {{cycle}} - 1), 180)
            ) AND toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
            AND shop_id = '{{shop_id}}'
            AND buyer_nick != ''
    )
    AND buyer_nick != '';


-- 订单*状态*产品粒度询单记录-询单未转化记录-预售订单相关
-- 询单周期内
INSERT INTO tmp.persell_ask_order_cov_detail_all
SELECT `day`,
    shop_id,
    'jd' AS platform,
    buyer_nick,
    argMax(real_buyer_nick, session_id) AS real_buyer_nick,
    snick,
    session_id,
    argMax(session_start_time, session_id) AS session_start_time,
    argMax(session_end_time, session_id) AS session_end_time,
    argMax(is_start_by_cnick, session_id) AS is_start_by_cnick,
    argMax(is_end_by_cnick, session_id) AS is_end_by_cnick,
    argMax(focus_goods_ids, session_id) AS focus_goods_ids,
    argMax(c_active_send_goods_ids, session_id) AS c_active_send_goods_ids,
    argMax(s_active_send_goods_ids, session_id) AS s_active_send_goods_ids,
    order_id,
    goods_id,
    if(
        has(groupArray(status), 'created'),
        toString(
            groupArray(modified) [indexOf(groupArray(status),'created')]
        ),
        ''
    ) AS created_time,
    if(
        has(groupArray(status), 'deposited'),
        toString(
            groupArray(modified) [indexOf(groupArray(status),'deposited')]
        ),
        ''
    ) AS deposited_time,
    if(
        has(groupArray(status), 'deposited'),
        groupArray(order_payment) [indexOf(groupArray(status),'deposited')],
        if(
            has(groupArray(status), 'created'),
            groupArray(order_payment) [indexOf(groupArray(status),'created')],
            0
        )
    ) AS order_payment,
    if(
        has(groupArray(status), 'deposited'),
        groupArray(goods_payment) [indexOf(groupArray(status),'deposited')],
        if(
            has(groupArray(status), 'created'),
            groupArray(goods_payment) [indexOf(groupArray(status),'created')],
            0
        )
    ) AS goods_payment,
    Max(step_trade_status),
    Max(step_paid_fee),
    Max(new_order_type),
    Max(goods_num),
    0 AS is_refund,
    0 AS is_transf,
    {{cycle}} AS `cycle`
FROM (
        SELECT order_info.*,
            new_modified AS modified,
            arrayMap(
                x->toDateTime(modified) - toDateTime(toDateTime64(x, 6)),
                session_start_time_arr
            ) AS time_subtract,
            -- 筛选订单状态-会话时间差大于0的值, 即订单状态记录生成时间, 与其之前发生的会话的时间差
            arrayFilter(x->x >= 0, time_subtract) AS time_subtract_filter,
            -- 在先聊天后下单的数据中, 取下单时间距离会话最近的时间差
            arrayReduce('min', time_subtract_filter) AS time_subtract_min,
            -- 定位下单时, 所处的会话序号
            indexOf(time_subtract, time_subtract_min) AS index_v,
            shop_id,
            buyer_nick,
            snick_arr [index_v] AS snick,
            day_arr [index_v] AS `day`,
            real_buyer_nick_arr [index_v] AS real_buyer_nick,
            session_id_arr [index_v] AS session_id,
            session_start_time_arr [index_v] AS session_start_time,
            session_end_time_arr [index_v] AS session_end_time,
            is_start_by_cnick_arr [index_v] AS is_start_by_cnick,
            is_end_by_cnick_arr [index_v] AS is_end_by_cnick,
            focus_goods_ids_arr [index_v] AS focus_goods_ids,
            c_active_send_goods_ids_arr [index_v] AS c_active_send_goods_ids,
            s_active_send_goods_ids_arr [index_v] AS s_active_send_goods_ids
        FROM (
                SELECT shop_id,
                    buyer_nick,
                    groupArray(snick) AS snick_arr,
                    groupArray(`day`) AS day_arr,
                    groupArray(real_buyer_nick) AS real_buyer_nick_arr,
                    groupArray(session_id) AS session_id_arr,
                    groupArray(session_start_time) AS session_start_time_arr,
                    groupArray(session_end_time) AS session_end_time_arr,
                    groupArray(is_start_by_cnick) AS is_start_by_cnick_arr,
                    groupArray(is_end_by_cnick) AS is_end_by_cnick_arr,
                    groupArray(focus_goods_ids) AS focus_goods_ids_arr,
                    groupArray(c_active_send_goods_ids) AS c_active_send_goods_ids_arr,
                    groupArray(s_active_send_goods_ids) AS s_active_send_goods_ids_arr
                FROM (
                        SELECT `day`,
                            shop_id,
                            buyer_nick,
                            real_buyer_nick,
                            snick,
                            session_id,
                            session_start_time,
                            session_end_time,
                            is_start_by_cnick,
                            is_end_by_cnick,
                            focus_goods_ids,
                            c_active_send_goods_ids,
                            s_active_send_goods_ids
                        FROM tmp.session_filter_all
                        where `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                            AND shop_id = '{{shop_id}}'
                            AND `cycle` = {{cycle}}
                            -- 只看询单周期内下过预售单的买家
                            AND concat(shop_id, '-', buyer_nick) IN
                            (
                                SELECT DISTINCT concat(shop_id, '-', buyer_nick)
                                FROM ft_dwd.order_detail_all
                                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                                    AND shop_id = '{{shop_id}}'
                                    AND status IN (
                                        'created',
                                        'deposited'
                                    )
                                    -- 筛选预售订单
                                    AND order_type = 'step'
                            )
                    )
                GROUP BY shop_id,
                    buyer_nick
            ) AS session
            LEFT JOIN (
                SELECT shop_id,
                    order_id,
                    toDateTime64(arrayReduce('min', groupArray(modified)), 6) AS new_modified,
                    argMin(buyer_nick, modified) AS buyer_nick,
                    goods_id,
                    argMin(goods_payment, modified) AS goods_payment,
                    argMin(order_payment, modified) AS order_payment,
                    argMin(step_trade_status, modified) AS step_trade_status,
                    argMin(step_paid_fee, modified) AS step_paid_fee,
                    argMin(order_type, modified) AS new_order_type,
                    argMin(goods_num, modified) AS goods_num,
                    status
                FROM ft_dwd.order_detail_all
                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    AND status IN ('created', 'deposited')
                    AND order_type = 'step'
                group by shop_id,
                    order_id,
                    status,
                    goods_id
            ) AS `order_info` USING(
                shop_id,
                buyer_nick
            )
        -- 剔除订单状态先于会话产生的订单状态记录, 筛选订单状态记录产生在会话中, 或者产生在会话后的
        WHERE length(time_subtract_filter) > 0
    )
GROUP BY `day`,
    shop_id,
    session_id,
    buyer_nick,
    order_id,
    goods_id,
    snick;



-- 订单*状态*产品粒度询单记录-询单未转化记录-预售订单相关
-- 询单周期内
INSERT INTO ft_dwd.persell_ask_order_cov_detail_all
SELECT *
FROM tmp.persell_ask_order_cov_detail_all
WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
    AND shop_id = '{{shop_id}}'
    AND `cycle` = {{cycle}}
    AND order_id IN (
        -- 询单周期内, 创建过的售前订单
        SELECT order_id
        FROM (
                SELECT order_id,
                    arrayFilter(x->x != '', groupArray(created_time)) AS flag
                FROM tmp.persell_ask_order_cov_detail_all
                WHERE `day` BETWEEN toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1)) AND toYYYYMMDD(toDate('{{ds}}'))
                    AND shop_id = '{{shop_id}}'
                    AND `cycle` = {{cycle}}
                GROUP BY order_id
                HAVING length(flag) != 0
            )
        UNION ALL
        -- 询单周期前一天, 创建但未付定金的订单
        SELECT order_id
        FROM ft_dwd.persell_ask_order_cov_detail_all
        WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}}))
            AND shop_id = '{{shop_id}}'
            AND created_time != ''
            AND `cycle` = {{cycle}}
            AND deposited_time = ''
    );



-- 订单*状态*产品粒度询单记录-询单未转化记录-预售订单相关
-- 询单周期首日
INSERT INTO ft_dwd.ask_order_cov_detail_all
select day,
    shop_id,
    'jd' AS platform,
    buyer_nick,
    real_buyer_nick,
    snick,
    session_id,
    session_start_time,
    session_end_time,
    is_start_by_cnick,
    is_end_by_cnick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
    order_id,
    goods_id,
    created_time,
    deposited_time AS paid_time,
    order_payment,
    goods_payment,
    step_trade_status,
    step_paid_fee,
    order_type,
    goods_num,
    is_refund,
    0 AS is_transf,
    cycle,
    1
FROM ft_dwd.persell_ask_order_cov_detail_all
where `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
    AND shop_id = '{{shop_id}}'
    AND `cycle` = {{cycle}}
    AND order_id != '';



-- 订单*状态*产品粒度询单记录-询单转化记录-预售订单相关
-- 询单周期首日
INSERT INTO ft_dwd.ask_order_cov_detail_all
SELECT order_info.`day`,
    shop_id,
    'jd' AS platform,
    buyer_nick,
    real_buyer_nick,
    if(
        persell_info.snick != '',
        persell_info.snick,
        order_info.snick
    ) AS snick,
    if(
        persell_info.snick != '',
        persell_info.session_id,
        order_info.session_id
    ) AS session_id,
    if(
        persell_info.snick != '',
        persell_info.session_start_time,
        order_info.session_start_time
    ) AS session_start_time,
    if(
        persell_info.snick != '',
        persell_info.session_end_time,
        order_info.session_end_time
    ) AS session_end_time,
    if(
        persell_info.snick != '',
        persell_info.is_start_by_cnick,
        order_info.is_start_by_cnick
    ) AS is_start_by_cnick,
    if(
        persell_info.snick != '',
        persell_info.is_end_by_cnick,
        order_info.is_end_by_cnick
    ) AS is_end_by_cnick,
    if(
        persell_info.snick != '',
        persell_info.focus_goods_ids,
        order_info.focus_goods_ids
    ) AS focus_goods_ids,
    if(
        persell_info.snick != '',
        persell_info.c_active_send_goods_ids,
        order_info.c_active_send_goods_ids
    ) AS c_active_send_goods_ids,
    if(
        persell_info.snick != '',
        persell_info.s_active_send_goods_ids,
        order_info.s_active_send_goods_ids
    ) AS s_active_send_goods_ids,
    order_id,
    goods_id,
    created_time,
    order_info.paid_time,
    if(
        persell_info.snick != '',
        persell_info.order_payment,
        order_info.order_payment
    ) AS order_payment,
    if(
        persell_info.snick != '',
        persell_info.goods_payment,
        order_info.goods_payment
    ) AS goods_payment,
    order_info.step_trade_status AS step_trade_status,
    order_info.step_paid_fee AS step_paid_fee,
    order_info.order_type AS order_type,
    order_info.goods_num AS goods_num,
    if(
        persell_info.snick != '',
        persell_info.is_refund,
        order_info.is_refund
    ) AS is_refund,
    1 AS is_transf,
    `cycle`,
    1
FROM (
        SELECT `day`,
            shop_id,
            buyer_nick,
            argMax(real_buyer_nick, session_id) AS real_buyer_nick,
            snick,
            session_id,
            argMax(session_start_time, session_id) AS session_start_time,
            argMax(session_end_time, session_id) AS session_end_time,
            argMax(is_start_by_cnick, session_id) AS is_start_by_cnick,
            argMax(is_end_by_cnick, session_id) AS is_end_by_cnick,
            argMax(focus_goods_ids, session_id) AS focus_goods_ids,
            argMax(c_active_send_goods_ids, session_id) AS c_active_send_goods_ids,
            argMax(s_active_send_goods_ids, session_id) AS s_active_send_goods_ids,
            order_id,
            goods_id,
            'FRONT_PAID_FINAL_PAID' AS step_trade_status,
            Max(step_paid_fee) AS step_paid_fee,
            Max(new_order_type) AS order_type,
            Max(goods_num) AS goods_num,
            if(
                has(groupArray(status), 'paid'),
                toString(
                    groupArray(modified) [indexOf(groupArray(status),'paid')]
                ),
                ''
            ) AS paid_time,
            groupArray(order_payment) [indexOf(groupArray(status),'paid')] AS order_payment,
            groupArray(goods_payment) [indexOf(groupArray(status),'paid')] AS goods_payment,
            0 AS is_refund,
            if(paid_time != '', 1, 0) AS is_transf,
            {{cycle}} AS `cycle`
        FROM (
                SELECT order_info.*,
                    new_modified AS modified,
                    arrayMap(
                        x->toDateTime(modified) - toDateTime(toDateTime64(x, 6)),
                        session_start_time_arr
                    ) AS time_subtract,
                    -- 筛选订单状态-会话时间差大于0的值, 即订单状态记录生成时间, 与其之前发生的会话的时间差
                    arrayFilter(x->x >= 0, time_subtract) AS time_subtract_filter,
                    -- 在先聊天后下单的数据中, 取下单时间距离会话最近的时间差
                    arrayReduce('min', time_subtract_filter) AS time_subtract_min,
                    -- 定位下单时, 所处的会话序号
                    indexOf(time_subtract, time_subtract_min) AS index_v,
                    shop_id,
                    buyer_nick,
                    snick_arr [index_v] AS snick,
                    day_arr [index_v] AS `day`,
                    real_buyer_nick_arr [index_v] AS real_buyer_nick,
                    session_id_arr [index_v] AS session_id,
                    session_start_time_arr [index_v] AS session_start_time,
                    session_end_time_arr [index_v] AS session_end_time,
                    is_start_by_cnick_arr [index_v] AS is_start_by_cnick,
                    is_end_by_cnick_arr [index_v] AS is_end_by_cnick,
                    focus_goods_ids_arr [index_v] AS focus_goods_ids,
                    c_active_send_goods_ids_arr [index_v] AS c_active_send_goods_ids,
                    s_active_send_goods_ids_arr [index_v] AS s_active_send_goods_ids
                FROM (
                        SELECT `day`,
                            shop_id,
                            order_id,
                            toDateTime(
                                splitByString('.', arrayReduce('min', groupArray(modified))) [1]
                            ) AS new_modified,
                            argMin(buyer_nick, modified) AS buyer_nick,
                            argMin(real_buyer_nick, modified) AS real_buyer_nick,
                            goods_id,
                            argMin(goods_payment, modified) AS goods_payment,
                            argMin(order_payment, modified) AS order_payment,
                            argMin(step_trade_status, modified) AS step_trade_status,
                            argMin(step_paid_fee, modified) AS step_paid_fee,
                            argMin(order_type, modified) AS new_order_type,
                            argMin(goods_num, modified) AS goods_num,
                            status
                        FROM ft_dwd.order_detail_all
                        WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
                            AND shop_id = '{{shop_id}}'
                            AND status = 'paid'
                            AND order_type = 'step'
                        GROUP BY shop_id,
                            order_id,
                            status,
                            goods_id,
                            `day`
                    ) AS order_info
                    LEFT JOIN (
                        SELECT shop_id,
                            buyer_nick,
                            groupArray(snick) AS snick_arr,
                            groupArray(`day`) AS day_arr,
                            groupArray(real_buyer_nick) AS real_buyer_nick_arr,
                            groupArray(session_id) AS session_id_arr,
                            groupArray(session_start_time) AS session_start_time_arr,
                            groupArray(session_end_time) AS session_end_time_arr,
                            groupArray(is_start_by_cnick) AS is_start_by_cnick_arr,
                            groupArray(is_end_by_cnick) AS is_end_by_cnick_arr,
                            groupArray(focus_goods_ids) AS focus_goods_ids_arr,
                            groupArray(c_active_send_goods_ids) AS c_active_send_goods_ids_arr,
                            groupArray(s_active_send_goods_ids) AS s_active_send_goods_ids_arr
                        FROM (
                                SELECT `day`,
                                    shop_id,
                                    buyer_nick,
                                    real_buyer_nick,
                                    snick,
                                    session_id,
                                    session_start_time,
                                    session_end_time,
                                    is_start_by_cnick,
                                    is_end_by_cnick,
                                    focus_goods_ids,
                                    c_active_send_goods_ids,
                                    s_active_send_goods_ids
                                FROM tmp.session_filter_all
                                WHERE `day` BETWEEN toYYYYMMDD(
                                        subtractDays(toDate('{{ds}}'), {{cycle}} - 1 + {{cycle}} - 1)
                                    ) AND toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
                                    AND shop_id = '{{shop_id}}'
                                    AND `cycle` = {{cycle}}
                                    -- 筛选询单周期首日, 付过预售订单尾款的买家
                                    AND concat(shop_id, '-', buyer_nick) IN
                                    (
                                        SELECT DISTINCT concat(shop_id, '-', buyer_nick)
                                        FROM ft_dwd.order_detail_all
                                        WHERE `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
                                            AND shop_id = '{{shop_id}}'
                                            AND status = 'paid'
                                            AND order_type = 'step'
                                    )
                            )
                        GROUP BY shop_id,
                            buyer_nick
                    ) AS session USING(
                        shop_id,
                        buyer_nick
                    )
            )
            -- 剔除订单状态先于会话产生的订单状态记录, 筛选订单状态记录产生在会话中, 或者产生在会话后的
            -- WHERE length(time_subtract_filter) > 0
        GROUP BY `day`,
            shop_id,
            session_id,
            buyer_nick,
            order_id,
            goods_id,
            snick
    ) AS order_info
    LEFT JOIN (
        SELECT *
        FROM ft_dwd.persell_ask_order_cov_detail_all
        WHERE `day` BETWEEN toYYYYMMDD(
                subtractMonths(subtractDays(toDate('{{ds}}'), {{cycle}} - 1), 2)
            ) AND toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
            AND shop_id = '{{shop_id}}'
            AND `cycle` = {{cycle}}
            AND step_trade_status = 'FRONT_PAID_FINAL_NOPAID'
    ) AS persell_info USING(
        shop_id,
        order_id,
        goods_id
    )
having if(
        order_info.session_id = '',
        persell_info.session_id,
        order_info.session_id
    ) != '';
