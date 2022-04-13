-- 1. 获取随机样本
-- 查询指定店铺的买家问题, 并随机抽取一定比例的样本
WITH t1 AS (
    SELECT split_part(snick, ':', 1) AS seller_nick,
        cnick,
        DAY,
        create_time,
        uuid() AS sample_id
    FROM dwd.mini_xdrs_log
    WHERE act = 'recv_msg'
        AND platform = "tb"
        AND DAY >= 20220401
        AND DAY <= 20220401
        AND act not in ('statistics_send_msg', '')
        AND split_part(snick, ':', 1) IN ("cntaobao安久酒类专营店")
),
t2 AS (
    -- 计算每条消息的随机排序id
    SELECT *,
        row_number() OVER (
            ORDER BY sample_id
        ) AS rank_id
    FROM t1
),
t3 AS (
    -- 过滤随机排序序号为指定值的记录
    -- PS: 也可以理解以取余的方式随机抽取一定比例的样本
    SELECT *
    FROM t2
    WHERE rank_id % 1 = 0
),
-- 查询指定店铺的所有消息
x1 AS (
    SELECT split_part(snick, ':', 1) AS seller_nick,
        cnick,
        category,
        act,
        msg,
        remind_answer,
        cast(msg_time AS String) AS msg_time,
        question_b_qid,
        question_b_proba,
        MODE,
        DAY,
        create_time,
        is_robot_answer,
        plat_goods_id,
        current_sale_stage
    FROM dwd.mini_xdrs_log
    WHERE platform = "tb"
        AND DAY >= 20220401
        AND DAY <= 20220401
        AND act not in ('statistics_send_msg', '')
        AND split_part(snick, ':', 1) IN ("cntaobao安久酒类专营店")
),
-- 查询指定店铺的所有消息, flag代表其消息是否是买家咨询问题
x2 AS (
    SELECT x1.*,
        t3.sample_id,
        if(
            x1.create_time = t3.create_time
            AND x1.act = 'recv_msg',
            1,
            0
        ) AS flag
    FROM x1
        RIGHT JOIN [shuffle] t3 ON x1.seller_nick = t3.seller_nick
        AND x1.cnick = t3.cnick
    --! 必定会存在针对买家问题随机样本的笛卡尔积, 即出现数据膨胀, 每个会话的膨胀系数为买家问题数
    --! 右表数据必定会存在重复
)
INSERT overwrite xd_tmp.algorithm_sample_data_all PARTITION (mission_id = '1737cf74480551f1bd93dffac1188ef9')
-- 关联商品信息
SELECT x2.seller_nick,
    x2.cnick,
    x2.category,
    x2.act,
    x2.msg,
    x2.remind_answer,
    x2.msg_time,
    x2.question_b_qid,
    x2.question_b_proba,
    x2.MODE,
    x2.DAY,
    x2.create_time,
    x2.sample_id,
    x2.flag,
    xd_data.question_b.question,
    x2.is_robot_answer,
    x2.plat_goods_id,
    x2.current_sale_stage
FROM x2
    LEFT JOIN [shuffle] xd_data.question_b ON cast(split_part(x2.question_b_qid, '.', 1) AS integer) = cast(
        split_part(xd_data.question_b.qid, '.', 1) AS integer
    );
-- trace:3e841cea11bd704bd3f7909727cb09ba


-- 2. 统计
-- 计算每条消息, 在其对应会话中的序号
WITH t AS (
    SELECT *,
        row_number() over (
            partition by snick,
            cnick
            order by create_time
        ) as time_rank
    FROM xd_tmp.algorithm_sample_data_all
    WHERE mission_id = '1737cf74480551f1bd93dffac1188ef9'
),
t1 AS (
    -- 筛选出买家问题
    SELECT snick,
        cnick,
        sample_id,
        create_time,
        time_rank
    FROM t
    WHERE flag = 1
),
res as (
    -- 使用买家问题随机id的排序序号, 标记每条消息
    -- PS: 如果会话中的一条消息在买家问题的指定上下文范围内, 则存在一个随机id
    SELECT t.*,
        dense_rank() over (
            order by t1.sample_id
        ) as dr
    FROM t1
        left JOIN [SHUFFLE] t on t1.snick = t.snick
        and t1.cnick = t.cnick
        and t.time_rank between t1.time_rank - 0 and t1.time_rank + 0
    --!如果上下文范围设置过大, 则会出现针对买家问题的笛卡尔积, 导致数据指数级膨胀
    --!左表数据必定会重复
)
-- 筛选买家问题随机排序序号在指定数值之前的会话记录
select *
from res
where dr <= 560;
-- trace:0d4a91ce3c3244d0252afa30f8933c24

-- 3. 删除样本数据
alter table xd_tmp.algorithm_sample_data_all drop partition (mission_id='1737cf74480551f1bd93dffac1188ef9');
-- trace:6974e3d73d210767dcfb48dd9068e69f