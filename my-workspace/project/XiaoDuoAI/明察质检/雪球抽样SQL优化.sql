-- 1. 获取随机样本
-- 查询指定店铺的所有消息
WITH x1 AS (
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
        current_sale_stage,
        uuid() AS sample_id
    FROM dwd.mini_xdrs_log
    WHERE platform = "tb"
        AND DAY >= 20220401
        AND DAY <= 20220401
        AND act not in ('statistics_send_msg', '')
        AND split_part(snick, ':', 1) IN ("cntaobao安久酒类专营店")
),
x2 AS (
    -- 计算每条消息的随机排序id
    SELECT
        *,
        row_number() OVER (
            PARTITION BY act ORDER BY sample_id
        ) AS rank_id
    FROM x1
)
INSERT overwrite test.algorithm_sample_data_all PARTITION (mission_id = '1737cf74480551f1bd93dffac1188ef8')
-- 关联商品信息, falg代表其消息是否是需要采样的买家问题
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
    if(x2.act = 'recv_msg' AND x2.rank_id % 1 = 0, 1, 0) AS flag,
    xd_data.question_b.question,
    x2.is_robot_answer,
    x2.plat_goods_id,
    x2.current_sale_stage
FROM x2
    LEFT JOIN [shuffle] xd_data.question_b ON cast(split_part(x2.question_b_qid, '.', 1) AS integer) = cast(
        split_part(xd_data.question_b.qid, '.', 1) AS integer
    );


-- 2. 统计
-- 计算每条消息, 在其对应会话中的序号
WITH t AS (
    SELECT *,
        row_number() over (
            partition by snick,
            cnick
            order by create_time
        ) as time_rank
    FROM test.algorithm_sample_data_all
    WHERE mission_id = '1737cf74480551f1bd93dffac1188ef8'
),
t1 AS (
    -- 筛选出买家问题, 并且符合抽样条件的消息记录
    SELECT snick,
        cnick,
        sample_id,
        create_time,
        time_rank
    FROM t
    WHERE flag = 1
),
res as (
    SELECT t.*,
        dense_rank() over (
            order by t1.sample_id
        ) as dr
    FROM t1
        left JOIN [SHUFFLE] t on t1.snick = t.snick
        and t1.cnick = t.cnick
        and t.time_rank between t1.time_rank - 0 and t1.time_rank + 0
)
select *
from res
where dr <= 560;
-- trace:0d4a91ce3c3244d0252afa30f8933c24

-- 3. 删除样本数据
alter table test.algorithm_sample_data_all drop partition (mission_id='1737cf74480551f1bd93dffac1188ef8');
-- trace:6974e3d73d210767dcfb48dd9068e69f