replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    act AS `消息收发`,
    msg AS `消息内容`,
    create_time AS `时间`,
    question_b_proba AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`,
    reason_zh AS `未回复原因`,
    sub_reason_zh AS `未回复子原因`DROP TABLE IF EXISTS ods.xdrs_no_reply_reason_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.xdrs_no_reply_reason_local ON CLUSTER cluster_3s_2r (
    `reason` INT,
    `sub_reason` INT,
    `reason_zh` String,
    `sub_reason_zh` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/xdrs_no_reply_reason_local',
    '{replica}'
)
ORDER BY (
    `reason`,
    `sub_reason`
) 
SETTINGS index_granularity = 8192
-- 
DROP TABLE IF EXISTS ods.xdrs_no_reply_reason_all ON CLUSTER cluster_3s_2r
CREATE TABLE ods.xdrs_no_reply_reason_all ON CLUSTER cluster_3s_2r AS ods.xdrs_no_reply_reason_local
ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'xdrs_no_reply_reason_local',
    rand()
)


-- 插入表数据
INSERT INTO ods.xdrs_no_reply_reason_all
VALUES 
(1,1,'通用回复未回复','辅助模式自动发送关闭'),
(1,2,'通用回复未回复','无人值守自动发送关闭'),
(1,3,'通用回复未回复','答案缺失（时效）'),
(1,4,'通用回复未回复','答案缺失'),
(2,1,'关联商品未回复','辅助模式自动发送关闭'),
(2,2,'关联商品未回复','无人值守自动发送关闭'),
(2,3,'关联商品未回复','答案缺失（时效）'),
(2,4,'关联商品未回复','答案缺失'),
(3,1,'关联商品类型未回复','辅助模式自动发送关闭'),
(3,2,'关联商品类型未回复','无人值守自动发送关闭'),
(3,3,'关联商品类型未回复','答案缺失（时效）'),
(3,4,'关联商品类型未回复','答案缺失'),
(5,10,'因接待设置未回复','机器人前置接待（客户端）'),
(5,11,'因接待设置未回复','针对特定买家不自动回复（客户端）'),
(5,12,'因接待设置未回复','人工抢答未回复（客户端）'),
(5,13,'因接待设置未回复','相同问题不重复回复'),
(5,14,'因接待设置未回复','仅提示模式未自动回复'),
(5,15,'因接待设置未回复','售后问题不自动回复'),
(5,16,'因接待设置未回复','物流答案耗尽'),
(5,17,'因接待设置未回复','账号一样不回复'),
(5,18,'因接待设置未回复','等待焦点商品链接未回复（客户端）'),
(5,19,'因接待设置未回复','转接后转接前问题不回复（客户端）'),
(5,30,'因接待设置未回复','未开启智能辅助开关（小程序）'),
(6,100,'小程序与客户端配合未回复','客户端清空答案'),
(6,101,'小程序与客户端配合未回复','小程序清空图片答案'),
(6,102,'小程序与客户端配合未回复','小程序清空行业场景亮黄答案');

INSERT INTO ods.xdrs_no_reply_reason_all (`reason`,`reason_zh`)
VALUES (4,'未达到发送阈值未回复');

-- 原始SQL
SELECT
    replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    '' AS `消息收发`,
    '' AS `消息内容`,
    create_time AS `时间`,
    '' AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`
FROM
    ods.no_reply_logs_all
WHERE `day` = CAST(20200819 AS Int32)
    AND replaceAll(snick,'cntaobao','') = '欧伊俪旗舰店:服务助手'
    AND replaceAll(cnick,'cntaobao','') = 'tb773628010'
    AND act in ('send_msg','recv_msg')
ORDER BY create_time

-- 修改后SQL
SELECT
    replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    act AS `消息收发`,
    msg AS `消息内容`,
    create_time AS `时间`,
    question_b_proba AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`,
    reason_zh AS `未回复原因`,
    sub_reason_zh AS `未回复子原因`
FROM (
    SELECT
        snick,
        cnick,
        act,
        msg,
        create_time,
        question_b_proba,
        plat_goods_id,
        no_reply_reason AS `reason`,
        no_reply_sub_reason  AS `sub_reason`
    FROM ods.xdrs_logs_all
    WHERE `day` = CAST(20200819 AS Int32)
    AND replaceAll(snick,'cntaobao','') = '欧伊俪旗舰店:服务助手'
    AND replaceAll(cnick,'cntaobao','') = 'tb773628010'
    AND act in ('send_msg','recv_msg')
)AS t1
GLOBAL LEFT JOIN ods.xdrs_no_reply_reason_all AS t2
USING reason,sub_reason
ORDER BY create_time
UNION ALL
SELECT
    replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    '' AS `消息收发`,
    '' AS `消息内容`,
    create_time AS `时间`,
    '' AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`,
    reason_zh AS `未回复原因`,
    sub_reason_zh AS `未回复子原因`
FROM (
    SELECT
        snick,
        cnick,
        create_time,
        plat_goods_id,
        reason,
        sub_reason
    FROM
        ods.no_reply_logs_all
    WHERE `day` = CAST(20200819 AS Int32)
        AND replaceAll(snick,'cntaobao','') = '钟铃0812:恭喜发财大吉大利'
        AND replaceAll(cnick,'cntaobao','') = '安7汐'
) AS t1
GLOBAL LEFT JOIN ods.xdrs_no_reply_reason_all AS t2
USING reason,sub_reason
ORDER BY create_time
-- 最终版
SELECT
    replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    act AS `消息收发`,
    msg AS `消息内容`,
    create_time AS `时间`,
    question_b_proba AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`,
    reason_zh AS `未回复原因`,
    sub_reason_zh AS `未回复子原因`
FROM (
    SELECT
        snick,
        cnick,
        act,
        msg,
        create_time,
        question_b_proba,
        plat_goods_id,
        no_reply_reason AS `reason`,
        no_reply_sub_reason  AS `sub_reason`
    FROM ods.xdrs_logs_all
    WHERE `day` = CAST(replaceAll('{{ ds }}','-','') AS Int32)
    AND replaceAll(snick,'cntaobao','') = '{{ s }}'
    AND replaceAll(cnick,'cntaobao','') = '{{ b }}'
    AND act in ('send_msg','recv_msg')
)AS t1
GLOBAL LEFT JOIN ods.xdrs_no_reply_reason_all AS t2
USING reason,sub_reason
ORDER BY create_time
UNION ALL
SELECT
    replaceAll(snick,'cntaobao','') AS `子账号`,
    replaceAll(cnick,'cntaobao','') AS `买家昵称`,
    '' AS `消息收发`,
    '' AS `消息内容`,
    create_time AS `时间`,
    '' AS `阈值`,
    plat_goods_id AS `焦点商品`,
    reason AS `未回复原因ID`,
    sub_reason AS `未回复子原因ID`,
    reason_zh AS `未回复原因`,
    sub_reason_zh AS `未回复子原因`
FROM (
    SELECT
        snick,
        cnick,
        create_time,
        plat_goods_id,
        reason,
        sub_reason
    FROM
        ods.no_reply_logs_all
    WHERE `day` = CAST(replaceAll('{{ ds }}','-','') AS Int32)
        AND replaceAll(snick,'cntaobao','') = '{{ s }}'
        AND replaceAll(cnick,'cntaobao','') = '{{ b }}'
) AS t1
GLOBAL LEFT JOIN ods.xdrs_no_reply_reason_all AS t2
USING reason,sub_reason
ORDER BY create_time