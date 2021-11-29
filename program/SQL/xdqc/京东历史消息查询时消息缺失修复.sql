-- 定位异常数据
SELECT columns('time'),day
FROM xqc_ods.message_all
WHERE day = toYYYYMMDD(toDateTime64('{{date}}',3))
AND time BETWEEN subtractHours(toDateTime64('{{date}}',3), 8) AND addHours(toDateTime64('{{date}}',3), 16)
LIMIT 10

-- 尝试备份已有数据 20210627~20211022, 按天来备份
CREATE TABLE tmp.xqc_ods_message_bak
AS xqc_ods.message_all
ENGINE = MergeTree()
PARTITION BY day
ORDER BY (platform, seller_nick, snick) 
SETTINGS storage_policy = 'rr',
index_granularity = 8192

INSERT INTO TABLE tmp.xqc_ods_message_bak
SELECT *
FROM xqc_ods.message_all
WHERE day = {ds_nodash}

-- PS: 后台执行
nohup python3 clickhouse_daily_executor.py 20210917 20211006 > tmp.log 2>&1 &
nohup python3 clickhouse_daily_executor.py 20210627 20210916 > tmp.log 2>&1 &

-- 对比备份数据和原始数据量
SELECT day, count(1) as cnt FROM tmp.xqc_ods_message_bak GROUP BY day ORDER BY day DESC
SELECT day, count(1) as cnt FROM xqc_ods.message_all GROUP BY day ORDER BY day DESC


-- 优先修复已有数据, 保证用户查询不会遗漏消息
-- PS: 先修复1003的数据(即day=20211002),然后在线上查询,观察是否将对应的BUG修复
20210627~20211021 需要将前一天多拉取的数据挪到后一天中去, 由于1022分区的数据中包含有1023的部分数据, 而
1023包含了1024的数据, 1024的数据在1024的时候会再次拉取, 只需要保证1024执行Task时, Airflow脚本的过滤条件是正确的即
可以保证1024数据的正确性, 而1023只需要将其中的数据删除即可, 而不必将其数据写入到1024中, 1024的数据之后会幂等写入的

INSERT INTO xqc_ods.message_all
SELECT
    `_id`,
    `raw_id`,
    `dialog_id`,
    `iscardmsg`,
    `create_time`,
    `update_time`,
    `platform`,
    `plat_goods_id`,
    `channel`,
    `cnick`,
    `snick`,
    `seller_nick`,
    `room_nick`,
    `source`,
    `content`,
    `content_type`,
    `time`,
    `is_after_sale`,
    `is_reminder`,
    `is_inside`,
    `employee_name`,
    `intent`,
    `qid`,
    `answer_explain`,
    `emotion`,
    `algo_emotion`,
    `emotion_score`,
    `suspected_emotion`,
    `abnormal_model`,
    `abnormal`,
    `abnormal_scroe.type`,
    `abnormal_scroe.score`,
    `excellent_model`,
    `excellent`,
    `excellent_score.type`,
    `excellent_score.score`,
    `suspected_abnormals`,
    `qc_word_stats.source`,
    `qc_word_stats.word`,
    `qc_word_stats.count`,
    `auto_send`,
    `is_transfer`,
    `ms_msg_time`,
    `withdraw_ms_time`,
    `rule_stats.id`,
    `rule_stats.count`,
    `rule_stats.score`,
    `rule_add_stats.id`,
    `rule_add_stats.count`,
    `rule_add_stats.score`,
    `wx_rule_stats.id`,
    `wx_rule_stats.count`,
    `wx_rule_stats.score`,
    `wx_rule_add_stats.id`,
    `wx_rule_add_stats.count`,
    `wx_rule_add_stats.score`,
    toYYYYMMDD(toDate('{ds}')+1)
FROM xqc_ods.message_all
WHERE day = toYYYYMMDD(toDate('{ds}'))
AND time >= addHours(toDateTime64(toString(parseDateTimeBestEffort('{ds}')),3), 16)

-- 修复数据(自测逻辑是否正确)
-- PS: 正确
nohup python3 clickhouse_daily_executor.py 20211002 20211002 > tmp.log 2>&1 &

-- 修复数据 20210627~20211021
nohup python3 clickhouse_daily_executor.py 20210627 20211021 > tmp.log 2>&1 &


-- 删除脏数据(自测逻辑是否正确)
-- PS: 正确
ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
DELETE WHERE day = toYYYYMMDD(toDate('{ds}')) 
AND time >= addHours(toDateTime64(toString(parseDateTimeBestEffort('{ds}')),3), 16)

-- 删除脏数据
nohup python3 clickhouse_daily_executor.py 20210627 20211021 > tmp.log 2>&1 &


-- 修复融合版数据拉取脚本, 依旧使用UTC时间, 但是保证分区日期是本地日期
-- 修改前
execution_datetime = datetime.strptime(day, '%Y%m%d') - timedelta(hours=-8)
-- 修改后
execution_datetime = datetime.strptime(day, '%Y%m%d') - timedelta(hours=8)

