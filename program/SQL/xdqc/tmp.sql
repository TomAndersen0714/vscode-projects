-- 创建本地表
CREATE TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `raw_id` String,
    `dialog_id` String,
    `iscardmsg` String,
    `create_time` String,
    `update_time` String,
    `platform` String,
    `plat_goods_id` String,
    `channel` String,
    `cnick` String,
    `snick` String,
    `seller_nick` String,
    `room_nick` String,
    `source` Int32,
    `content` String,
    `content_type` String,
    `time` DateTime64(3),
    `is_after_sale` String,
    `is_reminder` String,
    `is_inside` String,
    `employee_name` String,
    `intent` Array(Array(Float64)),
    `qid` Int64,
    `answer_explain` String,
    `emotion` Int32,
    `algo_emotion` Int32,
    `emotion_score` Int32,
    `suspected_emotion` String,
    `abnormal_model` Int32,
    `abnormal` Array(Int32),
    `abnormal_scroe.type` Array(Int32),
    `abnormal_scroe.score` Array(Int32),
    `excellent_model` Int32,
    `excellent` Array(Int32),
    `excellent_score.type` Array(Int32),
    `excellent_score.score` Array(Int32),
    `suspected_abnormals` Array(Int32),
    `qc_word_stats.source` Array(Int32),
    `qc_word_stats.word` Array(String),
    `qc_word_stats.count` Array(Int32),
    `auto_send` String,
    `is_transfer` String,
    `ms_msg_time` DateTime64(3),
    `withdraw_ms_time` DateTime64(3),
    `rule_stats.id` Array(String),
    `rule_stats.count` Array(Int32),
    `rule_stats.score` Array(Int32),
    `rule_add_stats.id` Array(String),
    `rule_add_stats.count` Array(Int32),
    `rule_add_stats.score` Array(Int32),
    `wx_rule_stats.id` Array(String),
    `wx_rule_stats.count` Array(Int32),
    `wx_rule_stats.score` Array(Int32),
    `wx_rule_add_stats.id` Array(String),
    `wx_rule_add_stats.count` Array(Int32),
    `wx_rule_add_stats.score` Array(Int32),
    `day` UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_ods/tables/{layer}_{shard}/message_local', '{replica}')
PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- 创建分布式表
CREATE TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
AS xqc_ods.message_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'message_local', rand())

-- 创建Buffer表
CREATE TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)



-- 创建 dwd 表的buffer表, 用于同步数据
CREATE TABLE buffer.dwd_xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS dwd.xdqc_dialog_all
ENGINE = Buffer('dwd', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 16777216, 67108864)


ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
DELETE WHERE toYYYYMMDD(begin_time) BETWEEN 20210627 AND 20210816




-- bigdata006 导出数据到本地 
docker exec -i 9e clickhouse-client --port=19000 --query=\
"SELECT * FROM dwd.xdqc_dialog_all \
WHERE toYYYYMMDD(begin_time) BETWEEN 20210627 AND 20210816 \
AND seller_nick IN (SELECT DISTINCT tenant_label \
FROM xqc_dim.company_tenant) FORMAT Avro" > /tmp/xdqc_dialog_all_20210627_20210816.avro

-- bigdata005 复制 bigdata006 数据到本地OSS盘
scp root@zjk-bigdata006:/tmp/xdqc_dialog_all_20210627_20210816.avro /opt/bigdata/

-- v1mini-bigdata-002 将OSS盘中数据上传到CH集群
docker exec -i 1b clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.dwd_xdqc_dialog_buffer FORMAT Avro" < xdqc_dialog_all_20210627_20210816.avro



-- bigdata006 导出数据到本地
docker exec -i 9e clickhouse-client --port=19000 --query=\
"SELECT * FROM xqc_ods.message_all \
WHERE floor(day/100) BETWEEN 202106 AND 202107 \
AND seller_nick IN (SELECT DISTINCT tenant_label FROM xqc_dim.company_tenant) \
FORMAT Avro" \
> /tmp/xqc_ods_message_all_202106_202107.avro

docker exec -i 9e clickhouse-client --port=19000 --query=\
"SELECT * FROM xqc_ods.message_all \
WHERE floor(day/100) BETWEEN 202108 AND 202109 \
AND seller_nick IN (SELECT DISTINCT tenant_label FROM xqc_dim.company_tenant) \
FORMAT Avro" \
> /tmp/xqc_ods_message_all_202108_202109.avro

-- bigdata005 复制 bigdata006 数据到本地OSS盘
scp root@zjk-bigdata006:/tmp/xqc_ods_message_all_202106_202107.avro /opt/bigdata/
scp root@zjk-bigdata006:/tmp/xqc_ods_message_all_202108_202109.avro /opt/bigdata/

-- v1mini-bigdata-002 将OSS盘中数据上传到CH集群
docker exec -i 1b clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.xqc_message_buffer FORMAT Avro" < xqc_ods_message_all_202106_202107.avro
docker exec -i 1b clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.xqc_message_buffer FORMAT Avro" < xqc_ods_message_all_202108_202109.avro