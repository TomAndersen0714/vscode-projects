-- 创建对应数据库
CREATE DATABASE IF NOT EXISTS xqc_dim ENGINE = Atomic

-- 创建本地表
CREATE TABLE IF NOT EXISTS xqc_dim.ai_check_emotion_item_local ON CLUSTER cluster_3s_2r(
    `type` String,
    `id` String,
    `name` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/xqc_dim/tables/{layer}_{shard}/ai_check_emotion_item_local', '{replica}') 
ORDER BY (`type`, `name`) SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 创建分布式表
CREATE TABLE IF NOT EXISTS xqc_dim.ai_check_emotion_item_all ON CLUSTER cluster_3s_2r
AS xqc_dim.ai_check_emotion_item_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'ai_check_emotion_item_local', rand())

-- 写入映射关系数据(方案待定)