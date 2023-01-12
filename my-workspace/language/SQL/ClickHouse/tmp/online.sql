-- XQC告警数据清洗方案
-- 1. 关闭告警消息消息流,避免洗数据时,新的消息写入失败
zjk-bigdata007: docker stop data_receiver_ch_xqc_ods_alert

-- 2. 创建同字段MergeTree本地表, 将已有告警数据备份到本地
CREATE TABLE tmp.xqc_ods_alert_bak
AS xqc_ods.alert_local
ENGINE = MergeTree()
ORDER BY (level, warning_type, id)
SETTINGS index_granularity = 8192,
storage_policy = 'rr'

INSERT INTO tmp.xqc_ods_alert_bak
SELECT * FROM xqc_ods.alert_all

-- 核对数据量
SELECT COUNT(id), COUNT(DISTINCT id) FROM xqc_ods.alert_all
SELECT COUNT(id), COUNT(DISTINCT id) FROM tmp.xqc_ods_alert_bak

-- 3. 删除原有分布式表, 重建新的分布式表
DROP TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r

CREATE TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
AS xqc_ods.alert_local
ENGINE = Distributed(
    'cluster_3s_2r', 'xqc_ods', 'alert_local', 
    xxHash64(level, warning_type, id)
)

-- 4. 清空旧的告警数据, 并将本地数据写入到分布式表, 即清洗完成
TRUNCATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r

INSERT INTO xqc_ods.alert_all
SELECT * FROM tmp.xqc_ods_alert_bak

-- 5. 查询消息, 检查是否清洗完成
SELECT COUNT(id), COUNT(DISTINCT id) FROM xqc_ods.alert_all
SELECT COUNT(id), COUNT(DISTINCT id) FROM xqc_ods.alert_all FINAL

-- 6. 开启告警消息流, 继续写入消息
zjk-bigdata007: docker start data_receiver_ch_xqc_ods_alert