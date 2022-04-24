-- xqc_message_transfer_mini.py 脚本代码段替换

-- 增加新版本自定义质检项字段
    "withdraw_ms_time": "utc_datetime",
    "rule_stats": "rule_score_stat",

    "withdraw_ms_time": "utc_datetime",
    "xrule_stats": "rule_score_stat",
    "rule_stats": "rule_score_stat",

-- 增加人工质检标签字段
    "wx_rule_add_stats": "rule_score_stat"

    "wx_rule_add_stats": "rule_score_stat",
    "tags": "msg_tag"


def msg_tag_mapper(k, v):
    tag_ids, cal_ops, scores, names = [], [], [], []
    if isinstance(v, list):
        for e in v:
            tag_ids.append(str(e.get('tag_id', '')))
            cal_ops.append(int(e.get('cal_op', 0)))
            scores.append(int(e.get('score', 0)))
            names.append(str(e.get('name', '')))

    return [(f"{k}.tag_id", tag_ids), (f"{k}.cal_op", cal_ops),
            (f"{k}.score", scores), (f"{k}.name", names)]

-- 添加新版自定义质检标签字段 xrule_stats
-- 添加人工质检标签字段 tags
-- mini/tb/dy/pdd
-- xqc_ods.message_local
ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS xrule_stats.id Array(String) AFTER `rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS xrule_stats.count Array(UInt32) AFTER `xrule_stats.id`,
ADD COLUMN IF NOT EXISTS xrule_stats.score Array(Int32) AFTER `xrule_stats.count`,
ADD COLUMN IF NOT EXISTS tags.tag_id Array(String) AFTER `wx_rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS tags.cal_op Array(Int32) AFTER `tags.tag_id`,
ADD COLUMN IF NOT EXISTS tags.score Array(Int32) AFTER `tags.cal_op`,
ADD COLUMN IF NOT EXISTS tags.name Array(String) AFTER `tags.score`
-- xqc_ods.message_all
ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS xrule_stats.id Array(String) AFTER `rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS xrule_stats.count Array(UInt32) AFTER `xrule_stats.id`,
ADD COLUMN IF NOT EXISTS xrule_stats.score Array(Int32) AFTER `xrule_stats.count`,
ADD COLUMN IF NOT EXISTS tags.tag_id Array(String) AFTER `wx_rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS tags.cal_op Array(Int32) AFTER `tags.tag_id`,
ADD COLUMN IF NOT EXISTS tags.score Array(Int32) AFTER `tags.cal_op`,
ADD COLUMN IF NOT EXISTS tags.name Array(String) AFTER `tags.score`
-- buffer.xqc_message_buffer
DROP TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- jd/ks
-- xqc_ods.message_all
ALTER TABLE xqc_ods.message_all
ADD COLUMN IF NOT EXISTS xrule_stats.id Array(String) AFTER `rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS xrule_stats.count Array(UInt32) AFTER `xrule_stats.id`,
ADD COLUMN IF NOT EXISTS xrule_stats.score Array(Int32) AFTER `xrule_stats.count`,
ADD COLUMN IF NOT EXISTS tags.tag_id Array(String) AFTER `wx_rule_add_stats.score`,
ADD COLUMN IF NOT EXISTS tags.cal_op Array(Int32) AFTER `tags.tag_id`,
ADD COLUMN IF NOT EXISTS tags.score Array(Int32) AFTER `tags.cal_op`,
ADD COLUMN IF NOT EXISTS tags.name Array(String) AFTER `tags.score`
-- buffer.xqc_message_buffer
DROP TABLE buffer.xqc_message_buffer
CREATE TABLE buffer.xqc_message_buffer
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)
