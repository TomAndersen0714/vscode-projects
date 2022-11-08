CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE tmp.xqc_dws_dialog_eval_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xqc_dws_dialog_eval_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `eval_code` Int32,
    `eval_cnt` Int64,
    `dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (day, platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.xqc_dws_dialog_eval_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xqc_dws_dialog_eval_stat_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_dialog_eval_stat_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_dws_dialog_eval_stat_local', rand())

-- DROP TABLE buffer.tmp_xqc_dws_dialog_eval_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_xqc_dws_dialog_eval_stat_buffer ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_dialog_eval_stat_all
ENGINE = Buffer('tmp', 'xqc_dws_dialog_eval_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


docker exec -i 1b73e87dc75e clickhouse-client --port=19000 --query=\
"select * from tmp.xqc_dws_dialog_eval_stat_all WHERE day BETWEEN 20220701 AND 20220824 FORMAT Avro" \
> ./tmp_xqc_dws_dialog_eval_stat_all_20220701_20220824.avro

docker exec -i 9043cb24167c clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.tmp_xqc_dws_dialog_eval_stat_buffer FORMAT Avro" \
< ./tmp_xqc_dws_dialog_eval_stat_all_20220701_20220824.avro

INSERT INTO xqc_dws.dialog_eval_stat_all
SELECT *
FROM tmp.xqc_dws_dialog_eval_stat_all
WHERE day BETWEEN 20220701 AND 20220731

INSERT INTO xqc_dws.dialog_eval_stat_all
SELECT *
FROM tmp.xqc_dws_dialog_eval_stat_all
WHERE day BETWEEN 20220801 AND 20220824