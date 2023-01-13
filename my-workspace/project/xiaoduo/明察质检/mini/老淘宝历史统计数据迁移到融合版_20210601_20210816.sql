-- 质检总览数据迁移
ods.qc_session_count_all
ods.qc_read_mark_detail_all
ods.qc_question_detail_all
ods.qc_words_detail_all
-- PS: 案例分析所需表(已废弃此功能) ods.qc_case_label_detail_all

-- 时间范围
WHERE toYYYYMMDD(date) BETWEEN 20210601 AND 20210816

-- 确定融合版已有数据范围
SELECT MIN(toYYYYMMDD(date)),MAX(toYYYYMMDD(date)) FROM ods.qc_session_count_all
SELECT MIN(toYYYYMMDD(date)),MAX(toYYYYMMDD(date)) FROM ods.qc_read_mark_detail_all
SELECT MIN(toYYYYMMDD(date)),MAX(toYYYYMMDD(date)) FROM ods.qc_question_detail_all
SELECT MIN(toYYYYMMDD(date)),MAX(toYYYYMMDD(date)) FROM ods.qc_words_detail_all


-- 老淘宝 zjk-bigdata006 CH数据导出为 Avro (PS:默认 Snappy压缩)
docker exec -i 9e clickhouse-client --port=19000 --query=\
"select * from ods.qc_session_count_all WHERE toYYYYMMDD(date) BETWEEN 20210601 AND 20210816 FORMAT Avro" \
> /tmp/ods.qc_session_count_all_20210601_20210816.Avro

docker exec -i 9e clickhouse-client --port=19000 --query=\
"select * from ods.qc_read_mark_detail_all WHERE toYYYYMMDD(date) BETWEEN 20210601 AND 20210816 FORMAT Avro" \
> /tmp/ods.qc_read_mark_detail_all_20210601_20210816.Avro

docker exec -i 9e clickhouse-client --port=19000 --query=\
"select * from ods.qc_question_detail_all WHERE toYYYYMMDD(date) BETWEEN 20210601 AND 20210816 FORMAT Avro" \
> /tmp/ods.qc_question_detail_all_20210601_20210816.Avro

docker exec -i 9e clickhouse-client --port=19000 --query=\
"select * from ods.qc_words_detail_all WHERE toYYYYMMDD(date) BETWEEN 20210601 AND 20210816 FORMAT Avro" \
> /tmp/ods.qc_words_detail_all_20210601_20210816.Avro


-- 老淘宝 zjk-bigdata005 复制 zjk-bigdata006 数据到OSS盘
scp root@zjk-bigdata006:/tmp/ods.qc_session_count_all_20210601_20210816.Avro /opt/bigdata/
scp root@zjk-bigdata006:/tmp/ods.qc_read_mark_detail_all_20210601_20210816.Avro /opt/bigdata/
scp root@zjk-bigdata006:/tmp/ods.qc_question_detail_all_20210601_20210816.Avro /opt/bigdata/
scp root@zjk-bigdata006:/tmp/ods.qc_words_detail_all_20210601_20210816.Avro /opt/bigdata/

-- v1mini-bigdata-002 CH(1b73e87dc75e)创建本地表, 用于备份数据!!!
CREATE TABLE tmp.qc_session_count_all_bak
AS ods.qc_session_count_all
ENGINE = MergeTree()
ORDER BY `date`

CREATE TABLE tmp.qc_read_mark_detail_all_bak
AS ods.qc_read_mark_detail_all
ENGINE = MergeTree()
ORDER BY `date`

CREATE TABLE tmp.qc_question_detail_all_bak
AS ods.qc_question_detail_all
ENGINE = MergeTree()
ORDER BY `date`

CREATE TABLE tmp.qc_words_detail_all_bak
AS ods.qc_words_detail_all
ENGINE = MergeTree()
ORDER BY `date`

-- v1mini-bigdata-002 CH(1b73e87dc75e)备份融合版已有数据(上次迁移时已经备份)
-- INSERT INTO TABLE tmp.qc_session_count_all_bak
-- SELECT * FROM ods.qc_session_count_all

-- INSERT INTO TABLE tmp.qc_read_mark_detail_all_bak
-- SELECT * FROM ods.qc_read_mark_detail_all

-- INSERT INTO TABLE tmp.qc_question_detail_all_bak
-- SELECT * FROM ods.qc_question_detail_all

-- INSERT INTO TABLE tmp.qc_words_detail_all_bak
-- SELECT * FROM ods.qc_words_detail_all

-- v1mini-bigdata-002 CH(1b73e87dc75e)融合版创建Buffer表, 用于写入数据
CREATE TABLE buffer.ods_qc_session_count_buffer
AS ods.qc_session_count_all
ENGINE = Buffer('ods', 'qc_session_count_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)

CREATE TABLE buffer.ods_qc_read_mark_detail_buffer
AS ods.qc_read_mark_detail_all
ENGINE = Buffer('ods', 'qc_read_mark_detail_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)

CREATE TABLE buffer.qc_question_detail_buffer
AS ods.qc_question_detail_all
ENGINE = Buffer('ods', 'qc_question_detail_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)

CREATE TABLE buffer.qc_words_detail_all_buffer
AS ods.qc_words_detail_all
ENGINE = Buffer('ods', 'qc_words_detail_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)



-- 融合版 v1mini-bigdata-002 CH(1b73e87dc75e)写入数据
-- ods.qc_session_count_all
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.ods_qc_session_count_buffer FORMAT Avro" \
< /opt/bigdata/ods.qc_session_count_all_20210601_20210816.Avro

-- ods.qc_read_mark_detail_all
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.ods_qc_read_mark_detail_buffer FORMAT Avro" \
< /opt/bigdata/ods.qc_read_mark_detail_all_20210601_20210816.Avro

-- ods.qc_question_detail_all
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.qc_question_detail_buffer FORMAT Avro" \
< /opt/bigdata/ods.qc_question_detail_all_20210601_20210816.Avro

-- ods.qc_words_detail_all
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"INSERT INTO buffer.qc_words_detail_all_buffer FORMAT Avro" \
< /opt/bigdata/ods.qc_words_detail_all_20210601_20210816.Avro

