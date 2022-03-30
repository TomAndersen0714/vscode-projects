-- 提交Airflow gitlab代码审查

-- 融合版v1mini-bigdata-002 CH(1b73e87dc75e)导出近2个月历史维度数据: 20210901
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"SELECT * FROM ods.xdqc_tag_all WHERE day >= 20210901 FORMAT Avro" \
> /tmp/ods.xdqc_tag_all_20210901_.Avro

docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"SELECT * FROM ods.xdqc_tag_sub_category_all WHERE day >= 20210901 FORMAT Avro" \
> /tmp/ods.xdqc_tag_sub_category_all_20210901_.Avro


-- 使用FileZilla将tmp文件下载到本地, 然后上传到京东平台 /tmp 路径

-- yd-bigdata-01:7dd8d79f4280 修改表结构,tmp.xdqc_tag,ods.xdqc_tag_all 增加qc_norm_id AFTER platform
-- PS: ods.xdqc_tag_sub_category_all表融合版已经和京东保持了一致
ALTER TABLE tmp.xdqc_tag
ADD COLUMN qc_norm_id String AFTER platform

ALTER TABLE ods.xdqc_tag_all
ADD COLUMN qc_norm_id String AFTER platform


-- 备份已有数据(20210901之后)
CREATE TABLE tmp.xdqc_tag_bak
AS ods.xdqc_tag_all
ENGINE = MergeTree()
PARTITION BY day 
ORDER BY (platform, category_id)
SETTINGS index_granularity = 8192

INSERT INTO TABLE tmp.xdqc_tag_bak
SELECT * FROM ods.xdqc_tag_all WHERE day >= 20210901

CREATE TABLE tmp.xdqc_tag_sub_category_bak
AS ods.xdqc_tag_sub_category_all
ENGINE = MergeTree() 
PARTITION BY day
ORDER BY (company_id, seller_nick) 
SETTINGS index_granularity = 8192

INSERT INTO TABLE tmp.xdqc_tag_sub_category_bak
SELECT * FROM ods.xdqc_tag_sub_category_all WHERE day >= 20210901

-- 验证备份数据量
SELECT COUNT(1) FROM tmp.xdqc_tag_bak
SELECT COUNT(1) FROM ods.xdqc_tag_all WHERE day >= 20210901

SELECT COUNT(1) FROM tmp.xdqc_tag_sub_category_bak
SELECT COUNT(1) FROM ods.xdqc_tag_sub_category_all WHERE day >= 20210901


-- 创建Buffer表
CREATE TABLE buffer.xdqc_tag_tmp_buffer
AS ods.xdqc_tag_all
ENGINE = Buffer('ods', 'xdqc_tag_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

CREATE TABLE buffer.xdqc_tag_sub_category_tmp_buffer
AS ods.xdqc_tag_sub_category_all
ENGINE = Buffer('ods', 'xdqc_tag_sub_category_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- 删除已有数据
ALTER TABLE ods.xdqc_tag_all DELETE WHERE day >= 20210901
ALTER TABLE ods.xdqc_tag_sub_category_all DELETE WHERE day >= 20210901

OPTIMIZE TABLE ods.xdqc_tag_all
OPTIMIZE TABLE ods.xdqc_tag_sub_category_all

-- 写入新版数据
docker exec -i 7dd8d79f4280 clickhouse-client --port=9900 --query=\
"INSERT INTO buffer.xdqc_tag_tmp_buffer FORMAT Avro" \
< /tmp/ods.xdqc_tag_all_20210901_.Avro

docker exec -i 7dd8d79f4280 clickhouse-client --port=9900 --query=\
"INSERT INTO buffer.xdqc_tag_sub_category_tmp_buffer FORMAT Avro" \
< /tmp/ods.xdqc_tag_sub_category_all_20210901_.Avro

-- 备份线上代码, 然后上线修改后的Airflow代码, 手动重刷一次当天统计数据


-- (可选)修改融合版和京东表结构, 便于后续接收新版数据
-- 修改表结构,tmp.xdqc_tag,ods.xdqc_tag_all 增加company_id AFTER platform
-- tmp.xdqc_tag_sub_category, ods.xdqc_tag_sub_category_all 增加qc_norm_id AFTER platform
-- 添加字段
ALTER TABLE tmp.xdqc_tag
ADD COLUMN company_id String AFTER platform

ALTER TABLE ods.xdqc_tag_all
ADD COLUMN company_id String AFTER platform

ALTER TABLE tmp.xdqc_tag_sub_category
ADD COLUMN qc_norm_id String AFTER platform

ALTER TABLE ods.xdqc_tag_sub_category_all
ADD COLUMN qc_norm_id String AFTER platform