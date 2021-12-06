-- 修改表数据源
ods.xinghuan_employee_snick_all
-- 修改统计逻辑
ods.qc_session_count_all


-- 融合版v1mini-bigdata-002 CH(1b73e87dc75e)导出历史维度数据到 OSS盘: /opt/bigdata
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"SELECT * FROM ods.xinghuan_employee_snick_all WHERE day >= 20210901 AND day<=20211128 FORMAT Avro" \
> /opt/bigdata/ods.xinghuan_employee_snick_all_20210901_20211128.Avro

-- bigdata005 将oos盘数据发送到 bigdata008
scp /opt/bigdata/ods.xinghuan_employee_snick_all_20210901_20211128.Avro root@zjk-bigdata008:/tmp/

-- 备份Airflow脚本
gitlab已备份

-- 备份已有数据
CREATE TABLE tmp.xinghuan_employee_snick_bak
AS ods.xinghuan_employee_snick_all
ENGINE = MergeTree()
ORDER BY day

INSERT INTO tmp.xinghuan_employee_snick_bak
SELECT * FROM ods.xinghuan_employee_snick_all
WHERE day >= 20210901

SELECT COUNT(1) FROM tmp.xinghuan_employee_snick_bak

-- 添加原表缺失字段 department_id
ALTER TABLE tmp.xinghuan_employee_snick_local ON CLUSTER cluster_3s_2r
ADD COLUMN department_id String AFTER mp_shop_id

ALTER TABLE tmp.xinghuan_employee_snick_all ON CLUSTER cluster_3s_2r
ADD COLUMN department_id String AFTER mp_shop_id

ALTER TABLE ods.xinghuan_employee_snick_local ON CLUSTER cluster_3s_2r
ADD COLUMN department_id String AFTER mp_shop_id

ALTER TABLE ods.xinghuan_employee_snick_all ON CLUSTER cluster_3s_2r
ADD COLUMN department_id String AFTER mp_shop_id


-- ods.xinghuan_employee_snick_all 切换 Mongo 数据源
mongo_conn_id='xdqc_offline',
mongo_db='xinghuan-mc',
mongo_collection='employee_snick',

mongo_conn_id='xdqc_offline',
mongo_db='xqc',
mongo_collection='snick',

-- 修改 Airflow SQL ods.qc_session_count_all 维度信息改成正确逻辑
1.修改 a.department_id AS department_id, 为 b.department_id AS department_id,
2.修改 LEFT JOIN 为 RIGHT JOIN


-- 创建Buffer表准备写入新版数据
CREATE TABLE buffer.ods_xinghuan_employee_snick_buffer
AS ods.xinghuan_employee_snick_all
ENGINE = Buffer('ods', 'xinghuan_employee_snick_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)

-- 清空已备份数据
ALTER TABLE ods.xinghuan_employee_snick_local ON CLUSTER cluster_3s_2r
DELETE WHERE day >= 20210901 AND day<=20211128

OPTIMIZE TABLE ods.xinghuan_employee_snick_local ON CLUSTER cluster_3s_2r

-- bigdata008:7524cac17c99 上传数据
docker exec -i 7524cac17c99 clickhouse-client --port=29000 --query=\
"INSERT INTO buffer.ods_xinghuan_employee_snick_buffer FORMAT Avro" \
< /tmp/ods.xinghuan_employee_snick_all_20210901_20211128.Avro


-- 重新加载元数据: 重跑load_data_tmp2ods及其上游任务, upstream & recursive


-- 重新计算质检总览统计数据: 重跑yesterday_run及其下游任务, downstream & recursive

