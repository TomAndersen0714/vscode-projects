-- 融合版v1mini-bigdata-002 CH(1b73e87dc75e)导出近2个月历史维度数据: 20210801-20211024
docker exec -i 1b7 clickhouse-client --port=19000 --query=\
"SELECT * FROM ods.xinghuan_employee_snick_all WHERE day BETWEEN 20210801 AND 20211024 AND platform = 'jd' FORMAT Avro" \
> /tmp/ods.xinghuan_employee_snick_all_20210801_20211024.Avro

-- 使用FileZilla将tmp文件下载到本地, 然后上传到京东平台 /tmp 路径

-- 京东备份维度表 20210801-20211024 数据
-- 创建备份表
CREATE TABLE tmp.xinghuan_employee_snick_bak
AS ods.xinghuan_employee_snick_all
ENGINE = MergeTree()
ORDER BY day

-- 备份维度数据
INSERT INTO tmp.xinghuan_employee_snick_bak
SELECT * FROM ods.xinghuan_employee_snick_all
WHERE day BETWEEN 20210801 AND 20211024 AND platform = 'jd'

-- 清空已备份的维度数据
ALTER TABLE ods.xinghuan_employee_snick_all
DELETE WHERE day BETWEEN 20210801 AND 20211024
-- 使得 Mutations 生效
OPTIMIZE TABLE ods.xinghuan_employee_snick_all

-- 创建buffer表用于写入数据
CREATE TABLE buffer.ods_xinghuan_employee_snick_buffer
AS ods.xinghuan_employee_snick_all
ENGINE = Buffer('ods', 'xinghuan_employee_snick_all', 16, 5, 10, 81920, 409600, 8388608, 16777216)

-- 7dd8d79f4280 刷入新的维度数据
docker exec -i 7dd8d79f4280 clickhouse-client --port=9900 --query=\
"INSERT INTO buffer.ods_xinghuan_employee_snick_buffer FORMAT Avro" \
< /tmp/ods.xinghuan_employee_snick_all_20210801_20211024.Avro

-- 发现缺少8月份数据, 因此从备份数据中恢复8月份的记录
INSERT INTO ods.xinghuan_employee_snick_all
SELECT * FROM tmp.xinghuan_employee_snick_bak
WHERE day BETWEEN 20210801 AND 20210831