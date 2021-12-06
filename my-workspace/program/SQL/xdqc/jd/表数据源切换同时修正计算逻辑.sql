-- 修改表数据源
ods.xinghuan_employee_snick_all
-- 修改统计逻辑
ods.qc_session_count_all

-- 备份Airflow脚本
已备份

-- 切换 Mongo 数据源
mongo_conn_id='xdqc_tmp',
mongo_db='xinghuan-mc',
mongo_collection='employee_snick',

mongo_conn_id='xdqc_offline',
mongo_db='xqc',
mongo_collection='snick',


-- 修改Airflow SQL ods.qc_session_count_all改成正确逻辑
1.修改a.department_id AS department_id, 为 b.department_id AS department_id,
2.修改LEFT JOIN为RIGHT JOIN

-- 添加tmp表字段 department_id
ALTER TABLE tmp.xinghuan_employee_snick
ADD COLUMN department_id String AFTER mp_shop_id
 
-- 添加ods表字段 department_id
ALTER TABLE ods.xinghuan_employee_snick_all
ADD COLUMN department_id String AFTER mp_shop_id

-- 重新加载元数据: 重跑load_data_tmp2ods及其上游任务, upstream & recursive

-- 重新计算质检总览统计数据: 重跑yesterday_run及其下游任务, downstream & recursive

