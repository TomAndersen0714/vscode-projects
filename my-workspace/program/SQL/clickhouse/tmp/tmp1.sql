-- 导出数据文件
docker exec -i 621d clickhouse-client --port=29000 --query="\
select messsage_type,plat_user_id,snick,cnick,create_time,msg_type,msg,plat_msg_id,\
send_failed_reason,recv_source,keban_task_id from ods.qn_new_rules_logs_all where day=20211020 \
FORMAT Parquet" \
> /tmp/ods.qn_new_rules_logs_all_20211020.parq

-- 上传文件到Impala
hadoop fs -put /tmp/ods.qn_new_rules_logs_all_20211020.parq /user/hive/warehouse/test.db/qn_new_rules_logs/day=20211020


-- 执行查询
/user/hive/warehouse/ods.db/qn_new_rules_logs/day=20211020

insert overwrite ods.qn_new_rules_logs partition (`day` = 20211020})
SELECT * FROM test.qn_new_rules_logs