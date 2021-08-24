alter table dipper_ods.ask_order_conversion_stat_day set tblproperties('EXTERNAL'='true');
alter table dipper_ods.ask_order_conversion_stat_day set tblproperties('EXTERNAL'='false');
ALTER TABLE dipper_ods.ask_order_conversion_stat_day SET TBLPROPERTIES('kudu.table_name' = 'impala::dipper_ods.ask_order_conversion_stat_day')

kudu table rename_table zjk-bigdata002:7051 impala::dipper_dwd.ask_order_conversion_stat_day impala::dipper_ods.ask_order_conversion_stat_day


ALTER TABLE dipper_ods.ask_order_conversion_stat_day 
SET TBLPROPERTIES('EXTERNAL' = 'TRUE')

ALTER TABLE dipper_ods.ask_order_conversion_stat_day 
SET TBLPROPERTIES('EXTERNAL' = 'FALSE')