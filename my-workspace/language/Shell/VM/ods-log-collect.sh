#!/bin/bash
# 此脚本用于每天凌晨执行
# 当未传入参数时,默认将前一天hdfs中收集的日志数据传入到对应hive表中
# 当传入单个参数时,获取hdfs中指定日期的日志传入到对应hive表中
# 本脚本最多只支持传入单个参数,且必须为特定格式日期,即:yyyy-MM-dd,如:2020-06-22

# 判断参数个数,只支持不传入参数,或者只传入单个参数
if (($# > 1)); then
    echo "This shellscript takes only one parameter at most!"
    exit 1
fi
# 获取当前时间(相对时间)
start_time=$(date +%s)
# 日志收集日期
log_date=""
# hive client主机
hive_client="hadoop101"
# hive_bin_dir
hive_bin_dir=${HIVE_HOME:-"/opt/module/hive-2.3.0"}/bin
# 登录hive客户端主机所用用户名
user="tomandersen"
# hive仓库名
warehouse="gmall"

# 如果输入参数为空,则将其设置为昨天的日期
# 如果输出参数不为空,则判断输入的参数是否符合日期格式,并尝试将其转换成标准日期格式
if [ "$1" = "" ]; then
    log_date=$(date -d '-1 day' +%F)
else
    log_date=$(date -d $1 +%F) || exit 1
fi;

# 导入表数据文件的hive-sql
hive_sql="
CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.ods_start_log(line string)
PARTITIONED BY (dt string)
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA INPATH '/logs/app_start/$log_date' INTO TABLE $warehouse.ods_start_log PARTITION(dt='$log_date');

CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.ods_event_log(line string)
PARTITIONED BY (dt string)
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA INPATH '/logs/app_event/$log_date' INTO TABLE $warehouse.ods_event_log PARTITION(dt='$log_date');
"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Collect logs from hdfs to hive table on [$host] takes $execute_time seconds----------\n"