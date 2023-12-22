hive -e "

-- 执行日期 batch_date
set mapred.max.split.size=512000000;
use imd_aml300_ads_safe;
"