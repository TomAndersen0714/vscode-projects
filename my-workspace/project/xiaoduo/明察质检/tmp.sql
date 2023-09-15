SELECT *,'ks' as platform 
 FROM remote('10.20.133.175:9000','app_fishpond','customer_pool_summary','' )
 where day = 20230914
 LIMIT 10
 
SELECT COUNT(1)
FROM buffer.customer_pool_summary_platform_buffer
WHERE day = 20230914 AND platform = 'ks'

SHOW CREATE TABLE buffer.customer_pool_summary_platform_buffer

SELECT COUNT(1)
FROM app_fishpond.customer_pool_summary_platform_all
WHERE day = 20230914 AND platform = 'ks'

desc buffer.customer_pool_summary_platform_buffer