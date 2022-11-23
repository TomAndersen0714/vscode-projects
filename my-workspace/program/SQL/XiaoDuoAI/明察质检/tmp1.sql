-- ks
Airflow Web UI:
https://airflow2.xiaoduoai.com/connection/list/

xdqc_mongo:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717, dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166
xdqc_mongo_ks:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717, dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166

scripts:
1. xqc_etl_ks.py
2. xqc_message_etl_ks.py

-- dy
Airflow Web UI:
https://dy-airflow-v1.xiaoduoai.com/admin/connection/

xdqc_mongo:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717, dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166
xdqc_mongo_dy:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717, dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166

scripts:
1. xqc_etl_dy.py
2. xqc_message_etl_dy.py

-- tb
Airflow Web UI:
https://airflow2.xiaoduoai.com/connection/list/

xdqc_mongo:
dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com

xdqc_mongo_tb:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717,dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166

xdqc_offline:
dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717,dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166

scripts:
1. message.py
2. xqc_dim_etl_tb.py
3. xqc_ods_message_etl_tb.py
3. xqc_ods_etl_tb.py
