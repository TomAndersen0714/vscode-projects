

webank-aml 数据同步任务客户端:
    aml_bdp/bin/utils/python_utils/etlTool.py | HAML_PYTHON_ETL_TOOL --type=spark
    aml_bdp/bin/utils/bash_utils/etl_submit.sh --type=sqoop
    aml_bdp/bin/utils/bash_utils/etl_submit.sh --type=spark
    aml_bdp/bin/utils/bash_utils/etl_submit.sh --type=blanca
        SparkJob
        Jdbc
    
    aml_bdp/bin/utils/python_utils/Sqoop.py | HAML_PYTHON_SQOOP_TOOL
    sqoop/import/blacklist/operator/import_mlist_type.py