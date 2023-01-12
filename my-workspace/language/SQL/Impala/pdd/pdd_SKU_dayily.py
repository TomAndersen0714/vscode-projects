from airflow.models.dag import DAG
from datetime import datetime
from airflow.contrib.operators.impala_executor import ImpalaOperator

args = {
    'owner': 'tangyining',
    'start_date': datetime(2020, 5, 1, 0),
    'email': ['tangyining@xiaoduotech.com'],
    'email_on_failure': True,
}


dag = DAG(
    dag_id='pdd_SKU_daily',
    schedule_interval="5 5 * * *",
    max_active_runs=1,
    default_args=args
)


data_stat = ImpalaOperator(
    task_id="data_stat",
    impala_conn_id="impala_001",
    sql_file='/home/worker/xiaoduo_bigdata/pdd_xdrs_logs/app_mp.pdd_goods_question_stat.sql',
    dag=dag
)

