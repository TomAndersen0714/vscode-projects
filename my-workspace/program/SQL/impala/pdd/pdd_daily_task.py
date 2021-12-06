

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'xianghai',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 2),
    'email': ['xianghai@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('pdd_daily_task', default_args=default_args, schedule_interval="30 3 * * *")
log_dir = '/home/worker/airflow/logs'
work_dir = '/home/worker/xiaoduo_bigdata'
dwd_xdrs_logs = BashOperator(
    task_id='dwd_xdrs_logs',
    bash_command='bash %s/pdd_xdrs_logs/flow_xdrs_logs.sh > %s/dwd.xdrs_logs.log 2>&1' % (work_dir, log_dir),
    dag=dag)

app_mp = BashOperator(
    task_id='app_mp',
    bash_command='bash %s/app_mp/flow_pdd_app_mp.sh > %s/app_mp.log 2>&1' % (work_dir, log_dir),
    dag=dag)

dwd_xdrs_logs >> app_mp




