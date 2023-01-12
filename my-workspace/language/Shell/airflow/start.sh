#!/bin/bash
#启动网站
airflow webserver -D
#启动守护进程运行调度
airflow scheduler -D
#启动celery worker
airflow celery worker -D
#启动flower
airflow celery flower -D
