老淘宝环境:
1. 上传airflow operator脚本
jstzjk-002129-prod-tb-bigdata-bigdata (10.20.2.129) worker复制
    pg_to_clickhouse_operator.py
    clickhouse_to_pg_operator.py
到指定路径下
    /home/worker/airflow_venv/lib64/python3.6/site-packages/airflow/contrib/operators

2. 执行DDL文件中的SQL, zjk-bigdata008 (10.20.133.149) root
ddl-tb-2022-12-30.sql

3. 替换sql文件夹
jstzjk-002129-prod-tb-bigdata-bigdata (10.20.2.129) worker复制
    ft_sql.tar.gz
中的文件夹到指定路径下
    /home/worker/airflow/resources/sql/ft

4. 替换airflow脚本
jstzjk-002129-prod-tb-bigdata-bigdata (10.20.2.129) worker复制
    sxx_ft_etl_jd.py
到指定路径下
    /home/worker/airflow/dags


融合版环境:
1. 执行DDL文件中的SQL (v1mini-bigdata-002(10.22.113.169) root)
mini-ddl-2022-12-30.sql

2. 替换airflow脚本
v1mini-bigdata-002(10.22.113.169) worker复制
    sxx_ft_etl_mini.py
到指定路径下
    /home/worker/airflow/dags